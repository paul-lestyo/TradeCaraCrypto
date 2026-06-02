# Tujuan
# Entry point aplikasi end-to-end Telegram signal trader.
# Caller
# `python -m CaraCrypto`.
# Dependensi
# Semua module internal + Binance client.
# Main Functions
# `main()`, startup reconcile posisi, loop pemrosesan signal, dan subscribe watcher pasca eksekusi order.
# Side Effects
# Menjalankan koneksi DB, Telegram, dan network service.

from __future__ import annotations

import asyncio
import inspect
import pathlib
import traceback

from dotenv import load_dotenv

try:
    from binance.um_futures import UMFutures
except Exception:
    from binance.client import Client as UMFutures

from .alert_service import AlertService
from .config import load_config
from .context_builder import ContextBuilder
from .database import Database
from .models import GeminiAction, TradeAction
from .position_manager import PositionManager
from .price_watcher import PriceWatcher
from .signal_listener import SignalListener
from .signal_parser import SignalParser
from .trade_engine import TradeEngine


def _bootstrap_env() -> None:
    """Muat `.env` dari working dir maupun root repo sebelum load_config().

    Aman dipanggil berkali-kali. Default env-nya tidak ditimpa kalau sudah
    di-set oleh harness (mis. docker-compose).
    """
    candidates = []
    cwd_env = pathlib.Path.cwd() / ".env"
    repo_env = pathlib.Path(__file__).resolve().parent.parent / ".env"
    if cwd_env.exists():
        candidates.append(cwd_env)
    if repo_env != cwd_env and repo_env.exists():
        candidates.append(repo_env)
    if not candidates:
        load_dotenv()
        return
    for path in candidates:
        load_dotenv(path, override=False)


async def _process_signals(queue, db, context_builder, parser, engine, watcher):
    while True:
        raw = await queue.get()
        print(f"[Pipeline] Processing message_id={raw.message_id} group={raw.group_id}")
        try:
            await _process_one_signal(raw, db, context_builder, parser, engine, watcher)
        except Exception as exc:
            err = f"message_id={raw.message_id} group={raw.group_id} err={exc}"
            print(f"[Pipeline][Error] {err}")
            await engine.alert_service.notify_error("pipeline_process_signal", err)
        queue.task_done()


async def _subscribe_watcher(watcher, pair, action):
    try:
        await watcher.subscribe(pair, action)
    except TypeError:
        await watcher.subscribe(pair)


async def _restore_watcher_subscriptions(watcher, position_manager) -> None:
    for pos in position_manager.get_running_positions():
        await _subscribe_watcher(watcher, pos.pair, None)
    get_pending_positions = getattr(position_manager, "get_pending_positions", None)
    pending_positions = get_pending_positions() if callable(get_pending_positions) else []
    for pos in pending_positions:
        action = TradeAction(action=GeminiAction.NEW_SIGNAL, pair=pos.pair)
        watcher.register_pending_order(str(pos.order_id), action)
        await _subscribe_watcher(watcher, pos.pair, action)


async def _process_one_signal(raw, db, context_builder, parser, engine, watcher):
    get_by_telegram_id = getattr(db, "get_message_by_telegram_id", None)
    existing_message = None
    if callable(get_by_telegram_id):
        existing_message = await get_by_telegram_id(raw.message_id, raw.group_id)
    if existing_message:
        message_db_id = existing_message.id
        await db.update_message_text(message_db_id, raw.text or "")
    else:
        message_db_id = await db.store_message(
            {
                "message_id": raw.message_id,
                "group_id": raw.group_id,
                "topic_id": raw.topic_id,
                "text": raw.text,
                "reply_to_message_id": raw.reply_to_message_id,
            }
        )
    await db.populate_reply_data(message_db_id, raw.group_id, raw.reply_to_message_id)
    context = await context_builder.build_context(raw)
    exchange_state = {}
    get_exchange_context_state = getattr(engine, "get_exchange_context_state", None)
    if callable(get_exchange_context_state):
        exchange_state = get_exchange_context_state()
    if isinstance(context, dict):
        context["exchange_state"] = exchange_state
        action = await parser.parse_and_classify(context, message_db_id)
        if not action or action.action == GeminiAction.SKIP:
            print(f"[Pipeline] message_id={raw.message_id} action=skip")
            return
        print(f"[Pipeline] message_id={raw.message_id} action={action.action.value} pair={action.pair}")
        executed = await engine.execute_action(action, message_db_id)
        if action.action in {GeminiAction.NEW_SIGNAL, GeminiAction.RE_ENTRY} and action.pair and executed:
            await _subscribe_watcher(watcher, action.pair, action)
            print(f"[Watcher] Subscribed pair={action.pair}")
        return
    context.exchange_state = exchange_state
    state = context.position_state
    text_preview = (raw.text or "").replace("\n", " ").strip()
    if len(text_preview) > 160:
        text_preview = text_preview[:160] + "..."
    print(
        "[Context] "
        f"message_id={raw.message_id} "
        f"group={raw.group_id} topic={raw.topic_id} "
        f"reply_to={raw.reply_to_message_id} "
        f"message='{text_preview}' "
        f"binance_running={context.exchange_state.get('running_pairs')} "
        f"binance_open_orders={context.exchange_state.get('open_order_pairs')} "
        f"closed_today={state.closed_today} "
        f"history_count={len(context.history)}"
    )
    action = await parser.parse_and_classify(context, message_db_id)
    if not action or action.action == GeminiAction.SKIP:
        print(f"[Pipeline] message_id={raw.message_id} action=skip")
        return
    print(f"[Pipeline] message_id={raw.message_id} action={action.action.value} pair={action.pair}")
    executed = await engine.execute_action(action, message_db_id)
    if action.action in {GeminiAction.NEW_SIGNAL, GeminiAction.RE_ENTRY} and action.pair and executed:
        await _subscribe_watcher(watcher, action.pair, action)
        print(f"[Watcher] Subscribed pair={action.pair}")


async def main() -> None:
    print("[Main] Starting CaraCrypto Trader...")
    _bootstrap_env()
    cfg = load_config()
    db = Database(cfg.database.url)
    await db.connect()
    print("[Main] Database connected")

    queue = asyncio.Queue()
    alert_service = AlertService(cfg.alert)
    position_manager = PositionManager(db)
    await position_manager.initialize()

    context_builder = ContextBuilder(db, position_manager)
    parser = SignalParser(cfg.gemini, db)
    params = set(inspect.signature(UMFutures.__init__).parameters.keys())
    if {"key", "secret"}.issubset(params):
        binance = UMFutures(
            key=cfg.binance.api_key,
            secret=cfg.binance.api_secret,
            base_url=cfg.binance.futures_base_url,
        )
    else:
        client_kwargs = {
            "api_key": cfg.binance.api_key,
            "api_secret": cfg.binance.api_secret,
        }
        if "testnet" in params and cfg.binance.env == "testnet":
            client_kwargs["testnet"] = True
        if "demo" in params and cfg.binance.env == "demo":
            client_kwargs["demo"] = True
        binance = UMFutures(**client_kwargs)
        # Tetap override URL untuk kompatibilitas versi python-binance lama
        # yang belum kenal kwargs `testnet`/`demo`.
        if cfg.binance.env != "mainnet":
            binance.FUTURES_URL = f"{cfg.binance.futures_base_url}/fapi"
            binance.FUTURES_DATA_URL = f"{cfg.binance.futures_base_url}/futures/data"
    print(f"[Main] Binance env={cfg.binance.env} base={cfg.binance.futures_base_url}")
    engine = TradeEngine(binance, db, alert_service, position_manager, cfg.risk)
    watcher = PriceWatcher(alert_service, position_manager, pending_missing_max_retry=3)
    watcher.trade_engine = engine
    engine.price_watcher = watcher
    print("[Main] Watcher wired to engine")
    reconcile = await engine.reconcile_positions_with_exchange()
    print(
        "[Main] Startup reconcile "
        f"kept={reconcile['kept']} recovered={reconcile['recovered']} removed={reconcile['removed']}"
    )
    await _restore_watcher_subscriptions(watcher, position_manager)

    listener = SignalListener(cfg.telegram, db, alert_service, queue)
    print("[Main] Services initialized.")
    print("[Main] Starting tasks: watcher, telegram listener, pipeline processor")

    try:
        await asyncio.gather(
            watcher.start(),
            listener.start(),
            _process_signals(queue, db, context_builder, parser, engine, watcher),
        )
    except Exception as exc:
        tb = traceback.format_exc(limit=12)
        print(f"[Main][Fatal] {exc}\n{tb}")
        await alert_service.notify_error("main_fatal", f"{exc}\n{tb}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
