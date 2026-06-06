# Tujuan
# Monitor harga dan user data stream Binance untuk alert dan trigger TP/SL pasca fill.
# Caller
# __main__ startup dan trade_engine saat limit order terpasang.
# Dependensi
# asyncio, position_manager, trade_engine.
# Main Functions
# subscribe/unsubscribe/register_pending_order/handle_order_update.
# Side Effects
# Menjalankan loop async monitoring.

from __future__ import annotations

import asyncio
import contextlib
from decimal import Decimal
from urllib.parse import urlparse
from typing import Dict

import aiohttp

from .alert_service import AlertService
from .models import Direction, TradeAction
from .position_manager import PositionManager


class PriceWatcher:
    def __init__(self, alert_service: AlertService, position_manager: PositionManager, pending_missing_max_retry: int = 1):
        self.alert_service = alert_service
        self.position_manager = position_manager
        self.trade_engine = None
        self._running = False
        self._subscriptions = set()
        self._pending_limit_orders: Dict[str, TradeAction] = {}
        self._pending_order_missing_count: Dict[str, int] = {}
        self._pending_missing_max_retry = max(1, int(pending_missing_max_retry))
        self._listen_key: str | None = None
        self._listen_key_task: asyncio.Task | None = None

    async def _safe_alert(self, method: str, *args) -> None:
        fn = getattr(self.alert_service, method, None)
        if callable(fn):
            await fn(*args)

    async def start(self) -> None:
        self._running = True
        print("[PriceWatcher] Started")
        asyncio.create_task(self._start_user_data_stream())
        while self._running:
            await self._watch_price()
            await self._reconcile_pending_limit_orders()
            await asyncio.sleep(1)

    async def stop(self) -> None:
        self._running = False
        if self._listen_key_task:
            self._listen_key_task.cancel()
            with contextlib.suppress(Exception):
                await self._listen_key_task
            self._listen_key_task = None
        print("[PriceWatcher] Stopped")

    async def subscribe(self, pair: str, action: TradeAction | None = None) -> None:
        self._subscriptions.add(pair)
        print(f"[PriceWatcher] subscribe pair={pair} active={sorted(self._subscriptions)}")
        _ = action

    async def unsubscribe(self, pair: str) -> None:
        self._subscriptions.discard(pair)
        print(f"[PriceWatcher] unsubscribe pair={pair} active={sorted(self._subscriptions)}")

    def register_pending_order(self, order_id: str, action: TradeAction) -> None:
        self._pending_limit_orders[order_id] = action
        self._pending_order_missing_count.pop(order_id, None)
        print(
            "[PriceWatcher] register_pending_order "
            f"order_id={order_id} pair={action.pair} action={action.action.value} "
            f"pending_count={len(self._pending_limit_orders)}"
        )

    async def _watch_price(self) -> None:
        for pair in list(self._subscriptions):
            pos = self.position_manager.get_position(pair)
            if not pos:
                continue
            current_price = None
            if self.trade_engine and hasattr(self.trade_engine, "_get_market_reference_price"):
                try:
                    current_price = await self.trade_engine._get_market_reference_price(pair)
                except Exception:
                    current_price = None
            if current_price is None:
                current_price = pos.entry_price

            tp1 = self._get_tp1_level(pos.direction, pos.tp_levels)
            if tp1 is not None and self._check_tp_level_reached(pos.direction, current_price, tp1):
                if not pos.tp1_notified:
                    await self.alert_service.notify_tp_hit(pair, str(tp1))
                    pos.tp1_notified = True
                if (
                    self.trade_engine
                    and not pos.tp1_sl_plus_applied
                    and hasattr(self.trade_engine, "_handle_set_sl_plus_buffer")
                ):
                    try:
                        applied = await self.trade_engine._handle_set_sl_plus_buffer(
                            pair,
                            source="watcher_tp1_auto",
                        )
                        if applied:
                            pos.tp1_sl_plus_applied = True
                    except Exception as exc:
                        print(f"[PriceWatcher] Failed to set SL+ buffer for {pair}: {exc}")
                        await self.alert_service.notify_error(
                            "watcher_sl_plus_failed",
                            f"Gagal set SL+ breakeven untuk {pair}: {exc}"
                        )

            if pos.current_sl is not None and self._check_sl_reached(pos.direction, current_price, pos.current_sl):
                if pos.last_sl_alerted != pos.current_sl:
                    await self.alert_service.notify_sl_hit(pair, str(pos.current_sl))
                    pos.last_sl_alerted = pos.current_sl

    async def _reconcile_pending_limit_orders(self) -> None:
        if not self._pending_limit_orders or not self.trade_engine:
            return
        running_pairs = None
        if hasattr(self.trade_engine, "_get_binance_running_pairs"):
            try:
                running_pairs = self.trade_engine._get_binance_running_pairs()
            except Exception:
                running_pairs = None
        open_order_ids_by_pair: dict[str, set[str]] = {}
        for order_id, action in list(self._pending_limit_orders.items()):
            pair = action.pair or ""
            if not pair:
                self._pending_limit_orders.pop(order_id, None)
                continue
            if pair not in open_order_ids_by_pair:
                open_order_ids: set[str] = set()
                try:
                    open_orders = self.trade_engine._get_open_orders(pair)
                    for order in open_orders:
                        oid = order.get("orderId")
                        if oid is not None:
                            open_order_ids.add(str(oid))
                except Exception:
                    continue
                open_order_ids_by_pair[pair] = open_order_ids
            if str(order_id) in open_order_ids_by_pair[pair]:
                self._pending_order_missing_count.pop(order_id, None)
                continue
            if running_pairs is not None and pair in running_pairs:
                await self._handle_order_update(str(order_id), "LIMIT", "FILLED", pair)
                continue
            missing_count = self._pending_order_missing_count.get(order_id, 0) + 1
            self._pending_order_missing_count[order_id] = missing_count
            if missing_count < self._pending_missing_max_retry:
                print(
                    "[PriceWatcher] pending order temporarily missing on exchange "
                    f"pair={pair} order_id={order_id} missing_count={missing_count}"
                )
                continue
            print(
                "[PriceWatcher] pending order missing on exchange -> assume canceled "
                f"pair={pair} order_id={order_id} missing_count={missing_count}"
            )
            self._pending_limit_orders.pop(order_id, None)
            self._pending_order_missing_count.pop(order_id, None)
            has_pending = getattr(self.position_manager, "has_pending_position", None)
            pending_exists = bool(callable(has_pending) and has_pending(pair))
            if self.position_manager.get_position(pair) or pending_exists:
                await self.position_manager.remove_position(pair)
            await self.unsubscribe(pair)
            await self._safe_alert("notify_closed", pair, "cancel")

    def _check_tp_level_reached(self, direction: Direction, price: Decimal, tp_level: Decimal) -> bool:
        if direction == Direction.LONG:
            return price >= tp_level
        return price <= tp_level

    def _check_sl_reached(self, direction: Direction, price: Decimal, sl_level: Decimal) -> bool:
        if direction == Direction.LONG:
            return price <= sl_level
        return price >= sl_level

    def _get_tp1_level(self, direction: Direction, tp_levels: list[Decimal]) -> Decimal | None:
        if not tp_levels:
            return None
        if direction == Direction.LONG:
            return min(tp_levels)
        return max(tp_levels)

    async def _start_user_data_stream(self) -> None:
        print("[PriceWatcher] User data stream loop started")
        if not self.trade_engine:
            print("[PriceWatcher] user stream disabled (trade_engine missing)")
            return
        while self._running:
            try:
                listen_key = self._create_futures_listen_key()
                if not listen_key:
                    await asyncio.sleep(5)
                    continue
                self._listen_key = listen_key
                ws_url = self._build_futures_user_ws_url(listen_key)
                self._listen_key_task = asyncio.create_task(self._keepalive_futures_listen_key(listen_key))
                print(f"[PriceWatcher] user stream connected url={ws_url}")
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(ws_url, heartbeat=20) as ws:
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            await self._handle_user_stream_payload(msg.json())
            except Exception as exc:
                print(f"[PriceWatcher] user stream error: {exc}")
            finally:
                if self._listen_key_task:
                    self._listen_key_task.cancel()
                    with contextlib.suppress(Exception):
                        await self._listen_key_task
                    self._listen_key_task = None
                if self._listen_key:
                    self._close_futures_listen_key(self._listen_key)
                    self._listen_key = None
            await asyncio.sleep(3)

    async def _handle_user_stream_payload(self, payload: dict) -> None:
        event_type = payload.get("e")
        if event_type != "ORDER_TRADE_UPDATE":
            return
        order_data = payload.get("o")
        if not isinstance(order_data, dict):
            return
        order_id = order_data.get("i")
        order_type = order_data.get("o")
        status = order_data.get("X")
        pair = order_data.get("s")
        side = order_data.get("S")
        reduce_only = self._to_bool(order_data.get("R"))
        close_position = self._to_bool(order_data.get("cp"))
        if order_id is None or not order_type or not status or not pair:
            return
        await self._handle_order_update(
            str(order_id),
            str(order_type),
            str(status),
            str(pair),
            side=str(side) if side is not None else None,
            reduce_only=reduce_only,
            close_position=close_position,
        )

    @staticmethod
    def _to_bool(value) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y"}
        if value is None:
            return False
        return bool(value)

    def _create_futures_listen_key(self) -> str | None:
        client = self.trade_engine.client
        methods = (
            "new_listen_key",
            "futures_stream_get_listen_key",
            "futures_get_listen_key",
        )
        for method_name in methods:
            fn = getattr(client, method_name, None)
            if not callable(fn):
                continue
            try:
                resp = fn()
            except Exception:
                continue
            if isinstance(resp, dict):
                key = resp.get("listenKey")
                if key:
                    return str(key)
            if isinstance(resp, str) and resp.strip():
                return resp.strip()
        return None

    async def _keepalive_futures_listen_key(self, listen_key: str) -> None:
        while self._running and self._listen_key == listen_key:
            await asyncio.sleep(30 * 60)
            self._keepalive_listen_key_once(listen_key)

    def _keepalive_listen_key_once(self, listen_key: str) -> None:
        client = self.trade_engine.client
        methods = (
            "renew_listen_key",
            "futures_stream_keepalive",
            "futures_keepalive",
        )
        for method_name in methods:
            fn = getattr(client, method_name, None)
            if not callable(fn):
                continue
            try:
                fn(listenKey=listen_key)
                return
            except TypeError:
                try:
                    fn()
                    return
                except Exception:
                    continue
            except Exception:
                continue

    def _close_futures_listen_key(self, listen_key: str) -> None:
        client = self.trade_engine.client
        methods = (
            "close_listen_key",
            "futures_stream_close",
            "futures_close_listen_key",
        )
        for method_name in methods:
            fn = getattr(client, method_name, None)
            if not callable(fn):
                continue
            try:
                fn(listenKey=listen_key)
                return
            except TypeError:
                try:
                    fn()
                    return
                except Exception:
                    continue
            except Exception:
                continue

    def _build_futures_user_ws_url(self, listen_key: str) -> str:
        client = self.trade_engine.client
        base = getattr(client, "base_url", None) or getattr(client, "BASE_URL", None) or getattr(client, "FUTURES_URL", None)
        if isinstance(base, str) and base:
            host = urlparse(base).netloc.lower()
            if "testnet" in host or "binancefuture.com" in host:
                return f"wss://stream.binancefuture.com/ws/{listen_key}"
            if "demo-fapi.binance.com" in host:
                return f"wss://fstream.binance.com/ws/{listen_key}"
        return f"wss://fstream.binance.com/ws/{listen_key}"

    async def _handle_order_update(
        self,
        order_id: str,
        order_type: str,
        status: str,
        pair: str,
        side: str | None = None,
        reduce_only: bool = False,
        close_position: bool = False,
    ) -> None:
        print(
            "[PriceWatcher] order_update "
            f"order_id={order_id} type={order_type} side={side} status={status} "
            f"reduce_only={reduce_only} close_position={close_position} pair={pair}"
        )
        limit_fill_handled = False
        if order_id in self._pending_limit_orders and order_type == "LIMIT" and status == "FILLED":
            action = self._pending_limit_orders.pop(order_id)
            self._pending_order_missing_count.pop(order_id, None)
            pair_for_pos = action.pair or pair
            promote_pending = getattr(self.position_manager, "promote_pending_position", None)
            pos = await promote_pending(pair_for_pos) if callable(promote_pending) else None
            if not pos:
                pos = self.position_manager.get_position(pair_for_pos)
            if pos and self.trade_engine:
                print(f"[PriceWatcher] limit filled -> set_tp_sl_orders pair={action.pair}")
                await self._safe_alert("notify_order_filled", pair, "limit", str(pos.entry_price))
                await self.trade_engine._set_tp_sl_orders(pos)
            if pos:
                final_tp = None
                if pos.tp_levels:
                    if self.trade_engine and hasattr(self.trade_engine, "_get_final_tp_level"):
                        final_tp = self.trade_engine._get_final_tp_level(pos.direction, pos.tp_levels)
                    else:
                        final_tp = max(pos.tp_levels) if pos.direction == Direction.LONG else min(pos.tp_levels)
                await self._safe_alert(
                    "notify_tp_sl_set",
                    pair,
                    str(final_tp) if final_tp is not None else None,
                    str(pos.current_sl) if pos.current_sl is not None else None,
                    "watcher_limit_fill",
                )
            limit_fill_handled = True
        if order_id in self._pending_limit_orders and order_type == "LIMIT" and status in {"CANCELED", "EXPIRED", "REJECTED"}:
            self._pending_limit_orders.pop(order_id, None)
            self._pending_order_missing_count.pop(order_id, None)
            has_pending = getattr(self.position_manager, "has_pending_position", None)
            pending_exists = bool(callable(has_pending) and has_pending(pair))
            if self.position_manager.get_position(pair) or pending_exists:
                await self.position_manager.remove_position(pair)
            await self.unsubscribe(pair)
            await self._safe_alert("notify_closed", pair, "cancel")
        if order_type == "LIMIT" and status == "FILLED" and not limit_fill_handled:
            pos = self.position_manager.get_position(pair)
            if not pos:
                promote_pending = getattr(self.position_manager, "promote_pending_position", None)
                promoted = await promote_pending(pair) if callable(promote_pending) else None
                pos = promoted if promoted else self.position_manager.get_position(pair)
            if pos and self.trade_engine:
                await self._safe_alert("notify_order_filled", pair, "limit", str(pos.entry_price))
                await self.trade_engine._set_tp_sl_orders(pos)
        if order_type in {"TAKE_PROFIT_MARKET", "STOP_MARKET"} and status == "FILLED":
            close_type = "TP" if order_type == "TAKE_PROFIT_MARKET" else "SL"
            print(f"[PriceWatcher] protection order filled -> close_type={close_type} pair={pair}")
            await self._handle_position_closed(pair, close_type)
            return

        if status == "FILLED" and (reduce_only or close_position):
            print(f"[PriceWatcher] close order filled -> pair={pair} side={side}")
            await self._handle_position_closed(pair, "manual_close")

    async def _handle_position_closed(self, pair: str, close_type: str) -> None:
        print(f"[PriceWatcher] handle_position_closed pair={pair} close_type={close_type}")
        await self.unsubscribe(pair)
        if self.trade_engine and hasattr(self.trade_engine, "_cleanup_protection_orders"):
            try:
                await self.trade_engine._cleanup_protection_orders(pair)
                print(f"[PriceWatcher] cleanup_protection_orders done pair={pair}")
            except Exception:
                pass
        await self.position_manager.remove_position(pair)
        await self._safe_alert("notify_closed", pair, close_type)

    async def handle_order_update(
        self,
        order_id: str,
        order_type: str,
        status: str,
        pair: str,
        side: str | None = None,
        reduce_only: bool = False,
        close_position: bool = False,
    ) -> None:
        await self._handle_order_update(
            order_id,
            order_type,
            status,
            pair,
            side=side,
            reduce_only=reduce_only,
            close_position=close_position,
        )
