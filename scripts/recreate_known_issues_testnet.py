from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import sqlite3
import sys
import tempfile
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN

from binance.client import Client
from binance.exceptions import BinanceAPIException
from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError

# Ensure project root is importable when script is executed directly.
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from CaraCrypto.config import load_config
from CaraCrypto.database import Database


def _round_down(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _extract_filter(symbol_info: dict, filter_type: str) -> dict:
    for row in symbol_info.get("filters", []):
        if row.get("filterType") == filter_type:
            return row
    return {}


@dataclass
class TestnetContext:
    symbol: str
    qty: Decimal
    tick: Decimal
    mark_price: Decimal
    tp_price: Decimal
    sl_price: Decimal
    sl_price_2: Decimal


def _build_testnet_client() -> Client:
    load_dotenv(".env")
    key = (os.getenv("BINANCE_API_KEY") or "").strip()
    secret = (os.getenv("BINANCE_API_SECRET") or "").strip()
    if not key or not secret:
        raise RuntimeError("Missing BINANCE_API_KEY/BINANCE_API_SECRET in .env")
    client = Client(key, secret)
    client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"
    client.FUTURES_DATA_URL = "https://testnet.binancefuture.com/futures/data"
    server = client.futures_time()["serverTime"]
    client.timestamp_offset = int(server - int(time.time() * 1000))
    return client


def _build_context(client: Client, symbol: str, quote_size: Decimal) -> TestnetContext:
    info = client.futures_exchange_info()
    sym = next((s for s in info.get("symbols", []) if s.get("symbol") == symbol), None)
    if not sym:
        raise RuntimeError(f"Symbol not found: {symbol}")
    lot = _extract_filter(sym, "LOT_SIZE")
    price = _extract_filter(sym, "PRICE_FILTER")
    notional = _extract_filter(sym, "MIN_NOTIONAL")

    step = Decimal(lot.get("stepSize", "0.001"))
    tick = Decimal(price.get("tickSize", "0.1"))
    mark = Decimal(client.futures_mark_price(symbol=symbol)["markPrice"])
    min_notional = Decimal(notional.get("notional", "0")) if notional else Decimal("0")

    raw_qty = quote_size / mark
    qty = _round_down(raw_qty, step)
    if min_notional > 0 and qty * mark < min_notional:
        qty = _round_down((min_notional / mark) * Decimal("1.02"), step)
    if qty <= 0:
        raise RuntimeError("qty rounded to 0; increase --quote-size")

    tp = _round_down(mark * Decimal("1.01"), tick)
    sl = _round_down(mark * Decimal("0.99"), tick)
    sl2 = _round_down(mark * Decimal("0.995"), tick)
    return TestnetContext(symbol=symbol, qty=qty, tick=tick, mark_price=mark, tp_price=tp, sl_price=sl, sl_price_2=sl2)


def _cancel_all_conditional(client: Client, symbol: str) -> None:
    try:
        opens = client.futures_get_open_orders(symbol=symbol)
    except Exception:
        return
    for row in opens:
        if row.get("type") in {"TAKE_PROFIT_MARKET", "STOP_MARKET", "TAKE_PROFIT", "STOP"}:
            oid = row.get("orderId")
            if oid is None:
                continue
            try:
                client.futures_cancel_order(symbol=symbol, orderId=oid)
            except Exception:
                pass


def _close_position_market(client: Client, symbol: str) -> None:
    pos = client.futures_position_information(symbol=symbol)
    if not pos:
        return
    amt = Decimal(pos[0].get("positionAmt", "0"))
    if amt == 0:
        return
    side = "SELL" if amt > 0 else "BUY"
    qty = abs(amt)
    client.futures_create_order(
        symbol=symbol,
        side=side,
        type="MARKET",
        quantity=str(qty),
        reduceOnly="true",
    )


def run_binance_4130(symbol: str, quote_size: Decimal, execute: bool) -> int:
    client = _build_testnet_client()
    ctx = _build_context(client, symbol, quote_size)
    print("Scenario: Binance -4130 duplicate closePosition order")
    print(
        {
            "symbol": ctx.symbol,
            "qty": str(ctx.qty),
            "mark": str(ctx.mark_price),
            "tp": str(ctx.tp_price),
            "sl_1": str(ctx.sl_price),
            "sl_2": str(ctx.sl_price_2),
            "mode": "execute" if execute else "dry-run",
        }
    )
    if not execute:
        print("Dry-run selesai. Tambahkan --execute untuk benar-benar kirim order testnet.")
        return 0

    try:
        _cancel_all_conditional(client, ctx.symbol)
        print("1) Open MARKET long")
        client.futures_create_order(symbol=ctx.symbol, side="BUY", type="MARKET", quantity=str(ctx.qty))

        print("2) Place TP + SL (closePosition=true)")
        client.futures_create_order(
            symbol=ctx.symbol,
            side="SELL",
            type="TAKE_PROFIT_MARKET",
            stopPrice=str(ctx.tp_price),
            closePosition="true",
            workingType="MARK_PRICE",
        )
        client.futures_create_order(
            symbol=ctx.symbol,
            side="SELL",
            type="STOP_MARKET",
            stopPrice=str(ctx.sl_price),
            closePosition="true",
            workingType="MARK_PRICE",
        )

        print("3) Recreate bug: place another SL closePosition without cancel old SL")
        try:
            client.futures_create_order(
                symbol=ctx.symbol,
                side="SELL",
                type="STOP_MARKET",
                stopPrice=str(ctx.sl_price_2),
                closePosition="true",
                workingType="MARK_PRICE",
            )
            print("WARNING: duplicate SL accepted; -4130 did not trigger this run.")
            return 1
        except BinanceAPIException as exc:
            print(f"Caught BinanceAPIException code={exc.code} msg={exc.message}")
            if exc.code == -4130:
                print("SUCCESS: error -4130 reproduced.")
                return 0
            return 2
    finally:
        print("Cleanup: cancel protection + close position")
        _cancel_all_conditional(client, ctx.symbol)
        try:
            _close_position_market(client, ctx.symbol)
        except Exception as exc:
            print(f"cleanup close warn: {exc}")
        _cancel_all_conditional(client, ctx.symbol)


async def run_db_duplicate_message(group_id: int, message_id: int) -> int:
    print("Scenario: DB duplicate message insert (uq_message_group)")
    try:
        cfg = load_config()
        db = Database(cfg.database.url)
        await db.connect()
        payload = {
            "message_id": message_id,
            "group_id": group_id,
            "topic_id": 4,
            "text": "[CANCEL] REPRO",
            "reply_to_message_id": None,
        }
        first_id = await db.store_message(payload)
        print(f"First insert ok (postgres): id={first_id}")
        try:
            await db.store_message(payload)
            print("WARNING: duplicate insert succeeded unexpectedly.")
            return 1
        except IntegrityError as exc:
            print(f"Caught IntegrityError (postgres): {exc.__class__.__name__}")
            print("SUCCESS: uq_message_group violation reproduced.")
            return 0
    except Exception as exc:
        print(f"Postgres unavailable, fallback to local sqlite: {exc.__class__.__name__}")
        with tempfile.NamedTemporaryFile(prefix="caracrypto_repro_", suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        con = sqlite3.connect(db_path)
        try:
            con.execute(
                """
                CREATE TABLE messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER NOT NULL,
                    group_id INTEGER NOT NULL,
                    topic_id INTEGER NULL,
                    text TEXT NOT NULL,
                    reply_to_message_id INTEGER NULL,
                    UNIQUE(message_id, group_id)
                )
                """
            )
            con.execute(
                "INSERT INTO messages (message_id, group_id, topic_id, text, reply_to_message_id) VALUES (?, ?, ?, ?, ?)",
                (message_id, group_id, 4, "[CANCEL] REPRO", None),
            )
            print(f"First insert ok (sqlite): db={db_path}")
            try:
                con.execute(
                    "INSERT INTO messages (message_id, group_id, topic_id, text, reply_to_message_id) VALUES (?, ?, ?, ?, ?)",
                    (message_id, group_id, 4, "[CANCEL] REPRO", None),
                )
                con.commit()
                print("WARNING: duplicate insert succeeded unexpectedly on sqlite.")
                return 1
            except sqlite3.IntegrityError:
                print("Caught IntegrityError (sqlite): UNIQUE constraint failed: messages.message_id, messages.group_id")
                print("SUCCESS: uq_message_group-equivalent violation reproduced via sqlite fallback.")
                return 0
        finally:
            con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Recreate known issues safely (testnet + DB).")
    sub = parser.add_subparsers(dest="scenario", required=True)

    p_4130 = sub.add_parser("binance-4130", help="recreate Binance API error -4130 on futures testnet")
    p_4130.add_argument("--symbol", default="BTCUSDT")
    p_4130.add_argument("--quote-size", default="120", help="target notional in USDT")
    p_4130.add_argument("--execute", action="store_true", help="actually place/cancel testnet orders")

    p_db = sub.add_parser("db-duplicate-message", help="recreate uq_message_group duplicate insert")
    p_db.add_argument("--group-id", type=int, default=-1002647537685)
    p_db.add_argument("--message-id", type=int, default=95249001)

    args = parser.parse_args()
    if args.scenario == "binance-4130":
        return run_binance_4130(args.symbol.upper(), Decimal(str(args.quote_size)), args.execute)
    return asyncio.run(run_db_duplicate_message(args.group_id, args.message_id))


if __name__ == "__main__":
    raise SystemExit(main())
