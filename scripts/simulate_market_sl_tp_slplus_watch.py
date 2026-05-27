# Tujuan
# Simulasi end-to-end futures demo: market open, set TP/SL, SL+ update, lalu monitor close manual dan cleanup watcher.
# Caller
# Operator lokal via terminal.
# Dependensi
# python-binance, python-dotenv, modul internal CaraCrypto.
# Main Functions
# Menjalankan flow trading + monitor sampai user menutup posisi manual di Binance.
# Side Effects
# Membuat order market/conditional pada akun futures demo dan dapat membatalkan order proteksi.

from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from types import SimpleNamespace

from binance.client import Client
from dotenv import load_dotenv

# Ensure project root is importable when script is executed directly.
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from CaraCrypto.models import Direction, RunningPosition, TradeAction, GeminiAction, RiskLevel
from CaraCrypto.price_watcher import PriceWatcher
from CaraCrypto.trade_engine import TradeEngine


def _round_price(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).to_integral_value(rounding=ROUND_DOWN) * tick


def _round_qty(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _extract_filter(symbol_info: dict, filter_type: str) -> dict:
    for f in symbol_info.get("filters", []):
        if f.get("filterType") == filter_type:
            return f
    return {}


class _Alert:
    async def send_alert(self, msg: str) -> None:
        print("[ALERT]", msg)

    async def notify_modification(self, pair: str, action_type: str, details: str) -> None:
        print("[MOD]", pair, action_type, details)

    async def notify_new_order(self, pair: str, direction: str, price: str, order_type: str) -> None:
        print("[ORDER]", pair, direction, order_type, price)

    async def notify_risk_limit(self, message: str) -> None:
        print("[RISK]", message)

    async def notify_tp_hit(self, pair: str, level: str) -> None:
        print("[TP HIT]", pair, level)

    async def notify_sl_hit(self, pair: str, level: str) -> None:
        print("[SL HIT]", pair, level)


class _DB:
    async def get_daily_loss(self, _):
        return Decimal("0")

    async def store_modification_log(self, *args, **kwargs):
        return None


class _PM:
    def __init__(self):
        self._pos = {}

    async def add_position(self, p: RunningPosition):
        self._pos[p.pair] = p

    async def remove_position(self, pair: str):
        self._pos.pop(pair, None)

    async def update_sl(self, pair: str, sl):
        if pair in self._pos:
            self._pos[pair].current_sl = sl

    def get_position(self, pair: str):
        return self._pos.get(pair)

    def has_position(self, pair: str):
        return pair in self._pos

    def get_running_pairs(self):
        return list(self._pos.keys())


@dataclass
class Context:
    symbol: str
    qty: Decimal
    mark_price: Decimal
    tp_price: Decimal
    sl_price: Decimal
    sl_plus_price: Decimal


def build_client() -> Client:
    load_dotenv(".env")
    key = (os.getenv("BINANCE_API_KEY") or "").strip()
    secret = (os.getenv("BINANCE_API_SECRET") or "").strip()
    if not key or not secret:
        raise RuntimeError("Missing BINANCE_API_KEY/BINANCE_API_SECRET in .env")
    c = Client(key, secret)
    c.FUTURES_URL = "https://demo-fapi.binance.com/fapi"
    c.FUTURES_DATA_URL = "https://demo-fapi.binance.com/futures/data"
    server = c.futures_time()["serverTime"]
    c.timestamp_offset = int(server - int(time.time() * 1000))
    return c


def build_context(client: Client, symbol: str, qty: Decimal, tp_mult: Decimal, sl_mult: Decimal, sl_plus_mult: Decimal) -> Context:
    info = client.futures_exchange_info()
    s = next(x for x in info["symbols"] if x["symbol"] == symbol)
    tick = Decimal(_extract_filter(s, "PRICE_FILTER").get("tickSize", "0.1"))
    step = Decimal(_extract_filter(s, "LOT_SIZE").get("stepSize", "0.001"))
    qty = _round_qty(qty, step)
    mark = Decimal(client.futures_mark_price(symbol=symbol)["markPrice"])
    tp = _round_price(mark * tp_mult, tick)
    sl = _round_price(mark * sl_mult, tick)
    sl_plus = _round_price(mark * sl_plus_mult, tick)
    return Context(symbol=symbol, qty=qty, mark_price=mark, tp_price=tp, sl_price=sl, sl_plus_price=sl_plus)


def place_initial_orders(client: Client, ctx: Context):
    print("1) Open MARKET BUY")
    m = client.futures_create_order(symbol=ctx.symbol, side="BUY", type="MARKET", quantity=str(ctx.qty))
    print("market:", {"orderId": m.get("orderId"), "status": m.get("status")})

    print("2) Set TP/SL (conditional closePosition)")
    tp = client.futures_create_order(
        symbol=ctx.symbol,
        side="SELL",
        type="TAKE_PROFIT_MARKET",
        stopPrice=str(ctx.tp_price),
        closePosition="true",
        workingType="MARK_PRICE",
    )
    sl = client.futures_create_order(
        symbol=ctx.symbol,
        side="SELL",
        type="STOP_MARKET",
        stopPrice=str(ctx.sl_price),
        closePosition="true",
        workingType="MARK_PRICE",
    )
    print("tp algo:", {"algoId": tp.get("algoId"), "triggerPrice": tp.get("triggerPrice"), "algoStatus": tp.get("algoStatus")})
    print("sl algo:", {"algoId": sl.get("algoId"), "triggerPrice": sl.get("triggerPrice"), "algoStatus": sl.get("algoStatus")})
    return tp, sl


def simulate_sl_plus(client: Client, ctx: Context, sl_algo_id: int | None):
    print("3) Simulate SL replace (cancel old SL, set same SL price)")
    # Behavior sesuai request: cancel SL lama, lalu pasang kembali SL dengan harga yang sama.
    if sl_algo_id:
        try:
            client._request_futures_api(
                "delete",
                "algoOrder",
                True,
                data={"symbol": ctx.symbol, "algoId": sl_algo_id},
            )
            print("old SL algo canceled:", sl_algo_id)
        except Exception as e:
            print("warn: failed cancel old SL algo:", str(e))
            print("skip placing new SL to avoid duplicate conditional order")
            return

    same_sl_price = ctx.sl_price
    sl2 = client.futures_create_order(
        symbol=ctx.symbol,
        side="SELL",
        type="STOP_MARKET",
        stopPrice=str(same_sl_price),
        closePosition="true",
        workingType="MARK_PRICE",
    )
    print("sl replaced:", {"algoId": sl2.get("algoId"), "triggerPrice": sl2.get("triggerPrice"), "algoStatus": sl2.get("algoStatus")})


async def monitor_manual_close(client: Client, watcher: PriceWatcher, pm: _PM, symbol: str, poll_sec: int):
    print("4) Monitor manual close. Silakan close posisi di Binance UI.")
    print("   Script akan auto trigger cleanup watcher saat position amount = 0.")
    while True:
        pos = client.futures_position_information(symbol=symbol)
        amt = Decimal(pos[0]["positionAmt"]) if pos else Decimal("0")
        if amt == 0:
            print("Position detected closed manually.")
            await watcher._handle_position_closed(symbol, "MANUAL")
            break
        await asyncio.sleep(poll_sec)

    remaining = client.futures_get_open_orders(symbol=symbol)
    print("5) Remaining open orders after cleanup:", len(remaining))
    for o in remaining:
        print({"orderId": o.get("orderId"), "type": o.get("type"), "status": o.get("status")})


async def main():
    parser = argparse.ArgumentParser(description="Simulasi market->TP/SL->SL+ lalu monitor close manual & cleanup.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--qty", default="0.0034")
    parser.add_argument("--tp-multiplier", default="1.01")
    parser.add_argument("--sl-multiplier", default="0.99")
    parser.add_argument("--sl-plus-multiplier", default="1.0001")
    parser.add_argument("--poll-sec", type=int, default=3)
    args = parser.parse_args()

    client = build_client()
    print("Health:", {"canTrade": client.futures_account().get("canTrade"), "wallet": client.futures_account().get("totalWalletBalance")})

    ctx = build_context(
        client,
        args.symbol.upper(),
        Decimal(args.qty),
        Decimal(args.tp_multiplier),
        Decimal(args.sl_multiplier),
        Decimal(args.sl_plus_multiplier),
    )

    _, sl = place_initial_orders(client, ctx)
    simulate_sl_plus(client, ctx, sl.get("algoId"))

    # Wire lightweight engine+watcher so cleanup path sama seperti kode utama.
    alert = _Alert()
    pm = _PM()
    db = _DB()
    risk = SimpleNamespace(
        trade_margin_percent=1.0,
        high_risk_multiplier=0.5,
        max_concurrent_positions=5,
        max_position_size_percent=200.0,
        daily_loss_limit_percent=5.0,
    )
    engine = TradeEngine(client, db, alert, pm, risk)
    watcher = PriceWatcher(alert, pm)
    watcher.trade_engine = engine
    engine.price_watcher = watcher

    await pm.add_position(
        RunningPosition(
            pair=ctx.symbol,
            direction=Direction.LONG,
            entry_price=ctx.mark_price,
            current_sl=ctx.sl_plus_price,
            tp_levels=[ctx.tp_price],
            leverage=50,
            order_id=f"manual-{int(datetime.now(timezone.utc).timestamp())}",
            quantity=ctx.qty,
            opened_at=datetime.now(timezone.utc),
        )
    )

    await watcher.subscribe(ctx.symbol)
    await monitor_manual_close(client, watcher, pm, ctx.symbol, args.poll_sec)
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
