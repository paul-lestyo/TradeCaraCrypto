# Tujuan
# Menjalankan simulasi trading "real" menggunakan kredensial dari .env (tanpa dijalankan otomatis oleh agent).
# Caller
# Developer/operator lokal via terminal.
# Dependensi
# python-binance, python-dotenv.
# Main Functions
# health check koneksi, auth check, dan safe order test (limit far + cancel) untuk Spot Demo/Futures Demo.
# Side Effects
# Dapat membuat dan membatalkan order bila flag --do-order digunakan.

from __future__ import annotations

import argparse
import os
import sys
import time
from decimal import Decimal, ROUND_DOWN

from binance.client import Client
from binance.exceptions import BinanceAPIException
from dotenv import load_dotenv


def _require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"Missing env: {name}")
    return value


def _round_price(price: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return price
    return (price / tick).to_integral_value(rounding=ROUND_DOWN) * tick


def _round_qty(qty: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return qty
    return (qty / step).to_integral_value(rounding=ROUND_DOWN) * step


def _extract_filter(symbol_info: dict, filter_type: str) -> dict:
    for f in symbol_info.get("filters", []):
        if f.get("filterType") == filter_type:
            return f
    return {}


def _quantize_up(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    units = (value / step).to_integral_value(rounding=ROUND_DOWN)
    if units * step < value:
        units += 1
    return units * step


def _client_from_env() -> Client:
    load_dotenv(".env")
    key = _require_env("BINANCE_API_KEY")
    secret = _require_env("BINANCE_API_SECRET")
    return Client(key, secret)


def run_spot_demo(args: argparse.Namespace) -> int:
    client = _client_from_env()
    client.API_URL = "https://demo-api.binance.com/api"

    print("=== Spot Demo Health Check ===")
    print("ping:", client.ping())
    server_time = client.get_server_time()["serverTime"]
    offset = int(server_time - int(time.time() * 1000))
    client.timestamp_offset = offset
    print("server_time:", server_time, "offset:", offset)

    account = client.get_account()
    print("account_type:", account.get("accountType"), "balances_count:", len(account.get("balances", [])))

    if not args.do_order:
        print("skip order test (use --do-order to place+cancel)")
        return 0

    symbol = args.symbol.upper()
    book = client.get_order_book(symbol=symbol, limit=5)
    best_bid = Decimal(book["bids"][0][0])
    price = best_bid * Decimal(args.price_factor)

    exch = client.get_exchange_info()
    sym_info = next((s for s in exch["symbols"] if s["symbol"] == symbol), None)
    if not sym_info:
        raise RuntimeError(f"symbol not found: {symbol}")

    price_filter = _extract_filter(sym_info, "PRICE_FILTER")
    lot_filter = _extract_filter(sym_info, "LOT_SIZE")
    percent_filter = _extract_filter(sym_info, "PERCENT_PRICE_BY_SIDE")
    tick = Decimal(price_filter.get("tickSize", "0.01"))
    step = Decimal(lot_filter.get("stepSize", "0.000001"))

    qty = _round_qty(Decimal(args.qty), step)
    px = _round_price(price, tick)

    # Spot demo enforces PERCENT_PRICE_BY_SIDE.
    if percent_filter:
        avg_price = Decimal(client.get_avg_price(symbol=symbol)["price"])
        bid_down = Decimal(percent_filter.get("bidMultiplierDown", "0"))
        bid_up = Decimal(percent_filter.get("bidMultiplierUp", "999999"))
        min_px = _round_price(avg_price * bid_down, tick)
        max_px = _round_price(avg_price * bid_up, tick)
        if px < min_px:
            px = min_px
        if px > max_px:
            px = max_px

    print("placing SPOT LIMIT BUY:", {"symbol": symbol, "qty": str(qty), "price": str(px)})
    order = client.create_order(
        symbol=symbol,
        side="BUY",
        type="LIMIT",
        timeInForce="GTC",
        quantity=str(qty),
        price=str(px),
    )
    order_id = order["orderId"]
    print("created order:", order_id, order.get("status"))

    canceled = client.cancel_order(symbol=symbol, orderId=order_id)
    print("canceled order:", canceled.get("orderId"), canceled.get("status"))
    return 0


def run_futures_demo(args: argparse.Namespace) -> int:
    client = _client_from_env()
    client.FUTURES_URL = "https://demo-fapi.binance.com/fapi"
    client.FUTURES_DATA_URL = "https://demo-fapi.binance.com/futures/data"

    print("=== Futures Demo Health Check ===")
    server_time = client.futures_time()["serverTime"]
    offset = int(server_time - int(time.time() * 1000))
    client.timestamp_offset = offset
    print("server_time:", server_time, "offset:", offset)

    account = client.futures_account()
    print("canTrade:", account.get("canTrade"), "wallet:", account.get("totalWalletBalance"))

    if args.tp_sl_flow:
        return run_futures_tp_sl_flow(client, args)

    if not args.do_order:
        print("skip order test (use --do-order to place+cancel)")
        return 0

    symbol = args.symbol.upper()
    book = client.futures_order_book(symbol=symbol, limit=5)
    best_bid = Decimal(book["bids"][0][0])
    price = best_bid * Decimal(args.price_factor)

    exch = client.futures_exchange_info()
    sym_info = next((s for s in exch["symbols"] if s["symbol"] == symbol), None)
    if not sym_info:
        raise RuntimeError(f"symbol not found: {symbol}")

    price_filter = _extract_filter(sym_info, "PRICE_FILTER")
    lot_filter = _extract_filter(sym_info, "LOT_SIZE")
    notional_filter = _extract_filter(sym_info, "MIN_NOTIONAL")
    tick = Decimal(price_filter.get("tickSize", "0.1"))
    step = Decimal(lot_filter.get("stepSize", "0.001"))

    qty = _round_qty(Decimal(args.qty), step)
    px = _round_price(price, tick)

    # Futures demo enforces minimum notional (often 50 USDT).
    min_notional = Decimal(notional_filter.get("notional", "0")) if notional_filter else Decimal("0")
    if min_notional > 0 and qty * px < min_notional:
        needed_qty = _quantize_up(min_notional / px, step)
        qty = needed_qty

    print("placing FUTURES LIMIT BUY:", {"symbol": symbol, "qty": str(qty), "price": str(px)})
    order = client.futures_create_order(
        symbol=symbol,
        side="BUY",
        type="LIMIT",
        timeInForce="GTC",
        quantity=str(qty),
        price=str(px),
    )
    order_id = order["orderId"]
    print("created order:", order_id, order.get("status"))

    canceled = client.futures_cancel_order(symbol=symbol, orderId=order_id)
    print("canceled order:", canceled.get("orderId"), canceled.get("status"))
    return 0


def run_futures_tp_sl_flow(client: Client, args: argparse.Namespace) -> int:
    symbol = args.symbol.upper()
    qty = Decimal(args.qty)

    exch = client.futures_exchange_info()
    sym_info = next((s for s in exch["symbols"] if s["symbol"] == symbol), None)
    if not sym_info:
        raise RuntimeError(f"symbol not found: {symbol}")

    lot_filter = _extract_filter(sym_info, "LOT_SIZE")
    step = Decimal(lot_filter.get("stepSize", "0.001"))
    qty = _round_qty(qty, step)
    if qty <= 0:
        raise RuntimeError("quantity invalid after step rounding")

    print("=== Futures TP/SL Flow ===")
    print("1) Open MARKET position")
    open_order = client.futures_create_order(
        symbol=symbol,
        side="BUY",
        type="MARKET",
        quantity=str(qty),
    )
    print("opened:", {"orderId": open_order.get("orderId"), "status": open_order.get("status")})

    mark_price = Decimal(client.futures_mark_price(symbol=symbol)["markPrice"])
    tp_price = _round_price(mark_price * Decimal(str(args.tp_multiplier)), Decimal("0.1"))
    sl_price = _round_price(mark_price * Decimal(str(args.sl_multiplier)), Decimal("0.1"))

    print("2) Place TP/SL closePosition orders")
    tp = client.futures_create_order(
        symbol=symbol,
        side="SELL",
        type="TAKE_PROFIT_MARKET",
        stopPrice=str(tp_price),
        closePosition="true",
        workingType="MARK_PRICE",
    )
    sl = client.futures_create_order(
        symbol=symbol,
        side="SELL",
        type="STOP_MARKET",
        stopPrice=str(sl_price),
        closePosition="true",
        workingType="MARK_PRICE",
    )
    print("tp_raw:", tp)
    print("sl_raw:", sl)
    print("tp:", {"orderId": tp.get("orderId"), "status": tp.get("status"), "stopPrice": str(tp_price)})
    print("sl:", {"orderId": sl.get("orderId"), "status": sl.get("status"), "stopPrice": str(sl_price)})

    if args.simulate_sl_plus:
        print("2b) Simulate SL+ (replace old SL with new SL at/near breakeven)")
        # IMPORTANT:
        # Response untuk conditional closePosition di Futures Demo memakai algoId,
        # bukan orderId biasa. Jadi cancel harus via endpoint algo-order.
        old_sl_algo_id = sl.get("algoId")
        canceled = False
        if old_sl_algo_id:
            # python-binance tidak expose wrapper khusus di semua versi;
            # coba beberapa path yang dipakai variasi environment demo.
            for path in ("algoOrder", "algo/order", "algo-order"):
                try:
                    client._request_futures_api(
                        "delete",
                        path,
                        True,
                        2,
                        data={"symbol": symbol, "algoId": old_sl_algo_id},
                    )
                    print("canceled old SL algo:", old_sl_algo_id, "via", path)
                    canceled = True
                    break
                except Exception as e:
                    print(f"warn: cancel via {path} failed:", str(e))
        if not canceled:
            # Fallback aman untuk flow simulasi ini:
            # cancel semua open orders symbol (di flow ini hanya TP/SL protection).
            try:
                client.futures_cancel_all_open_orders(symbol=symbol)
                print("fallback cancel_all_open_orders executed")
                canceled = True
            except Exception as e:
                print("warn: fallback cancel_all_open_orders failed:", str(e))

        new_sl_price = _round_price(mark_price * Decimal(str(args.sl_plus_multiplier)), Decimal("0.1"))
        # Recreate TP (karena bisa ikut ter-cancel di fallback).
        tp2 = client.futures_create_order(
            symbol=symbol,
            side="SELL",
            type="TAKE_PROFIT_MARKET",
            stopPrice=str(tp_price),
            closePosition="true",
            workingType="MARK_PRICE",
        )
        sl2 = client.futures_create_order(
            symbol=symbol,
            side="SELL",
            type="STOP_MARKET",
            stopPrice=str(new_sl_price),
            closePosition="true",
            workingType="MARK_PRICE",
        )
        print("recreated TP raw:", tp2)
        print("new SL raw (SL+):", sl2)
        print("new SL (SL+):", {"algoId": sl2.get("algoId"), "algoStatus": sl2.get("algoStatus"), "stopPrice": str(new_sl_price)})

    open_orders = client.futures_get_open_orders(symbol=symbol)
    print("3) Open orders snapshot:")
    for o in open_orders:
        if o.get("type") in {"TAKE_PROFIT_MARKET", "STOP_MARKET"}:
            print(
                {
                    "orderId": o.get("orderId"),
                    "type": o.get("type"),
                    "side": o.get("side"),
                    "stopPrice": o.get("stopPrice"),
                    "status": o.get("status"),
                }
            )

    print("\nKonfirmasi Anda dibutuhkan:")
    print("1 = close position now (market) + cleanup TP/SL")
    print("2 = keep open (no close)")
    choice = input("Pilih opsi [1/2]: ").strip()

    if choice != "1":
        print("Posisi tetap terbuka. Tidak ada close yang dilakukan.")
        return 0

    print("4) Closing position now...")
    close_order = client.futures_create_order(
        symbol=symbol,
        side="SELL",
        type="MARKET",
        quantity=str(qty),
        reduceOnly="true",
    )
    print("close_order:", {"orderId": close_order.get("orderId"), "status": close_order.get("status")})

    # Cleanup any remaining TP/SL orders.
    remaining = client.futures_get_open_orders(symbol=symbol)
    for o in remaining:
        if o.get("type") in {"TAKE_PROFIT_MARKET", "STOP_MARKET"}:
            try:
                client.futures_cancel_order(symbol=symbol, orderId=o["orderId"])
            except Exception:
                pass
    print("Done. Position close flow selesai.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Simulasi trade real pakai .env (Spot Demo / Futures Demo).")
    parser.add_argument("--mode", choices=["spot-demo", "futures-demo"], required=True)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--qty", default="0.001", help="base asset quantity")
    parser.add_argument(
        "--price-factor",
        default="0.20",
        help="faktor dari best bid untuk harga LIMIT BUY (default 0.20 = jauh di bawah harga pasar)",
    )
    parser.add_argument("--do-order", action="store_true", help="aktifkan untuk benar-benar place+cancel order")
    parser.add_argument("--tp-sl-flow", action="store_true", help="khusus futures-demo: open market + set TP/SL + tunggu konfirmasi close")
    parser.add_argument("--tp-multiplier", default="1.01", help="TP = mark_price * value (default 1.01)")
    parser.add_argument("--sl-multiplier", default="0.99", help="SL = mark_price * value (default 0.99)")
    parser.add_argument("--simulate-sl-plus", action="store_true", help="khusus tp-sl-flow: cancel SL lama lalu pasang SL baru")
    parser.add_argument("--sl-plus-multiplier", default="1.0001", help="SL+ = mark_price * value (default 1.0001)")
    args = parser.parse_args()

    try:
        if args.mode == "spot-demo":
            return run_spot_demo(args)
        return run_futures_demo(args)
    except BinanceAPIException as e:
        print("BinanceAPIException:", str(e))
        return 2
    except Exception as e:
        print("Error:", str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
