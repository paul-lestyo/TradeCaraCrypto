# Tujuan
# Skenario end-to-end image signal ke Binance Futures testnet.
# Caller
# Operator lokal via terminal.
# Dependensi
# python-dotenv, google-generativeai, Pillow, python-binance, modul internal CaraCrypto.
# Main Functions
# Extract image signal, bangun TradeAction, adjust passive limit, execute lewat TradeEngine.
# Side Effects
# Dapat membuat market/limit/protection order di Binance Futures testnet saat --execute dipakai.

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import re
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from types import SimpleNamespace
from typing import Any, Dict, Optional

from binance.client import Client
from binance.exceptions import BinanceAPIException
from dotenv import load_dotenv
from PIL import Image
import google.generativeai as genai


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_IMAGE = PROJECT_ROOT / "CaraCrypto" / "image_caracrypto" / "photo_2026-04-13_09-00-09.jpg"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from CaraCrypto.models import Direction, GeminiAction, OrderType, RiskLevel, RunningPosition, TradeAction
from CaraCrypto.signal_parser import SignalParser
from CaraCrypto.trade_engine import TradeEngine


class ConsoleAlert:
    async def send_alert(self, message: str) -> None:
        print("[ALERT]", message)

    async def notify_error(self, source: str, error: str) -> None:
        print("[ERROR]", source, error)

    async def notify_new_order(self, pair: str, direction: str, price: str, order_type: str) -> None:
        print("[ORDER]", {"pair": pair, "side": direction, "type": order_type, "price": price})

    async def notify_new_order_detail(
        self,
        pair: str,
        direction: str,
        order_type: str,
        entry_price: str,
        quantity: str,
        leverage: int,
        margin_used_usd: str,
        stop_loss: Optional[str],
        take_profit: Optional[str],
        reason: str,
    ) -> None:
        print(
            "[ORDER DETAIL]",
            {
                "pair": pair,
                "side": direction,
                "type": order_type,
                "entry": entry_price,
                "qty": quantity,
                "leverage": leverage,
                "margin_used_usd": margin_used_usd,
                "sl": stop_loss,
                "tp": take_profit,
                "reason": reason,
            },
        )

    async def notify_order_filled(self, pair: str, order_type: str, price: Optional[str] = None) -> None:
        print("[FILLED]", {"pair": pair, "type": order_type, "price": price})

    async def notify_tp_sl_set(self, pair: str, tp: Optional[str], sl: Optional[str], source: str) -> None:
        print("[PROTECTION]", {"pair": pair, "tp": tp, "sl": sl, "source": source})

    async def notify_modification(self, pair: str, action_type: str, details: str) -> None:
        print("[MODIFICATION]", {"pair": pair, "action": action_type, "details": details})

    async def notify_risk_limit(self, message: str) -> None:
        print("[RISK LIMIT]", message)


class MemoryDB:
    async def get_daily_loss(self, _day_start):
        return Decimal("0")

    async def store_modification_log(self, *_args, **_kwargs):
        return None


class MemoryPositionManager:
    def __init__(self) -> None:
        self.positions: Dict[str, RunningPosition] = {}

    async def add_position(self, position: RunningPosition) -> None:
        self.positions[position.pair] = position

    async def remove_position(self, pair: str) -> None:
        self.positions.pop(pair, None)

    async def update_sl(self, pair: str, new_sl) -> None:
        if pair in self.positions:
            self.positions[pair].current_sl = new_sl

    def has_position(self, pair: str) -> bool:
        return pair in self.positions

    def get_position(self, pair: str):
        return self.positions.get(pair)

    def get_running_pairs(self):
        return sorted(self.positions.keys())


class TestnetFuturesClient:
    def __init__(self, client: Client) -> None:
        self.client = client
        self._exchange_info: Optional[dict] = None

    def __getattr__(self, name: str):
        if name == "new_order":
            raise AttributeError(name)
        return getattr(self.client, name)

    def _symbol_info(self, symbol: str) -> dict:
        if self._exchange_info is None:
            self._exchange_info = self.client.futures_exchange_info()
        for item in self._exchange_info.get("symbols", []):
            if item.get("symbol") == symbol:
                return item
        raise RuntimeError(f"Symbol tidak ditemukan di Futures testnet: {symbol}")

    def _filter(self, symbol: str, filter_type: str) -> dict:
        for item in self._symbol_info(symbol).get("filters", []):
            if item.get("filterType") == filter_type:
                return item
        return {}

    def _round_down(self, value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

    def _quantize_up(self, value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        units = (value / step).to_integral_value(rounding=ROUND_DOWN)
        if units * step < value:
            units += 1
        return units * step

    def _mark_price(self, symbol: str) -> Decimal:
        return Decimal(str(self.client.futures_mark_price(symbol=symbol)["markPrice"]))

    def _normalize_order_payload(self, kwargs: dict) -> dict:
        payload = dict(kwargs)
        symbol = str(payload["symbol"]).upper()
        price_filter = self._filter(symbol, "PRICE_FILTER")
        lot_filter = self._filter(symbol, "LOT_SIZE")
        notional_filter = self._filter(symbol, "MIN_NOTIONAL")
        tick = Decimal(str(price_filter.get("tickSize", "0.0001")))
        step = Decimal(str(lot_filter.get("stepSize", "0.001")))

        if payload.get("price") is not None:
            payload["price"] = str(self._round_down(Decimal(str(payload["price"])), tick))
        if payload.get("stopPrice") is not None:
            payload["stopPrice"] = str(self._round_down(Decimal(str(payload["stopPrice"])), tick))
        if payload.get("quantity") is not None:
            qty = self._round_down(Decimal(str(payload["quantity"])), step)
            reference_price = Decimal(str(payload.get("price") or self._mark_price(symbol)))
            min_notional = Decimal(str(notional_filter.get("notional") or notional_filter.get("minNotional") or "0"))
            if min_notional > 0 and qty * reference_price < min_notional:
                qty = self._quantize_up(min_notional / reference_price, step)
            payload["quantity"] = str(qty)
        return payload

    def futures_create_order(self, **kwargs):
        payload = self._normalize_order_payload(kwargs)
        print("[BINANCE CREATE]", payload)
        return self.client.futures_create_order(**payload)


def _require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"Missing env: {name}")
    return value


def _extract_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    text = re.sub(r"^```json\s*|\s*```$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()
    try:
        return json.loads(text)
    except Exception:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            raise RuntimeError(f"Gemini response bukan JSON: {text[:300]}")
        return json.loads(match.group(0))


def fallback_payload() -> Dict[str, Any]:
    return {
        "action": "new_signal",
        "pair": "PROMUSDT",
        "direction": "long",
        "order_type": "limit",
        "entry_price": "1.359",
        "take_profit_levels": ["1.401"],
        "stop_loss": "1.307",
        "risk_level": "normal",
        "close_percentage": None,
        "notes": "Fallback manual dari photo_2026-04-13_09-00-09.jpg.",
    }


def extract_image_payload(image_path: pathlib.Path, allow_fallback: bool) -> Dict[str, Any]:
    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    model_name = (os.getenv("GEMINI_MODEL") or "gemini-2.0-flash").strip()
    if not api_key:
        if allow_fallback:
            print("[WARN] GEMINI_API_KEY kosong, memakai fallback payload dari image target.")
            return fallback_payload()
        raise RuntimeError("GEMINI_API_KEY kosong dan --no-fallback dipakai.")

    prompt = (
        "Kamu parser signal trading dari screenshot chart Caracrypto.\n"
        "Return JSON saja dengan schema berikut:\n"
        "{"
        '"action":"new_signal",'
        '"pair":"SYMBOLUSDT",'
        '"direction":"long|short",'
        '"order_type":"market|limit",'
        '"entry_price":number|null,'
        '"take_profit_levels":[number],'
        '"stop_loss":number|null,'
        '"risk_level":"normal|high",'
        '"close_percentage":null,'
        '"notes":"bukti visual singkat"'
        "}\n"
        "Normalisasi pair: PROM/USDT atau PROMUSDT.P menjadi PROMUSDT. "
        "Jika ada level horizontal entry yang jelas, gunakan order_type=limit dan entry_price dari level itu. "
        "Jika hanya arah tanpa entry jelas, gunakan order_type=market dan entry_price=null. "
        "Jangan tulis markdown."
    )
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name)
    try:
        with Image.open(image_path) as image:
            response = model.generate_content([prompt, image])
        return _extract_json(response.text)
    except Exception:
        if allow_fallback:
            print("[WARN] Gemini gagal, memakai fallback payload dari image target.")
            return fallback_payload()
        raise


def build_action(payload: Dict[str, Any], order_mode: str) -> TradeAction:
    parser = SignalParser(SimpleNamespace(api_key="", model="local-validator"), MemoryDB())
    action = parser._validate_and_build_action(payload)
    if not action or action.action != GeminiAction.NEW_SIGNAL:
        raise RuntimeError(f"Payload tidak menghasilkan new_signal valid: {payload}")
    if order_mode == "market":
        action.order_type = OrderType.MARKET
        action.entry_price = None
    elif order_mode == "limit":
        action.order_type = OrderType.LIMIT
        if action.entry_price is None:
            raise RuntimeError("order_mode=limit but payload entry_price kosong.")
    return action


def build_testnet_client() -> TestnetFuturesClient:
    key = _require_env("BINANCE_API_KEY")
    secret = _require_env("BINANCE_API_SECRET")
    client = Client(key, secret)
    client.FUTURES_URL = "https://demo-fapi.binance.com/fapi"
    client.FUTURES_DATA_URL = "https://demo-fapi.binance.com/futures/data"
    server_time = client.futures_time()["serverTime"]
    client.timestamp_offset = int(server_time - int(time.time() * 1000))
    return TestnetFuturesClient(client)


def build_engine(client: TestnetFuturesClient, margin_percent: Decimal) -> tuple[TradeEngine, MemoryPositionManager]:
    alert = ConsoleAlert()
    db = MemoryDB()
    pm = MemoryPositionManager()
    risk = SimpleNamespace(
        trade_margin_percent=float(margin_percent),
        high_risk_multiplier=0.5,
        max_concurrent_positions=5,
        max_position_size_percent=200.0,
        daily_loss_limit_percent=5.0,
    )
    return TradeEngine(client, db, alert, pm, risk), pm


def apply_passive_limit_price(client: TestnetFuturesClient, action: TradeAction, factor: Decimal) -> None:
    if action.order_type != OrderType.LIMIT or not action.pair or not action.direction:
        return
    mark = client._mark_price(action.pair)
    if action.direction == Direction.LONG:
        action.entry_price = mark * factor
    else:
        action.entry_price = mark * (Decimal("2") - factor)
    print(
        "[PASSIVE LIMIT]",
        {
            "pair": action.pair,
            "mark_price": str(mark),
            "adjusted_entry": str(action.entry_price),
            "direction": action.direction.value,
        },
    )


async def run(args: argparse.Namespace) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    image_path = pathlib.Path(args.image)
    if not image_path.is_absolute():
        image_path = PROJECT_ROOT / image_path
    if not image_path.exists():
        raise RuntimeError(f"Image tidak ditemukan: {image_path}")

    payload = extract_image_payload(image_path, allow_fallback=not args.no_fallback)
    action = build_action(payload, args.order_mode)

    print("=== IMAGE EXTRACTION ===")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    print("=== TRADE ACTION ===")
    print(
        {
            "pair": action.pair,
            "direction": action.direction.value if action.direction else None,
            "order_type": action.order_type.value if action.order_type else None,
            "entry_price": str(action.entry_price) if action.entry_price is not None else None,
            "tp": [str(x) for x in (action.take_profit_levels or [])],
            "sl": str(action.stop_loss) if action.stop_loss is not None else None,
            "risk": action.risk_level.value if action.risk_level else None,
        }
    )

    if not args.execute:
        print("\nDry-run selesai. Tambahkan --execute untuk benar-benar kirim order ke Futures testnet.")
        return 0

    client = build_testnet_client()
    account = client.futures_account()
    print("=== TESTNET ACCOUNT ===")
    print({"canTrade": account.get("canTrade"), "wallet": account.get("totalWalletBalance")})
    if args.limit_price_source == "passive-testnet":
        apply_passive_limit_price(client, action, Decimal(str(args.passive_limit_factor)))

    engine, pm = build_engine(client, Decimal(str(args.margin_percent)))
    accepted = await engine.execute_action(action)
    if not accepted:
        print("Order ditolak engine. Cek alert/error di output atas.")
        return 2

    position = pm.get_position(action.pair or "")
    print("=== ENGINE POSITION SNAPSHOT ===")
    if position:
        print(
            {
                "pair": position.pair,
                "direction": position.direction.value,
                "entry": str(position.entry_price),
                "qty": str(position.quantity),
                "order_id": position.order_id,
                "opened_at": position.opened_at.isoformat(),
            }
        )

    if action.pair:
        orders = client.futures_get_open_orders(symbol=action.pair)
        print("=== OPEN ORDERS ON TESTNET ===")
        for order in orders:
            print(
                {
                    "orderId": order.get("orderId"),
                    "symbol": order.get("symbol"),
                    "side": order.get("side"),
                    "type": order.get("type"),
                    "price": order.get("price"),
                    "stopPrice": order.get("stopPrice"),
                    "status": order.get("status"),
                }
            )
    print("Done. Silakan cek Binance Futures testnet UI.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="E2E image signal -> TradeEngine -> Binance Futures testnet.")
    parser.add_argument("--image", default=str(DEFAULT_IMAGE), help="path image signal")
    parser.add_argument("--order-mode", choices=["image", "market", "limit"], default="image")
    parser.add_argument("--limit-price-source", choices=["image", "passive-testnet"], default="image")
    parser.add_argument("--passive-limit-factor", default="0.98", help="LONG limit = mark * factor, SHORT = mark * (2-factor)")
    parser.add_argument("--margin-percent", default="0.1", help="margin simulasi engine dari saldo placeholder 1000 USDT")
    parser.add_argument("--execute", action="store_true", help="benar-benar kirim order ke Binance Futures testnet")
    parser.add_argument("--no-fallback", action="store_true", help="matikan fallback manual jika Gemini gagal")
    args = parser.parse_args()
    try:
        return asyncio.run(run(args))
    except BinanceAPIException as exc:
        print("BinanceAPIException:", exc)
        return 2
    except Exception as exc:
        print("Error:", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
