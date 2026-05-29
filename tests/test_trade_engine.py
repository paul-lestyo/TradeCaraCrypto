# Tujuan
# Test dispatch dasar dan helper trade engine.
# Caller
# pytest.
# Dependensi
# CaraCrypto.trade_engine, CaraCrypto.models.
# Main Functions
# Validasi property task 11.6, order executable, dan adapter Binance.
# Side Effects
# Tidak ada.

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from CaraCrypto.models import Direction, GeminiAction, OrderType, RiskLevel, RunningPosition, TradeAction
from CaraCrypto.trade_engine import TradeEngine


class _Client:
    def __init__(self):
        self.orders = []
        self.canceled = []

    def change_margin_type(self, **_):
        return {}

    def change_leverage(self, **kwargs):
        return {"leverage": kwargs.get("leverage", 50)}

    def new_order(self, **kwargs):
        self.orders.append(kwargs)
        return {"orderId": 1000 + len(self.orders)}

    def futures_get_open_orders(self, **_):
        return [{"orderId": 10, "type": "STOP_MARKET"}, {"orderId": 11, "type": "TAKE_PROFIT_MARKET"}]

    def futures_cancel_order(self, **kwargs):
        self.canceled.append(kwargs)
        return {"status": "CANCELED"}

    def balance(self, **_):
        return [{"asset": "USDT", "availableBalance": "1000"}]

    def futures_exchange_info(self, **_):
        return {"symbols": []}


class _PythonBinanceClient:
    def __init__(self):
        self.orders = []
        self.canceled = []
        self.margin_calls = []
        self.leverage_calls = []

    def futures_change_margin_type(self, **kwargs):
        self.margin_calls.append(kwargs)
        return {}

    def futures_change_leverage(self, **kwargs):
        self.leverage_calls.append(kwargs)
        return {"leverage": kwargs.get("leverage", 50)}

    def futures_create_order(self, **kwargs):
        self.orders.append(kwargs)
        return {"orderId": 2000 + len(self.orders)}

    def futures_get_open_orders(self, **_):
        return []

    def futures_cancel_order(self, **kwargs):
        self.canceled.append(kwargs)
        return {"status": "CANCELED"}

    def futures_account_balance(self, **_):
        return [{"asset": "USDT", "availableBalance": "1000"}]


class _DB:
    async def store_modification_log(self, *_):
        return None

    async def get_daily_loss(self, *_):
        return Decimal("0")


class _Alert:
    def __init__(self):
        self.errors = []

    async def notify_new_order(self, *_):
        return None

    async def notify_modification(self, *_):
        return None

    async def notify_risk_limit(self, *_):
        return None

    async def notify_error(self, *args):
        self.errors.append(args)


class _PM:
    def __init__(self):
        self.m = {}

    async def add_position(self, p):
        self.m[p.pair] = p

    async def remove_position(self, pair):
        self.m.pop(pair, None)

    async def update_sl(self, pair, sl):
        if pair in self.m:
            self.m[pair].current_sl = sl

    def has_position(self, pair):
        return pair in self.m

    def get_position(self, pair):
        return self.m.get(pair)

    def get_running_pairs(self):
        return list(self.m.keys())


def _engine(client=None):
    risk = SimpleNamespace(
        trade_margin_percent=1.0,
        high_risk_multiplier=0.5,
        max_concurrent_positions=5,
        daily_loss_limit_percent=5.0,
    )
    return TradeEngine(client or _Client(), _DB(), _Alert(), _PM(), risk)


def test_fixed_leverage_property():
    e = _engine()
    assert e.get_leverage("BTCUSDT") == 125
    assert e.get_leverage("ETHUSDT") == 100
    assert e.get_leverage("XRPUSDT") == 50


def test_final_tp_level_property():
    e = _engine()
    assert e._get_final_tp_level(Direction.LONG, [Decimal("10"), Decimal("12")]) == Decimal("12")
    assert e._get_final_tp_level(Direction.SHORT, [Decimal("10"), Decimal("12")]) == Decimal("10")


def test_python_binance_order_adapter_property():
    client = _PythonBinanceClient()
    e = _engine(client)
    response = e._create_futures_order(symbol="BTCUSDT", side="BUY", type="MARKET", quantity="0.1")
    assert response["orderId"] == 2001
    assert client.orders[0]["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_python_binance_margin_and_leverage_adapter_property():
    client = _PythonBinanceClient()
    e = _engine(client)
    await e._set_margin_mode_cross("BTCUSDT")
    leverage = await e._set_leverage("BTCUSDT", 125)
    assert client.margin_calls[0]["symbol"] == "BTCUSDT"
    assert client.leverage_calls[0]["leverage"] == 125
    assert leverage == 125


@pytest.mark.asyncio
async def test_reverse_action_property():
    e = _engine()
    await e.position_manager.add_position(
        RunningPosition("BTCUSDT", Direction.LONG, Decimal("100"), Decimal("90"), [Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow())
    )
    await e.execute_action(TradeAction(action=GeminiAction.REVERSE, pair="BTCUSDT", order_type=OrderType.MARKET, risk_level=RiskLevel.NORMAL))
    pos = e.position_manager.get_position("BTCUSDT")
    assert pos is not None
    assert pos.direction == Direction.SHORT


@pytest.mark.asyncio
async def test_sl_breakeven_replaces_old_sl_order():
    e = _engine()
    await e.position_manager.add_position(
        RunningPosition("BTCUSDT", Direction.LONG, Decimal("100"), Decimal("90"), [Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow())
    )
    await e.execute_action(TradeAction(action=GeminiAction.SET_SL_BREAKEVEN, pair="BTCUSDT", risk_level=RiskLevel.NORMAL))
    # old STOP_MARKET cancelled
    assert any(c.get("orderId") == 10 for c in e.client.canceled)
    # new STOP_MARKET created at entry price
    stop_orders = [o for o in e.client.orders if o.get("type") == "STOP_MARKET"]
    assert stop_orders
    assert stop_orders[-1].get("stopPrice") == "100"


@pytest.mark.asyncio
async def test_limit_order_without_entry_rejected_property():
    e = _engine()
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="PROMPTUSDT",
            direction=Direction.LONG,
            order_type=OrderType.LIMIT,
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is False
    assert e.client.orders == []
    assert not e.position_manager.has_position("PROMPTUSDT")


@pytest.mark.asyncio
async def test_limit_order_normalizes_pair_and_stores_real_order_id_property():
    e = _engine()
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="JELLY/USDT",
            direction=Direction.LONG,
            order_type=OrderType.LIMIT,
            entry_price=Decimal("0.01"),
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["symbol"] == "JELLYUSDT"
    assert e.position_manager.get_position("JELLYUSDT").order_id == "1001"


@pytest.mark.asyncio
async def test_limit_order_normalizes_price_and_quantity_by_symbol_filters_property():
    class _ClientWithFilters(_Client):
        def futures_exchange_info(self, **_):
            return {
                "symbols": [
                    {
                        "symbol": "BRETTUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                        ],
                    }
                ]
            }

    e = _engine(_ClientWithFilters())
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="BRETTUSDT",
            direction=Direction.LONG,
            order_type=OrderType.LIMIT,
            entry_price=Decimal("0.123456"),
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["quantity"] == "40500"
    assert e.client.orders[0]["price"] == "0.1234"


@pytest.mark.asyncio
async def test_limit_order_uses_entry_zone_average_then_normalizes_ticksize_property():
    class _ClientWithFilters(_Client):
        def futures_exchange_info(self, **_):
            return {
                "symbols": [
                    {
                        "symbol": "BRETTUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                        ],
                    }
                ]
            }

    e = _engine(_ClientWithFilters())
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="BRETTUSDT",
            direction=Direction.SHORT,
            order_type=OrderType.LIMIT,
            entry_zone=[Decimal("0.006601"), Decimal("0.006643")],
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["price"] == "0.0066"
    assert e.position_manager.get_position("BRETTUSDT").entry_price == Decimal("0.0066")
