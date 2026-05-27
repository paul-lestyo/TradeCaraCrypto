# Tujuan
# Test dispatch dasar dan helper trade engine.
# Caller
# pytest.
# Dependensi
# CaraCrypto.trade_engine, CaraCrypto.models.
# Main Functions
# Validasi property task 11.6 (baseline).
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
        return {"ok": True}

    def futures_get_open_orders(self, **_):
        return [{"orderId": 10, "type": "STOP_MARKET"}, {"orderId": 11, "type": "TAKE_PROFIT_MARKET"}]

    def futures_cancel_order(self, **kwargs):
        self.canceled.append(kwargs)
        return {"status": "CANCELED"}


class _DB:
    async def store_modification_log(self, *_):
        return None

    async def get_daily_loss(self, *_):
        return Decimal("0")


class _Alert:
    async def notify_new_order(self, *_):
        return None

    async def notify_modification(self, *_):
        return None

    async def notify_risk_limit(self, *_):
        return None


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


def _engine():
    risk = SimpleNamespace(
        trade_margin_percent=1.0,
        high_risk_multiplier=0.5,
        max_concurrent_positions=5,
        max_position_size_percent=200.0,
        daily_loss_limit_percent=5.0,
    )
    return TradeEngine(_Client(), _DB(), _Alert(), _PM(), risk)


def test_fixed_leverage_property():
    e = _engine()
    assert e.get_leverage("BTCUSDT") == 125
    assert e.get_leverage("ETHUSDT") == 100
    assert e.get_leverage("XRPUSDT") == 50


def test_final_tp_level_property():
    e = _engine()
    assert e._get_final_tp_level(Direction.LONG, [Decimal("10"), Decimal("12")]) == Decimal("12")
    assert e._get_final_tp_level(Direction.SHORT, [Decimal("10"), Decimal("12")]) == Decimal("10")


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
