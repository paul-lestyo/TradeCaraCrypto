# Tujuan
# Test deteksi TP/SL dan update order stream watcher.
# Caller
# pytest.
# Dependensi
# CaraCrypto.price_watcher, CaraCrypto.models.
# Main Functions
# Validasi property task 12.3.
# Side Effects
# Tidak ada.

from datetime import datetime
from decimal import Decimal

import pytest

from CaraCrypto.models import Direction, RunningPosition, TradeAction, GeminiAction
from CaraCrypto.price_watcher import PriceWatcher


class _Alert:
    def __init__(self):
        self.sent = []

    async def notify_tp_hit(self, pair, tp):
        self.sent.append(f"tp:{pair}:{tp}")

    async def notify_sl_hit(self, pair, sl):
        self.sent.append(f"sl:{pair}:{sl}")

    async def send_alert(self, msg):
        self.sent.append(msg)

    async def notify_closed(self, pair, reason):
        self.sent.append(f"closed:{pair}:{reason}")


class _PM:
    def __init__(self, pos):
        self.pos = pos
        self.removed = []

    def get_position(self, pair):
        return self.pos if self.pos and self.pos.pair == pair else None

    async def remove_position(self, pair):
        self.removed.append(pair)
        if self.pos and self.pos.pair == pair:
            self.pos = None


class _TE:
    def __init__(self):
        self.called = 0

    async def _set_tp_sl_orders(self, _):
        self.called += 1

    def _get_open_orders(self, _pair):
        return []

    def _get_binance_running_pairs(self):
        return set()


@pytest.mark.asyncio
async def test_limit_fill_triggers_tp_sl_property():
    pos = RunningPosition("BTCUSDT", Direction.LONG, Decimal("100"), Decimal("95"), [Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow())
    alert = _Alert()
    pm = _PM(pos)
    w = PriceWatcher(alert, pm)
    te = _TE()
    w.trade_engine = te
    w.register_pending_order("oid1", TradeAction(action=GeminiAction.NEW_SIGNAL, pair="BTCUSDT"))
    await w.handle_order_update("oid1", "LIMIT", "FILLED", "BTCUSDT")
    assert te.called == 1


@pytest.mark.asyncio
async def test_position_closure_detection_property():
    pos = RunningPosition("BTCUSDT", Direction.LONG, Decimal("100"), Decimal("95"), [Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow())
    alert = _Alert()
    pm = _PM(pos)
    w = PriceWatcher(alert, pm)
    await w.handle_order_update("oid2", "TAKE_PROFIT_MARKET", "FILLED", "BTCUSDT")
    assert "BTCUSDT" in pm.removed


def test_tp_sl_direction_detection_property():
    w = PriceWatcher(_Alert(), _PM(None))
    assert w._check_tp_level_reached(Direction.LONG, Decimal("101"), Decimal("100"))
    assert w._check_tp_level_reached(Direction.SHORT, Decimal("99"), Decimal("100"))
    assert w._check_sl_reached(Direction.LONG, Decimal("99"), Decimal("100"))
    assert w._check_sl_reached(Direction.SHORT, Decimal("101"), Decimal("100"))


@pytest.mark.asyncio
async def test_pending_limit_missing_on_exchange_assumed_canceled_property():
    pos = RunningPosition("BTCUSDT", Direction.LONG, Decimal("100"), Decimal("95"), [Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow())
    alert = _Alert()
    pm = _PM(pos)
    w = PriceWatcher(alert, pm)
    te = _TE()
    w.trade_engine = te
    await w.subscribe("BTCUSDT")
    w.register_pending_order("oid-cancel", TradeAction(action=GeminiAction.NEW_SIGNAL, pair="BTCUSDT"))
    await w._reconcile_pending_limit_orders()
    assert "BTCUSDT" in pm.removed
    assert "closed:BTCUSDT:cancel" in alert.sent
