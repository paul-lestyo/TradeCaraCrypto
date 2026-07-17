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


@pytest.mark.asyncio
async def test_reduce_only_filled_treated_as_manual_close_property():
    pos = RunningPosition("BTCUSDT", Direction.LONG, Decimal("100"), Decimal("95"), [Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow())
    alert = _Alert()
    pm = _PM(pos)
    w = PriceWatcher(alert, pm)
    await w.subscribe("BTCUSDT")
    await w.handle_order_update(
        "oid-close",
        "MARKET",
        "FILLED",
        "BTCUSDT",
        side="SELL",
        reduce_only=True,
        close_position=False,
    )
    assert "BTCUSDT" in pm.removed
    assert "closed:BTCUSDT:manual_close" in alert.sent


@pytest.mark.asyncio
async def test_sl_hit_alert_only_sent_once_per_level_property():
    pos = RunningPosition("BTCUSDT", Direction.LONG, Decimal("70000"), Decimal("69374.1"), [Decimal("71000")], 50, "1", Decimal("0.1"), datetime.utcnow())
    alert = _Alert()
    pm = _PM(pos)
    w = PriceWatcher(alert, pm)

    class _TEWithPrice:
        async def _get_market_reference_price(self, _pair):
            return Decimal("69300")

    w.trade_engine = _TEWithPrice()
    await w.subscribe("BTCUSDT")
    await w._watch_price()
    await w._watch_price()

    assert alert.sent.count("sl:BTCUSDT:69374.1") == 1


@pytest.mark.asyncio
async def test_tp2_hit_triggers_sl_plus_property():
    pos = RunningPosition("BTCUSDT", Direction.LONG, Decimal("100"), Decimal("95"), [Decimal("110"), Decimal("120")], 50, "1", Decimal("0.1"), datetime.utcnow())
    alert = _Alert()
    pm = _PM(pos)
    w = PriceWatcher(alert, pm)

    class _TEWithBuffer:
        def __init__(self):
            self.applied_pair = None
            self.source = None

        async def _get_market_reference_price(self, _pair):
            return Decimal("112") if not self.applied_pair else Decimal("122")

        async def _handle_set_sl_plus_buffer(self, pair, source):
            self.applied_pair = pair
            self.source = source
            return True

    te = _TEWithBuffer()
    w.trade_engine = te
    await w.subscribe("BTCUSDT")

    # 1. Price is at 112 (TP1 hit)
    await w._watch_price()
    assert pos.tp1_notified is True
    assert te.applied_pair is None  # Not applied on TP1

    # 2. Update price to 122 (TP2 hit)
    async def get_price_122(_pair):
        return Decimal("122")
    te._get_market_reference_price = get_price_122

    await w._watch_price()
    assert getattr(pos, "tp2_notified", False) is True
    assert te.applied_pair is None


@pytest.mark.asyncio
async def test_trades_json_logging_property():
    pos = RunningPosition("BTCUSDT", Direction.LONG, Decimal("100"), Decimal("95"), [Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow())
    alert = _Alert()
    pm = _PM(pos)
    w = PriceWatcher(alert, pm)
    te = _TE()
    w.trade_engine = te
    
    # Track calls to _write_trades_json
    calls = []
    def dummy_write(event, **kwargs):
        calls.append((event, kwargs))
    w._write_trades_json = dummy_write
    
    # 1. Test limit order filled
    w.register_pending_order("oid1", TradeAction(action=GeminiAction.NEW_SIGNAL, pair="BTCUSDT"))
    await w.handle_order_update("oid1", "LIMIT", "FILLED", "BTCUSDT")
    
    assert len(calls) == 1
    assert calls[0][0] == "limit_order_filled"
    assert calls[0][1]["pair"] == "BTCUSDT"
    assert calls[0][1]["order_id"] == "oid1"
    
    # 2. Test position closed
    calls.clear()
    await w.handle_order_update("oid2", "TAKE_PROFIT_MARKET", "FILLED", "BTCUSDT")
    assert len(calls) == 1
    assert calls[0][0] == "position_closed"
    assert calls[0][1]["pair"] == "BTCUSDT"
    assert calls[0][1]["reason"] == "TP"


def test_write_trades_json_file_writing_property():
    from pathlib import Path
    import json
    
    log_path = Path("logs/trades.json")
    # Back up existing file if any
    backup_path = Path("logs/trades.json.bak")
    if log_path.exists():
        if backup_path.exists():
            backup_path.unlink()
        log_path.rename(backup_path)
        
    try:
        w = PriceWatcher(_Alert(), _PM(None))
        w._write_trades_json("test_event", foo=Decimal("12.34"), bar="hello")
        
        assert log_path.exists()
        with log_path.open("r", encoding="utf-8") as f:
            lines = f.readlines()
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["event"] == "test_event"
        assert data["foo"] == "12.34"
        assert data["bar"] == "hello"
        assert "timestamp" in data
    finally:
        if log_path.exists():
            log_path.unlink()
        if backup_path.exists():
            backup_path.rename(log_path)


@pytest.mark.asyncio
async def test_pending_limit_order_expired_after_24h_property():
    from datetime import timedelta, timezone
    pos = RunningPosition("BTCUSDT", Direction.LONG, Decimal("100"), Decimal("95"), [Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow())
    alert = _Alert()
    pm = _PM(pos)
    w = PriceWatcher(alert, pm)
    
    class _TEMock:
        def __init__(self):
            self.cancelled = []
        def _get_open_orders(self, pair):
            order_time = (datetime.now(timezone.utc) - timedelta(hours=25)).timestamp() * 1000.0
            return [{"orderId": "oid-expired", "type": "LIMIT", "time": order_time}]
        def _cancel_order(self, pair, order_id):
            self.cancelled.append((pair, order_id))
            
    te = _TEMock()
    w.trade_engine = te
    
    await w.subscribe("BTCUSDT")
    w.register_pending_order("oid-expired", TradeAction(action=GeminiAction.NEW_SIGNAL, pair="BTCUSDT"))
    
    assert "oid-expired" in w._pending_limit_orders
    assert "oid-expired" in w._pending_limit_order_times
    
    await w._reconcile_pending_limit_orders()
    
    assert ("BTCUSDT", "oid-expired") in te.cancelled
    assert "oid-expired" not in w._pending_limit_orders
    assert "oid-expired" not in w._pending_limit_order_times
    assert "BTCUSDT" in pm.removed

