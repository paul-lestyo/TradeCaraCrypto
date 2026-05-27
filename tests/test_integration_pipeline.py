# Tujuan
# Integration tests pipeline satu pesan untuk alur simpan->parse->execute.
# Caller
# pytest.
# Dependensi
# CaraCrypto.__main__._process_one_signal.
# Main Functions
# Validasi skenario 14.2.
# Side Effects
# Tidak ada.

import pytest

from CaraCrypto.__main__ import _process_one_signal
from CaraCrypto.models import Direction, GeminiAction, OrderType, RawSignalMessage, RiskLevel, TradeAction


class _Db:
    def __init__(self):
        self.stored = []
        self.reply_calls = []

    async def store_message(self, payload):
        self.stored.append(payload)
        return 99

    async def populate_reply_data(self, message_db_id, group_id, reply_to_message_id):
        self.reply_calls.append((message_db_id, group_id, reply_to_message_id))


class _ContextBuilder:
    async def build_context(self, raw):
        return {"raw": raw}


class _Parser:
    def __init__(self, action):
        self.action = action
        self.calls = []

    async def parse_and_classify(self, context, message_db_id):
        self.calls.append((context, message_db_id))
        return self.action


class _Engine:
    def __init__(self):
        self.calls = []

    async def execute_action(self, action, message_db_id):
        self.calls.append((action, message_db_id))


class _Watcher:
    def __init__(self):
        self.subscribed = []

    async def subscribe(self, pair):
        self.subscribed.append(pair)


@pytest.mark.asyncio
async def test_full_flow_message_to_execute_property():
    db = _Db()
    watcher = _Watcher()
    engine = _Engine()
    action = TradeAction(
        action=GeminiAction.NEW_SIGNAL,
        pair="BTCUSDT",
        direction=Direction.LONG,
        order_type=OrderType.MARKET,
        risk_level=RiskLevel.NORMAL,
    )
    parser = _Parser(action)
    raw = RawSignalMessage(text="[OPEN] BTC", group_id=-1, message_id=1)
    await _process_one_signal(raw, db, _ContextBuilder(), parser, engine, watcher)
    assert len(db.stored) == 1
    assert len(engine.calls) == 1
    assert watcher.subscribed == ["BTCUSDT"]


@pytest.mark.asyncio
async def test_skip_flow_no_execute_property():
    db = _Db()
    watcher = _Watcher()
    engine = _Engine()
    parser = _Parser(TradeAction(action=GeminiAction.SKIP))
    raw = RawSignalMessage(text="chat biasa", group_id=-1, message_id=2)
    await _process_one_signal(raw, db, _ContextBuilder(), parser, engine, watcher)
    assert len(db.stored) == 1
    assert len(engine.calls) == 0
    assert watcher.subscribed == []


@pytest.mark.asyncio
async def test_re_entry_subscribe_property():
    db = _Db()
    watcher = _Watcher()
    engine = _Engine()
    action = TradeAction(
        action=GeminiAction.RE_ENTRY,
        pair="ETHUSDT",
        direction=Direction.SHORT,
        order_type=OrderType.LIMIT,
        risk_level=RiskLevel.NORMAL,
    )
    parser = _Parser(action)
    raw = RawSignalMessage(text="re-entry", group_id=-1, message_id=3)
    await _process_one_signal(raw, db, _ContextBuilder(), parser, engine, watcher)
    assert watcher.subscribed == ["ETHUSDT"]
