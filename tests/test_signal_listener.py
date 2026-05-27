# Tujuan
# Test filtering forum topic dan klasifikasi edit message listener.
# Caller
# pytest.
# Dependensi
# CaraCrypto.signal_listener.
# Main Functions
# Validasi property task 8.2.
# Side Effects
# Tidak ada.

import asyncio
from types import SimpleNamespace

import pytest

from CaraCrypto.signal_listener import SignalListener


class _DB:
    def __init__(self):
        self.text_updates = []
        self.row = SimpleNamespace(id=1, text="[OPEN] BTC")

    async def get_message_by_telegram_id(self, *_):
        return self.row

    async def update_message_text(self, row_id, text):
        self.text_updates.append((row_id, text))


class _Alert:
    async def notify_error(self, *_):
        return None


def _cfg():
    return SimpleNamespace(api_id=1, api_hash="h", phone="p", groups=[100], forum_topics={100: [7]})


def test_forum_topic_filter_property():
    listener = SignalListener(_cfg(), _DB(), _Alert(), asyncio.Queue())
    assert listener._should_process_message(100, 7) is True
    assert listener._should_process_message(100, 8) is False
    assert listener._should_process_message(101, 7) is False


@pytest.mark.asyncio
async def test_message_edit_classification_property():
    q = asyncio.Queue()
    db = _DB()
    listener = SignalListener(_cfg(), db, _Alert(), q)

    e1 = SimpleNamespace(chat_id=100, raw_text="[CLOSED] BTC", message=SimpleNamespace(id=9, reply_to_top_id=7))
    await listener._handle_message_edit(e1)
    assert db.text_updates[-1][1] == "[CLOSED] BTC"
    assert q.empty()

    e2 = SimpleNamespace(chat_id=100, raw_text="[CANCEL] BTC", message=SimpleNamespace(id=9, reply_to_top_id=7))
    await listener._handle_message_edit(e2)
    assert db.text_updates[-1][1] == "[CANCEL] BTC"
    assert not q.empty()
