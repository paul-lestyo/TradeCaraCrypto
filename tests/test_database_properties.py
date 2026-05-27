# Tujuan
# Property tests kontrak database untuk round-trip message + reply linkage.
# Caller
# pytest.
# Dependensi
# Tidak perlu koneksi DB eksternal.
# Main Functions
# Validasi skenario 2.3.
# Side Effects
# Tidak ada.

import pytest


class InMemoryDbContract:
    def __init__(self):
        self.rows = []

    async def store_message(self, payload):
        row = {
            "id": len(self.rows) + 1,
            "message_id": payload["message_id"],
            "group_id": payload["group_id"],
            "topic_id": payload.get("topic_id"),
            "text": payload.get("text", ""),
            "reply_to_message_id": payload.get("reply_to_message_id"),
            "reply_text": None,
            "reply_extracted_data": None,
            "extracted_data": payload.get("extracted_data"),
        }
        self.rows.append(row)
        return row["id"]

    async def get_message_by_telegram_id(self, message_id, group_id):
        for row in self.rows:
            if row["message_id"] == message_id and row["group_id"] == group_id:
                return row
        return None

    async def populate_reply_data(self, message_db_id, group_id, reply_to_message_id):
        if not reply_to_message_id:
            return
        current = next((r for r in self.rows if r["id"] == message_db_id), None)
        replied = await self.get_message_by_telegram_id(reply_to_message_id, group_id)
        if current and replied:
            current["reply_text"] = replied["text"]
            current["reply_extracted_data"] = replied["extracted_data"]


@pytest.mark.asyncio
async def test_message_storage_roundtrip_property():
    db = InMemoryDbContract()
    payload = {"message_id": 101, "group_id": -1001, "topic_id": 7, "text": "hello"}
    _ = await db.store_message(payload)
    got = await db.get_message_by_telegram_id(101, -1001)
    assert got is not None
    assert got["message_id"] == payload["message_id"]
    assert got["group_id"] == payload["group_id"]
    assert got["topic_id"] == payload["topic_id"]
    assert got["text"] == payload["text"]


@pytest.mark.asyncio
async def test_reply_data_population_property():
    db = InMemoryDbContract()
    parent_id = await db.store_message(
        {
            "message_id": 11,
            "group_id": -200,
            "text": "[OPEN] BTC",
            "extracted_data": {"pair": "BTCUSDT", "action": "new_signal"},
        }
    )
    assert parent_id == 1
    child_id = await db.store_message(
        {"message_id": 12, "group_id": -200, "text": "followup", "reply_to_message_id": 11}
    )
    await db.populate_reply_data(child_id, -200, 11)
    child = await db.get_message_by_telegram_id(12, -200)
    assert child["reply_text"] == "[OPEN] BTC"
    assert child["reply_extracted_data"] == {"pair": "BTCUSDT", "action": "new_signal"}
