# Tujuan
# Test kelengkapan payload context builder.
# Caller
# pytest.
# Dependensi
# CaraCrypto.context_builder, CaraCrypto.models.
# Main Functions
# Validasi property task 7.2.
# Side Effects
# Tidak ada.

from datetime import datetime

import pytest

from CaraCrypto.context_builder import ContextBuilder
from CaraCrypto.models import MessageContext, PositionState, RawSignalMessage


class _DB:
    async def get_recent_messages(self, group_id, topic_id, limit=10):
        return [{"id": i, "text": f"m{i}"} for i in range(12)][:limit]


class _PM:
    def get_context_state(self):
        return PositionState(closed_today=[])


@pytest.mark.asyncio
async def test_message_context_payload_completeness_property():
    cb = ContextBuilder(_DB(), _PM())
    msg = RawSignalMessage(text="hello", group_id=1, message_id=99, topic_id=7, timestamp=datetime.utcnow())
    ctx = await cb.build_context(msg)
    assert isinstance(ctx, MessageContext)
    assert ctx.current_message.message_id == 99
    assert len(ctx.history) <= 10
    assert ctx.position_state.closed_today == []
