# Tujuan
# Membangun payload konteks message untuk single-call Gemini.
# Caller
# __main__ sebelum parser dipanggil.
# Dependensi
# database.py, position_manager.py.
# Main Functions
# `build_context`.
# Side Effects
# Query history dari database.

from __future__ import annotations

from .database import Database
from .models import MessageContext, RawSignalMessage
from .position_manager import PositionManager


class ContextBuilder:
    def __init__(self, db: Database, position_manager: PositionManager):
        self.db = db
        self.position_manager = position_manager

    async def build_context(self, message: RawSignalMessage) -> MessageContext:
        history = await self.db.get_recent_messages(message.group_id, message.topic_id, limit=10)
        state = self.position_manager.get_context_state()
        return MessageContext(current_message=message, history=history, position_state=state)
