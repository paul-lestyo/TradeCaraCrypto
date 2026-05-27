# Tujuan
# Menjaga state posisi berjalan dan allowed_running in-memory.
# Caller
# __main__, trade_engine, context_builder.
# Dependensi
# database.py, models.py.
# Main Functions
# CRUD state posisi dan context snapshot.
# Side Effects
# Sinkronisasi ke tabel running_positions.

from __future__ import annotations

from typing import Dict, List, Optional, Set

from .database import Database
from .models import PositionState, RunningPosition


class PositionManager:
    def __init__(self, db: Database):
        self.db = db
        self._running_positions: Dict[str, RunningPosition] = {}
        self._closed_today: Set[str] = set()
        self._allowed_running: Set[str] = set()

    async def initialize(self) -> None:
        positions = await self.db.get_running_positions()
        self._running_positions = {p.pair: p for p in positions}

    async def add_position(self, position: RunningPosition) -> None:
        self._running_positions[position.pair] = position
        await self.db.store_position(position)

    async def remove_position(self, pair: str) -> None:
        self._running_positions.pop(pair, None)
        self._closed_today.add(pair)
        await self.db.remove_position(pair)

    async def update_sl(self, pair: str, new_sl) -> None:
        if pair in self._running_positions:
            self._running_positions[pair].current_sl = new_sl

    async def update_tp(self, pair: str, tp_levels) -> None:
        if pair in self._running_positions:
            self._running_positions[pair].tp_levels = list(tp_levels)

    def add_to_allowed_running(self, pair: str) -> None:
        self._allowed_running.add(pair)

    def remove_from_allowed_running(self, pair: str) -> None:
        self._allowed_running.discard(pair)

    def get_running_positions(self) -> List[RunningPosition]:
        return list(self._running_positions.values())

    def get_running_pairs(self) -> List[str]:
        return sorted(self._running_positions.keys())

    def get_closed_today(self) -> List[str]:
        return sorted(self._closed_today)

    def get_allowed_running(self) -> List[str]:
        return sorted(self._allowed_running)

    def has_position(self, pair: str) -> bool:
        return pair in self._running_positions

    def get_position(self, pair: str) -> Optional[RunningPosition]:
        return self._running_positions.get(pair)

    def get_context_state(self) -> PositionState:
        return PositionState(
            running_positions=self.get_running_positions(),
            running_pairs=self.get_running_pairs(),
            closed_today=self.get_closed_today(),
            allowed_running=self.get_allowed_running(),
        )
