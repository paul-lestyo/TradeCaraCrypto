# Tujuan
# Menjaga state posisi berjalan in-memory untuk watcher/protection order.
# Caller
# __main__, trade_engine, context_builder.
# Dependensi
# database.py, models.py.
# Main Functions
# CRUD state posisi dan context snapshot.
# Side Effects
# Menyimpan state posisi aktif selama proses berjalan.

from __future__ import annotations

from typing import Dict, List, Optional, Set

from .database import Database
from .models import PositionState, RunningPosition


class PositionManager:
    def __init__(self, db: Database):
        self.db = db
        self._running_positions: Dict[str, RunningPosition] = {}
        self._closed_today: Set[str] = set()

    async def initialize(self) -> None:
        self._running_positions = {}

    async def add_position(self, position: RunningPosition) -> None:
        self._running_positions[position.pair] = position

    async def remove_position(self, pair: str) -> None:
        self._running_positions.pop(pair, None)
        self._closed_today.add(pair)

    async def update_sl(self, pair: str, new_sl) -> None:
        if pair in self._running_positions:
            self._running_positions[pair].current_sl = new_sl

    async def update_tp(self, pair: str, tp_levels) -> None:
        if pair in self._running_positions:
            self._running_positions[pair].tp_levels = list(tp_levels)

    def get_running_positions(self) -> List[RunningPosition]:
        return list(self._running_positions.values())

    def get_closed_today(self) -> List[str]:
        return sorted(self._closed_today)

    def has_position(self, pair: str) -> bool:
        return pair in self._running_positions

    def get_position(self, pair: str) -> Optional[RunningPosition]:
        return self._running_positions.get(pair)

    def get_context_state(self) -> PositionState:
        return PositionState(
            closed_today=self.get_closed_today(),
        )
