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
        self._pending_positions: Dict[str, RunningPosition] = {}
        self._closed_today: Set[str] = set()
        self._allowed_running: Set[str] = set()

    async def initialize(self) -> None:
        self._running_positions = {}
        self._pending_positions = {}
        self._allowed_running = set()

    async def add_pending_position(self, position: RunningPosition) -> None:
        self._pending_positions[position.pair] = position

    async def add_position(self, position: RunningPosition) -> None:
        self._running_positions[position.pair] = position

    async def remove_position(self, pair: str) -> None:
        removed_running = self._running_positions.pop(pair, None)
        removed_pending = self._pending_positions.pop(pair, None)
        if removed_running is None and removed_pending is None:
            return
        self._closed_today.add(pair)

    async def remove_pending_position(self, pair: str) -> None:
        self._pending_positions.pop(pair, None)

    def get_pending_position(self, pair: str) -> Optional[RunningPosition]:
        return self._pending_positions.get(pair)

    def has_pending_position(self, pair: str) -> bool:
        return pair in self._pending_positions

    async def promote_pending_position(self, pair: str) -> Optional[RunningPosition]:
        pos = self._pending_positions.pop(pair, None)
        if not pos:
            return None
        self._running_positions[pair] = pos
        return pos

    async def promote_pending_by_order_id(self, order_id: str) -> Optional[RunningPosition]:
        target_pair = None
        for pair, pos in self._pending_positions.items():
            if str(pos.order_id) == str(order_id):
                target_pair = pair
                break
        if not target_pair:
            return None
        return await self.promote_pending_position(target_pair)

    async def update_sl(self, pair: str, new_sl) -> None:
        if pair in self._running_positions:
            self._running_positions[pair].current_sl = new_sl

    async def update_tp(self, pair: str, tp_levels) -> None:
        if pair in self._running_positions:
            self._running_positions[pair].tp_levels = list(tp_levels)

    async def update_quantity(self, pair: str, new_qty) -> None:
        if pair in self._running_positions:
            self._running_positions[pair].quantity = new_qty

    def get_running_positions(self) -> List[RunningPosition]:
        return list(self._running_positions.values())

    def get_running_pairs(self) -> List[str]:
        return sorted(self._running_positions.keys())

    def get_closed_today(self) -> List[str]:
        return sorted(self._closed_today)

    def has_position(self, pair: str) -> bool:
        return pair in self._running_positions

    def get_position(self, pair: str) -> Optional[RunningPosition]:
        return self._running_positions.get(pair)

    def add_to_allowed_running(self, pair: str) -> None:
        self._allowed_running.add(pair)

    def remove_from_allowed_running(self, pair: str) -> None:
        self._allowed_running.discard(pair)

    def get_allowed_running(self) -> List[str]:
        return sorted(self._allowed_running)

    def get_context_state(self) -> PositionState:
        return PositionState(
            closed_today=self.get_closed_today(),
            running_pairs=sorted(self._running_positions.keys()),
            pending_pairs=sorted(self._pending_positions.keys()),
        )
