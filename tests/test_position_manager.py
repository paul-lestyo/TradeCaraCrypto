# Tujuan
# Test konsistensi state PositionManager.
# Caller
# pytest.
# Dependensi
# CaraCrypto.position_manager, CaraCrypto.models.
# Main Functions
# Validasi property task 6.2.
# Side Effects
# Tidak ada.

from datetime import datetime
from decimal import Decimal

import pytest

from CaraCrypto.models import Direction, RunningPosition
from CaraCrypto.position_manager import PositionManager


class _DB:
    def __init__(self):
        self.positions = {}

    async def get_running_positions(self):
        return [pos for pos, status in self.positions.values() if status == "running"]

    async def get_pending_positions(self):
        return [pos for pos, status in self.positions.values() if status == "pending"]

    async def store_position(self, position, status="running"):
        self.positions[position.pair] = (position, status)

    async def remove_position(self, pair):
        self.positions.pop(pair, None)

    async def update_position_status(self, pair, status):
        pos, _ = self.positions[pair]
        self.positions[pair] = (pos, status)

    async def update_position_sl(self, pair, new_sl):
        self.positions[pair][0].current_sl = new_sl

    async def update_position_tp(self, pair, tp_levels):
        self.positions[pair][0].tp_levels = list(tp_levels)

    async def update_position_quantity(self, pair, new_qty):
        self.positions[pair][0].quantity = new_qty


def _pos(pair="BTCUSDT"):
    return RunningPosition(
        pair=pair,
        direction=Direction.LONG,
        entry_price=Decimal("100"),
        current_sl=Decimal("90"),
        tp_levels=[Decimal("110")],
        leverage=50,
        order_id="1",
        quantity=Decimal("0.1"),
        opened_at=datetime.utcnow(),
    )


@pytest.mark.asyncio
async def test_position_state_consistency_property():
    db = _DB()
    pm = PositionManager(db)
    await pm.initialize()
    await pm.add_position(_pos("BTCUSDT"))
    assert pm.has_position("BTCUSDT")
    assert set(pm.get_running_pairs()) == {"BTCUSDT"}
    await pm.remove_position("BTCUSDT")
    assert not pm.has_position("BTCUSDT")
    assert "BTCUSDT" in pm.get_closed_today()
    assert "BTCUSDT" not in db.positions


@pytest.mark.asyncio
async def test_initialize_reloads_positions_from_db_property():
    db = _DB()
    await db.store_position(_pos("FIGHTUSDT"), status="running")
    await db.store_position(_pos("PHAUSDT"), status="pending")
    pm = PositionManager(db)
    await pm.initialize()
    assert pm.has_position("FIGHTUSDT")
    assert pm.has_pending_position("PHAUSDT")


@pytest.mark.asyncio
async def test_pending_promotion_persists_running_status_property():
    db = _DB()
    pm = PositionManager(db)
    await pm.initialize()
    await pm.add_pending_position(_pos("PHAUSDT"))
    promoted = await pm.promote_pending_position("PHAUSDT")
    assert promoted is not None
    assert pm.has_position("PHAUSDT")
    assert not pm.has_pending_position("PHAUSDT")
    assert db.positions["PHAUSDT"][1] == "running"


@pytest.mark.asyncio
async def test_allowed_running_lifecycle_property():
    db = _DB()
    pm = PositionManager(db)
    await pm.initialize()
    pm.add_to_allowed_running("ETHUSDT")
    assert "ETHUSDT" in pm.get_allowed_running()
    pm.remove_from_allowed_running("ETHUSDT")
    assert "ETHUSDT" not in pm.get_allowed_running()
