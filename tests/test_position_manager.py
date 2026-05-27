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
        self.positions = []

    async def get_running_positions(self):
        return self.positions

    async def store_position(self, position):
        self.positions = [p for p in self.positions if p.pair != position.pair]
        self.positions.append(position)

    async def remove_position(self, pair):
        self.positions = [p for p in self.positions if p.pair != pair]


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


@pytest.mark.asyncio
async def test_allowed_running_lifecycle_property():
    db = _DB()
    pm = PositionManager(db)
    await pm.initialize()
    pm.add_to_allowed_running("ETHUSDT")
    assert "ETHUSDT" in pm.get_allowed_running()
    pm.remove_from_allowed_running("ETHUSDT")
    assert "ETHUSDT" not in pm.get_allowed_running()
