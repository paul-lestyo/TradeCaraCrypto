# Tujuan
# Property tests risk management di TradeEngine.
# Caller
# pytest.
# Dependensi
# CaraCrypto.trade_engine.
# Main Functions
# Validasi skenario 15.2.
# Side Effects
# Tidak ada.

from decimal import Decimal
from types import SimpleNamespace

import pytest

from CaraCrypto.models import RiskLevel
from CaraCrypto.trade_engine import TradeEngine


class _Client:
    pass


class _Db:
    def __init__(self, daily_loss=Decimal("0")):
        self.daily_loss = daily_loss

    async def get_daily_loss(self, _):
        return self.daily_loss


class _Alert:
    def __init__(self):
        self.messages = []

    async def notify_risk_limit(self, message):
        self.messages.append(message)


class _PM:
    def __init__(self, pairs=None):
        self.pairs = pairs or []

    def has_position(self, pair):
        return pair in self.pairs

    def get_running_pairs(self):
        return list(self.pairs)


def _engine(max_positions=5, max_size_pct=10.0, daily_loss_pct=5.0, daily_loss=Decimal("0"), existing_pairs=None):
    risk = SimpleNamespace(
        trade_margin_percent=1.0,
        high_risk_multiplier=0.5,
        max_concurrent_positions=max_positions,
        max_position_size_percent=max_size_pct,
        daily_loss_limit_percent=daily_loss_pct,
    )
    return TradeEngine(_Client(), _Db(daily_loss), _Alert(), _PM(existing_pairs), risk)


@pytest.mark.asyncio
async def test_risk_max_positions_queue_property():
    e = _engine(max_positions=1, max_size_pct=200.0, existing_pairs=["BTCUSDT"])
    allowed = await e._check_risk_limits("ETHUSDT", Decimal("100"), RiskLevel.NORMAL, 50)
    assert allowed is False
    assert len(e._queued_actions) == 1


@pytest.mark.asyncio
async def test_risk_max_size_reject_property():
    e = _engine(max_size_pct=1.0)
    allowed = await e._check_risk_limits("BTCUSDT", Decimal("100"), RiskLevel.NORMAL, 125)
    assert allowed is False


@pytest.mark.asyncio
async def test_risk_daily_loss_refuse_property():
    # balance placeholder engine=1000, limit 5%=50, daily loss 60 => refuse.
    e = _engine(daily_loss=Decimal("60"), daily_loss_pct=5.0)
    allowed = await e._check_risk_limits("BTCUSDT", Decimal("100"), RiskLevel.NORMAL, 50)
    assert allowed is False


@pytest.mark.asyncio
async def test_risk_insufficient_balance_skip_property():
    # trade_margin_percent super tinggi memaksa margin > balance.
    e = _engine()
    e.risk_config.trade_margin_percent = 200.0
    allowed = await e._check_risk_limits("BTCUSDT", Decimal("100"), RiskLevel.NORMAL, 50)
    assert allowed is False
