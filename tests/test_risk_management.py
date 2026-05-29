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


class _BalanceClient:
    def __init__(self, balance=Decimal("1000"), fail_times=0):
        self._balance = Decimal(str(balance))
        self._fail_times = int(fail_times)
        self.calls = 0

    def balance(self, **_):
        self.calls += 1
        if self.calls <= self._fail_times:
            raise RuntimeError(f"transient binance error #{self.calls}")
        return [{"asset": "USDT", "availableBalance": str(self._balance)}]


class _Db:
    def __init__(self, daily_loss=Decimal("0")):
        self.daily_loss = daily_loss

    async def get_daily_loss(self, _):
        return self.daily_loss


class _Alert:
    def __init__(self):
        self.messages = []
        self.errors = []

    async def notify_risk_limit(self, message):
        self.messages.append(message)

    async def notify_error(self, source, error):
        self.errors.append((source, error))


class _PM:
    def __init__(self, pairs=None):
        self.pairs = pairs or []

    def has_position(self, pair):
        return pair in self.pairs

    def get_running_pairs(self):
        return list(self.pairs)


def _engine(
    max_positions=5,
    daily_loss_pct=5.0,
    daily_loss=Decimal("0"),
    existing_pairs=None,
    client=None,
):
    risk = SimpleNamespace(
        trade_margin_percent=1.0,
        high_risk_multiplier=0.5,
        max_concurrent_positions=max_positions,
        daily_loss_limit_percent=daily_loss_pct,
    )
    engine = TradeEngine(client or _Client(), _Db(daily_loss), _Alert(), _PM(existing_pairs), risk)
    engine._balance_retry_delay_sec = 0.0
    return engine


@pytest.mark.asyncio
async def test_risk_max_positions_queue_property():
    e = _engine(max_positions=1, existing_pairs=["BTCUSDT"])
    allowed = await e._check_risk_limits(
        "ETHUSDT", Decimal("100"), RiskLevel.NORMAL, 50, account_balance=Decimal("1000")
    )
    assert allowed is False
    assert len(e._queued_actions) == 1


@pytest.mark.asyncio
async def test_risk_daily_loss_refuse_property():
    e = _engine(daily_loss=Decimal("60"), daily_loss_pct=5.0)
    allowed = await e._check_risk_limits(
        "BTCUSDT", Decimal("100"), RiskLevel.NORMAL, 50, account_balance=Decimal("1000")
    )
    assert allowed is False


@pytest.mark.asyncio
async def test_risk_insufficient_balance_skip_property():
    e = _engine()
    e.risk_config.trade_margin_percent = 200.0
    allowed = await e._check_risk_limits(
        "BTCUSDT", Decimal("100"), RiskLevel.NORMAL, 50, account_balance=Decimal("1000")
    )
    assert allowed is False


@pytest.mark.asyncio
async def test_balance_fetch_succeeds_after_retry():
    client = _BalanceClient(balance=Decimal("500"), fail_times=2)
    e = _engine(client=client)
    balance = await e._get_account_balance()
    assert balance == Decimal("500")
    assert client.calls == 3
    assert e.alert_service.errors == []


@pytest.mark.asyncio
async def test_balance_fetch_alerts_after_three_failures():
    client = _BalanceClient(fail_times=10)
    e = _engine(client=client)
    balance = await e._get_account_balance()
    assert balance is None
    assert client.calls == 3
    assert len(e.alert_service.errors) == 1
    source, detail = e.alert_service.errors[0]
    assert source == "trade_engine_balance"
    assert "after 3 attempts" in detail


@pytest.mark.asyncio
async def test_check_risk_limits_skips_when_balance_unavailable():
    e = _engine()
    allowed = await e._check_risk_limits("BTCUSDT", Decimal("100"), RiskLevel.NORMAL, 50)
    assert allowed is False
    assert len(e.alert_service.errors) == 1
