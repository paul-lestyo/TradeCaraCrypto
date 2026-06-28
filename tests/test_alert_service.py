# Tujuan
# Test format body alert, konten notifikasi, dan rumus PnL.
# Caller
# pytest.
# Dependensi
# CaraCrypto.alert_service, CaraCrypto.models.
# Main Functions
# Validasi property task 5.2.
# Side Effects
# Tidak ada.

import pytest

from CaraCrypto.alert_service import AlertService
from CaraCrypto.models import Direction


class _Cfg:
    base_url = "https://example.com"
    token = "abc"
    phone = "628123"


@pytest.mark.asyncio
async def test_alert_message_body_format_property():
    service = AlertService(_Cfg())
    sent = []

    async def _fake_send(msg):
        sent.append(msg)

    service.send_alert = _fake_send
    await service.notify_error("unit", "boom")
    assert sent[0] == "ERROR\nsource=unit\ndetail=boom"


@pytest.mark.asyncio
async def test_order_notification_content_property():
    service = AlertService(_Cfg())
    sent = []

    async def _fake_send(msg):
        sent.append(msg)

    service.send_alert = _fake_send
    await service.notify_new_order("BTCUSDT", "long", "105000", "market")
    assert "BTCUSDT" in sent[0]
    assert "long" in sent[0]
    assert "105000" in sent[0]
    assert "market" in sent[0]


def test_pnl_formula_property():
    service = AlertService(_Cfg())
    assert round(service._calculate_pnl_percent(100.0, 110.0, Direction.LONG), 4) == 10.0
    assert round(service._calculate_pnl_percent(100.0, 90.0, Direction.SHORT), 4) == 10.0


@pytest.mark.asyncio
async def test_notify_new_order_detail_compact_format_property():
    service = AlertService(_Cfg())
    sent = []

    async def _fake_send(msg):
        sent.append(msg)

    service.send_alert = _fake_send
    await service.notify_new_order_detail(
        pair="BTCUSDT",
        direction="long",
        order_type="market",
        entry_price="59690.39623913",
        quantity="0.001",
        leverage=125,
        margin_used_usd="0.48",
        stop_loss="58000.0",
        take_profit="64772.80",
        reason="new_signal"
    )
    msg = sent[0]
    assert "🟢 BTCUSDT LONG" in msg
    assert "MKT @ 59690.40" in msg
    assert "💼 Pos $59.69" in msg
    assert "Margin $0.48 (125x)" in msg
    assert "🎯 TP 64772.80" in msg
    assert "+1064.33%" in msg
    assert "+$5.08" in msg
    assert "🛑 SL 58000.00" in msg
    assert "-353.99%" in msg
    assert "-$1.69" in msg


@pytest.mark.asyncio
async def test_notify_new_order_detail_double_entry_scenarios_property():
    service = AlertService(_Cfg())
    sent = []

    async def _fake_send(msg):
        sent.append(msg)

    service.send_alert = _fake_send
    # Entry 1: 0.001 BTC @ 59690.40
    # Entry 2: 0.004 BTC @ 59400.00
    # Leverage: 50
    # TP: 64772.80
    # SL: 58000.00
    await service.notify_new_order_detail(
        pair="BTCUSDT",
        direction="long",
        order_type="double_entry:market/limit",
        entry_price="59690.40/59400.00",
        quantity="0.001/0.004",
        leverage=50,
        margin_used_usd="1.20",
        stop_loss="58000.0",
        take_profit="64772.80",
        reason="new_signal"
    )
    msg = sent[0]
    assert "🟢 BTCUSDT LONG (Double Entry)" in msg
    assert "① MKT @ 59690.40" in msg
    assert "Pos $59.69 | Margin $1.19" in msg
    assert "② LMT @ 59400.00" in msg
    assert "Pos $237.60 | Margin $4.75" in msg
    assert "💼 Total Pos $297.29" in msg
    assert "Margin $5.95 (50x)" in msg
    assert "🎯 TP 64772.80" in msg
    assert "🛑 SL 58000.00" in msg
    assert "📊 Scenarios" in msg
    # E1 -> TP: (64772.80 - 59690.40) * 0.001 = 5.0824 -> +$5.08
    assert "① E1 → TP +$5.08" in msg
    # E1 + E2 -> TP: (64772.80 - 59690.40) * 0.001 + (64772.80 - 59400.00) * 0.004 = 5.0824 + 5.3728 * 4 = 5.0824 + 21.4912 = 26.5736 -> +$26.57
    assert "② E1 + E2 → TP +$26.57" in msg
    # E1 + E2 -> SL: (58000.00 - 59690.40) * 0.001 + (58000.00 - 59400.00) * 0.004 = -1.6904 + (-1.400) * 4 = -1.6904 - 5.60 = -7.2904 -> -$7.29
    assert "③ E1 + E2 → SL -$7.29" in msg
