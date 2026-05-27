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
