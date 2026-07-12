# Tujuan
# Test konfigurasi leverage map parser.
# Caller
# pytest.
# Dependensi
# CaraCrypto.config, CaraCrypto.signal_parser.
# Main Functions
# Validasi property task 1.4.
# Side Effects
# Tidak ada.

from CaraCrypto.config import DEFAULT_LEVERAGE, LEVERAGE_MAP


def test_fixed_leverage_map_property():
    assert LEVERAGE_MAP["BTCUSDT"] == 75
    assert LEVERAGE_MAP["ETHUSDT"] == 50
    assert LEVERAGE_MAP.get("XRPUSDT", DEFAULT_LEVERAGE) == 20
