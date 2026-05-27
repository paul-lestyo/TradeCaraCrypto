# Tujuan
# Test konfigurasi leverage map dan hint order type parser.
# Caller
# pytest.
# Dependensi
# CaraCrypto.config, CaraCrypto.signal_parser.
# Main Functions
# Validasi property task 1.4.
# Side Effects
# Tidak ada.

from CaraCrypto.config import DEFAULT_LEVERAGE, LEVERAGE_MAP
from CaraCrypto.signal_parser import SignalParser


class _DummyDB:
    pass


def test_fixed_leverage_map_property():
    assert LEVERAGE_MAP["BTCUSDT"] == 125
    assert LEVERAGE_MAP["ETHUSDT"] == 100
    assert LEVERAGE_MAP.get("XRPUSDT", DEFAULT_LEVERAGE) == 50


def test_order_type_keyword_detection_property():
    parser = SignalParser(type("Cfg", (), {"api_key": "", "model": "x"})(), _DummyDB())
    assert parser._infer_order_type_hint("entry NOW") == "market"
    assert parser._infer_order_type_hint("open now") == "market"
    assert parser._infer_order_type_hint("antri di kuning") == "limit"
    assert parser._infer_order_type_hint("pasang LIMIT dulu") == "limit"
