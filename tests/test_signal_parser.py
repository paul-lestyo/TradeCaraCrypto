# Tujuan
# Test hint tag dan validasi response parser.
# Caller
# pytest.
# Dependensi
# CaraCrypto.signal_parser, CaraCrypto.models.
# Main Functions
# Validasi property task 9.2.
# Side Effects
# Tidak ada.

from types import SimpleNamespace

from CaraCrypto.models import GeminiAction
from CaraCrypto.signal_parser import SignalParser


class _DB:
    async def update_message_gemini_response(self, *_):
        return None


def _parser():
    return SignalParser(SimpleNamespace(api_key="", model="gemini-2.0-flash"), _DB())


def test_tag_based_classification_hints_property():
    p = _parser()
    assert p._infer_action_hint("[OPEN] BTC") == "new_signal"
    assert p._infer_action_hint("[CLOSED] ETH") == "new_signal"
    assert p._infer_action_hint("[CANCEL] XRP") == "cancel"


def test_invalid_response_rejection_property():
    p = _parser()
    assert p._validate_and_build_action({"action": "unknown"}) is None


def test_skip_action_property():
    p = _parser()
    action = p._validate_and_build_action({"action": "skip"})
    assert action is not None
    assert action.action == GeminiAction.SKIP


def test_null_risk_level_fallback_property():
    p = _parser()
    action = p._validate_and_build_action(
        {
            "action": "new_signal",
            "pair": "BTCUSDT",
            "direction": "long",
            "order_type": "market",
            "entry_price": 100000,
            "risk_level": None,
        }
    )
    assert action is not None
    assert action.risk_level.value == "normal"
