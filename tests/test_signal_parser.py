# Tujuan
# Test hint tag dan validasi response parser.
# Caller
# pytest.
# Dependensi
# CaraCrypto.signal_parser, CaraCrypto.models.
# Main Functions
# Validasi property task 9.2, normalisasi payload Gemini, dan image-aware prompt.
# Side Effects
# Tidak ada.

import io
from types import SimpleNamespace

from decimal import Decimal

from PIL import Image

from CaraCrypto.models import Direction, GeminiAction, MessageContext, OrderType, PositionState, RawSignalMessage
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


def test_uppercase_payload_enum_normalization_property():
    p = _parser()
    action = p._validate_and_build_action(
        {
            "action": "NEW_SIGNAL",
            "pair": "JELLY/USDT",
            "direction": "LONG",
            "order_type": "LIMIT",
            "entry_price": "0.0123",
            "risk_level": "NORMAL",
        }
    )
    assert action is not None
    assert action.action == GeminiAction.NEW_SIGNAL
    assert action.pair == "JELLYUSDT"
    assert action.direction == Direction.LONG
    assert action.order_type == OrderType.LIMIT
    assert action.entry_price == Decimal("0.0123")


def test_prompt_declares_yellow_level_as_entry_property():
    p = _parser()
    context = MessageContext(
        current_message=RawSignalMessage(text="[OPEN] PROM", group_id=-1, message_id=1),
        history=[],
        position_state=PositionState(running_positions=[], running_pairs=[], closed_today=[], allowed_running=[]),
    )
    prompt = p._build_prompt(context)
    assert "garis/label kuning adalah area ENTRY" in prompt
    assert "entry_price wajib diisi" in prompt


def test_gemini_content_includes_image_parts_property():
    p = _parser()
    buf = io.BytesIO()
    Image.new("RGB", (1, 1), color="yellow").save(buf, format="PNG")
    content = p._build_gemini_content("prompt", [buf.getvalue()])
    assert isinstance(content, list)
    assert content[0] == "prompt"
    assert len(content) == 2
