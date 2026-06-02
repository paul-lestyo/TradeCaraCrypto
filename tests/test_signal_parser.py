# Tujuan
# Test hint tag dan validasi response parser.
# Caller
# pytest.
# Dependensi
# CaraCrypto.signal_parser, CaraCrypto.models.
# Main Functions
# Validasi property task 9.2, normalisasi payload Gemini, guard teks aksi, dan image-aware prompt.
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
    assert p._infer_action_hint("USUAL kami cancel. Gak gerak.") == "cancel"
    assert p._infer_action_hint("USUAL lanjut hold, persempit SL di 0.01285") == "update_sl"


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
            "entry_zone": ["0.0122", "0.0124"],
            "entry_price": "0.0123",
            "risk_level": "NORMAL",
        }
    )
    assert action is not None
    assert action.action == GeminiAction.NEW_SIGNAL
    assert action.pair == "JELLYUSDT"
    assert action.direction == Direction.LONG
    assert action.order_type is None
    assert action.entry_zone == [Decimal("0.0122"), Decimal("0.0124")]
    assert action.entry_price == Decimal("0.0123")


def test_prompt_declares_yellow_level_as_entry_property():
    p = _parser()
    context = MessageContext(
        current_message=RawSignalMessage(text="[OPEN] PROM", group_id=-1, message_id=1),
        history=[],
        position_state=PositionState(closed_today=[]),
    )
    prompt = p._build_prompt(context)
    assert "garis/label kuning adalah area ENTRY" in prompt
    assert "Jangan ambil bid/ask box, current price" in prompt
    assert "untuk SHORT gunakan level kuning paling atas" in prompt
    assert "untuk LONG gunakan level kuning paling bawah" in prompt
    assert "isi entry_zone sebagai array [lower, upper]" in prompt
    assert "Aksi utama wajib ditentukan dari Message saat ini" in prompt
    assert "PRIORITAS WAJIB: Message saat ini >> Exchange state >> Local position state >> History >> Reply text" in prompt
    assert "Reply text hanya informasi tambahan, bukan sumber utama aksi" in prompt
    assert "Jika Reply text berisi trade plan [OPEN]/entry/TP/SL lama, jangan dijadikan aksi baru" in prompt


def test_gemini_content_includes_image_parts_property():
    p = _parser()
    buf = io.BytesIO()
    Image.new("RGB", (1, 1), color="yellow").save(buf, format="PNG")
    content = p._build_gemini_content("prompt", [buf.getvalue()])
    assert isinstance(content, list)
    assert content[0] == "prompt"
    assert content[1] == "Image:"
    assert len(content) == 3


def test_current_cancel_text_overrides_reply_signal_payload_property():
    p = _parser()
    context = MessageContext(
        current_message=RawSignalMessage(
            text="USUAL kami cancel. Gak gerak. Kalo lanjut hold, persempit SL di 0.01285",
            group_id=-1,
            message_id=5239,
            reply_text="[OPEN] USUAL long setup",
        ),
        history=[],
        position_state=PositionState(closed_today=[]),
    )
    payload = {
        "action": "new_signal",
        "pair": "USUALUSDT",
        "direction": "LONG",
        "entry_zone": [0.01283, 0.01311],
        "take_profit_levels": [0.01342, 0.0155],
        "stop_loss": 0.01261,
    }
    guarded = p._apply_current_text_guard(context, payload)
    action = p._validate_and_build_action(guarded)
    assert action is not None
    assert action.action == GeminiAction.CANCEL
    assert action.pair == "USUALUSDT"


def test_current_sl_update_text_overrides_old_reply_sl_property():
    p = _parser()
    context = MessageContext(
        current_message=RawSignalMessage(
            text="USUAL lanjut hold, persempit SL di 0.01285",
            group_id=-1,
            message_id=5240,
            reply_text="[OPEN] USUAL old setup",
        ),
        history=[],
        position_state=PositionState(closed_today=[]),
    )
    payload = {
        "action": "new_signal",
        "pair": "USUALUSDT",
        "direction": "LONG",
        "entry_zone": [0.01283, 0.01311],
        "stop_loss": 0.01261,
    }
    guarded = p._apply_current_text_guard(context, payload)
    action = p._validate_and_build_action(guarded)
    assert action is not None
    assert action.action == GeminiAction.UPDATE_SL
    assert action.pair == "USUALUSDT"
    assert action.stop_loss == Decimal("0.01285")


def test_current_sl_update_text_parses_k_suffix_property():
    p = _parser()
    context = MessageContext(
        current_message=RawSignalMessage(
            text="BTC ngeri gk ada buyernya. Probably hit SL soon, tapi kami akan geser SL ke 68,5K",
            group_id=-1,
            message_id=5282,
            reply_text="[OPEN] BTC old setup",
        ),
        history=[],
        position_state=PositionState(closed_today=[]),
    )
    payload = {
        "action": "update_sl",
        "pair": "BTCUSDT",
        "direction": "LONG",
        "stop_loss": 68500,
    }
    guarded = p._apply_current_text_guard(context, payload)
    action = p._validate_and_build_action(guarded)
    assert action is not None
    assert action.action == GeminiAction.UPDATE_SL
    assert action.stop_loss == Decimal("68500")


def test_now_literal_forces_market_order_override_property():
    p = _parser()
    context = MessageContext(
        current_message=RawSignalMessage(
            text="[OPEN] D long entry now now now",
            group_id=-1,
            message_id=6001,
            reply_text="[OPEN] D old plan",
        ),
        history=[],
        position_state=PositionState(closed_today=[]),
    )
    payload = {
        "action": "new_signal",
        "pair": "DUSDT",
        "direction": "LONG",
        "entry_zone": [0.0114, 0.0117],
    }
    guarded = p._apply_current_text_guard(context, payload)
    action = p._validate_and_build_action(guarded)
    assert action is not None
    assert action.order_type == OrderType.MARKET


def test_single_now_word_forces_market_order_override_property():
    p = _parser()
    context = MessageContext(
        current_message=RawSignalMessage(
            text="[CLOSED] APT short now.",
            group_id=-1,
            message_id=6002,
            reply_text="old plan",
        ),
        history=[],
        position_state=PositionState(closed_today=[]),
    )
    payload = {
        "action": "new_signal",
        "pair": "APTUSDT",
        "direction": "SHORT",
    }
    guarded = p._apply_current_text_guard(context, payload)
    action = p._validate_and_build_action(guarded)
    assert action is not None
    assert action.order_type == OrderType.MARKET
