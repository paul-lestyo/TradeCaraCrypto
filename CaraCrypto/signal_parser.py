# Tujuan
# Parser Gemini single-call untuk extraction + classification sekaligus.
# Caller
# __main__ signal processing loop.
# Dependensi
# google.generativeai, Pillow, database, models.
# Main Functions
# `parse_and_classify`, image-aware Gemini call, validasi enum tolerant-case, normalisasi pair.
# Side Effects
# Memanggil Gemini API dan update row messages.

from __future__ import annotations

import asyncio
import io
import json
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Optional

import google.generativeai as genai
from PIL import Image

from .config import GeminiConfig
from .database import Database
from .models import Direction, GeminiAction, MessageContext, OrderType, RiskLevel, TradeAction


class SignalParser:
    def __init__(self, config: GeminiConfig, db: Database):
        self.config = config
        self.db = db
        if self.config.api_key:
            genai.configure(api_key=self.config.api_key)
        print(
            "[SignalParser] Ready "
            f"model={self.config.model} "
            f"api_key={'set' if bool(self.config.api_key) else 'missing'}"
        )

    def _build_prompt(self, context: MessageContext) -> str:
        order_hint = self._infer_order_type_hint(context.current_message.text)
        action_hint = self._infer_action_hint(context.current_message.text)
        return (
            "Kamu parser signal Caracrypto. Return JSON saja dengan field: "
            "action, pair, direction, order_type, entry_zone, entry_price, take_profit_levels, stop_loss, risk_level, close_percentage. "
            "Action valid: new_signal, update_sl, set_sl_breakeven, tp_partial, cancel, reverse, re_entry, cutloss, skip. "
            f"Message: {context.current_message.text}\n"
            f"Reply text: {context.current_message.reply_text}\n"
            f"History: {context.history}\n"
            f"Exchange state (Binance): {context.exchange_state}\n"
            f"Order hint: {order_hint}\n"
            f"Action hint: {action_hint}\n"
            "Hint: market hanya jika ada perintah eksplisit seperti 'order now', 'open now', atau 'market order'. "
            "Kata 'now' saja tanpa perintah eksplisit jangan dianggap market. "
            "antri/limit/kuning/tunggu kuning => limit. "
            "Untuk image chart Caracrypto: garis/label kuning adalah area ENTRY; isi entry_zone sebagai array [lower, upper] dari area entry kuning. "
            "Jangan ambil bid/ask box, current price, candle price, atau label harga non-kuning sebagai entry. "
            "Jika ada beberapa label/garis kuning, tentukan direction dulu: untuk SHORT gunakan level kuning paling atas/upper entry zone; untuk LONG gunakan level kuning paling bawah/lower entry zone. "
            "Garis/label merah biasanya STOP LOSS. Garis/label hijau biasanya TAKE PROFIT bertingkat. "
            "Jika entry_zone tidak bisa didapat, baru fallback ke entry_price tunggal. Jika order_type=limit, entry_zone/entry_price wajib diisi dari level entry visual/teks. "
            "Gunakan Exchange state (Binance) untuk memahami pair yang benar-benar sedang open position / open order. "
            "Tag [OPEN]/[CLOSED] condong new_signal, [CANCEL] condong cancel."
        )

    def _infer_order_type_hint(self, text: str) -> str:
        t = (text or "").lower()
        explicit_market_phrases = (
            "order now",
            "open now",
            "entry now",
            "market order",
            "execute now",
        )
        if any(phrase in t for phrase in explicit_market_phrases):
            return "market"
        if any(k in t for k in ["antri", "limit", "kuning", "tunggu kuning"]):
            return "limit"
        return "unknown"

    def _infer_action_hint(self, text: str) -> str:
        t = (text or "").upper()
        if "[CANCEL]" in t:
            return "cancel"
        if "[OPEN]" in t or "[CLOSED]" in t:
            return "new_signal"
        return "unknown"

    def _collect_images(self, context: MessageContext) -> list:
        images = []
        if context.current_message.image_data:
            images.append(context.current_message.image_data)
        if context.current_message.reply_image_data:
            images.append(context.current_message.reply_image_data)
        return images

    def _build_gemini_content(self, prompt: str, images: list) -> Any:
        if not images:
            return prompt
        content = [prompt]
        for image_data in images:
            try:
                with Image.open(io.BytesIO(image_data)) as img:
                    content.append(img.copy())
            except Exception:
                continue
        return content if len(content) > 1 else prompt

    async def _call_gemini(self, prompt: str, images: Optional[list] = None) -> Dict[str, Any]:
        model = genai.GenerativeModel(self.config.model)
        content = self._build_gemini_content(prompt, images or [])
        for i in range(3):
            try:
                resp = await asyncio.to_thread(model.generate_content, content)
                text = (resp.text or "").strip()
                if text.startswith("```"):
                    text = text.strip("`")
                    text = text.replace("json", "", 1).strip()
                return json.loads(text)
            except Exception:
                if i == 2:
                    raise
                await asyncio.sleep(5)
        return {"action": "skip"}

    def _normalize_enum_value(self, value: Any) -> str:
        if isinstance(value, Enum):
            value = value.value
        return str(value).strip().lower()

    def _enum_from_payload(self, enum_type, value: Any):
        return enum_type(self._normalize_enum_value(value))

    def _normalize_pair(self, pair: Any) -> Optional[str]:
        if pair is None:
            return None
        symbol = str(pair).strip().upper().replace("/", "").replace(" ", "")
        return symbol or None

    def _validate_and_build_action(self, payload: Dict[str, Any]) -> Optional[TradeAction]:
        try:
            action = self._enum_from_payload(GeminiAction, payload["action"])
        except Exception:
            return None
        if action == GeminiAction.SKIP:
            return TradeAction(action=action, raw_response=payload)
        try:
            pair = self._normalize_pair(payload.get("pair"))
            direction = self._enum_from_payload(Direction, payload["direction"]) if payload.get("direction") else None
            order_type = self._enum_from_payload(OrderType, payload["order_type"]) if payload.get("order_type") else None
            entry_price = Decimal(str(payload["entry_price"])) if payload.get("entry_price") is not None else None
            entry_zone = [Decimal(str(x)) for x in payload.get("entry_zone", [])] if payload.get("entry_zone") else None
            tp_levels = [Decimal(str(x)) for x in payload.get("take_profit_levels", [])] if payload.get("take_profit_levels") else None
            stop_loss = Decimal(str(payload["stop_loss"])) if payload.get("stop_loss") is not None else None
        except Exception:
            return None
        risk_raw = payload.get("risk_level", "normal")
        try:
            risk_level = self._enum_from_payload(RiskLevel, risk_raw or "normal")
        except Exception:
            risk_level = RiskLevel.NORMAL
        return TradeAction(
            action=action,
            pair=pair,
            direction=direction,
            order_type=order_type,
            entry_price=entry_price,
            entry_zone=entry_zone,
            take_profit_levels=tp_levels,
            stop_loss=stop_loss,
            risk_level=risk_level,
            close_percentage=payload.get("close_percentage"),
            raw_response=payload,
        )

    async def parse_and_classify(self, context: MessageContext, message_db_id: int) -> Optional[TradeAction]:
        prompt = self._build_prompt(context)
        images = self._collect_images(context)
        print(
            "[SignalParser] "
            f"message_id={context.current_message.message_id} "
            f"group={context.current_message.group_id} "
            f"images={len(images)} "
            f"history={len(context.history)}"
        )
        payload = await self._call_gemini(prompt, images)
        print(f"[SignalParser] Gemini parsed payload={payload}")
        action = self._validate_and_build_action(payload)
        if not action:
            print(f"[SignalParser] Invalid payload -> skip message_db_id={message_db_id}")
            await self.db.update_message_gemini_response(message_db_id, payload, "skip")
            return None
        print(
            "[SignalParser] "
            f"message_db_id={message_db_id} action={action.action.value} "
            f"pair={action.pair} order_type={action.order_type.value if action.order_type else None} "
            f"risk={action.risk_level.value}"
        )
        await self.db.update_message_gemini_response(message_db_id, payload, action.action.value)
        return action
