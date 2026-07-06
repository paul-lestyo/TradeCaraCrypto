# Tujuan
# Parser Gemini single-call untuk extraction + classification sekaligus.
# Caller
# __main__ signal processing loop.
# Dependensi
# google.generativeai, Pillow, database, models.
# Main Functions
# `parse_and_classify`, image-aware Gemini call, guard teks aksi, validasi enum tolerant-case, normalisasi pair.
# Side Effects
# Memanggil Gemini API dan update row messages.

from __future__ import annotations

import asyncio
import io
import json
import re
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import google.generativeai as genai
from PIL import Image

from .config import GeminiConfig
from .database import Database
from .models import Direction, GeminiAction, MessageContext, OrderType, RiskLevel, TradeAction


class SignalParser:
    _JKT_TZ = ZoneInfo("Asia/Jakarta")
    _PAIR_STOPWORDS = {
        "ACTION",
        "BATAL",
        "BATALKAN",
        "BE",
        "BEP",
        "CANCEL",
        "CANCELED",
        "CANCELLED",
        "CLOSED",
        "DI",
        "GAK",
        "GA",
        "GERAK",
        "HOLD",
        "KALO",
        "KALAU",
        "KAMI",
        "KE",
        "LANJUT",
        "LONG",
        "OPEN",
        "PERSEMPIT",
        "SET",
        "SHORT",
        "SL",
        "STOP",
        "TP",
        "UPDATE",
        "USDT",
    }

    def __init__(self, config: GeminiConfig, db: Database):
        self.config = config
        self.db = db
        if self.config.api_key:
            genai.configure(api_key=self.config.api_key)
        print(
            f"{self._log_prefix()} Ready "
            f"model={self.config.model} "
            f"api_key={'set' if bool(self.config.api_key) else 'missing'}"
        )

    def _log_prefix(self) -> str:
        now_jkt = datetime.now(self._JKT_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
        return f"[{now_jkt}] [SignalParser]"

    def _build_prompt(self, context: MessageContext) -> str:
        action_hint = self._infer_action_hint(context.current_message.text)
        return (
            "Kamu parser signal Caracrypto. Return JSON saja dengan field: "
            "action, pair, direction, entry_zone, entry_price, take_profit_levels, stop_loss, risk_level, close_percentage. "
            "Action valid: new_signal, update_sl, set_sl_breakeven, tp_partial, cancel, reverse, re_entry, cutloss, skip. "
            f"Message: {context.current_message.text}\n"
            f"Reply text: {context.current_message.reply_text}\n"
            f"History: {context.history}\n"
            f"Local position state: {context.position_state}\n"
            f"Exchange state (Binance): {context.exchange_state}\n"
            f"Action hint: {action_hint}\n"
            "PRIORITAS WAJIB: Message saat ini >> Exchange state >> Local position state >> History >> Reply text. "
            "Aksi utama wajib ditentukan dari Message saat ini. "
            "Reply text hanya informasi tambahan, bukan sumber utama aksi. "
            "Jika Reply text berisi trade plan [OPEN]/entry/TP/SL lama, jangan dijadikan aksi baru. "
            "Jangan mengambil action, direction, entry, TP, atau SL dari Reply text kecuali untuk klarifikasi minor saat Message saat ini ambigu. "
            "Reply text, history, dan image reply hanya konteks pair/level. "
            "Jika Message berisi cancel/batal/kami cancel, action wajib cancel walaupun reply/history/image terlihat seperti [OPEN]. "
            "Jika Message berisi geser/update/persempit SL, action wajib update_sl dan ambil angka SL dari Message. "
            "Jangan isi order_type. Penentuan market/limit dilakukan engine. "
            "Untuk image chart Caracrypto: garis/label kuning adalah area ENTRY; isi entry_zone sebagai array [lower, upper] dari area entry kuning. "
            "Jangan ambil bid/ask box, current price, candle price, atau label harga non-kuning sebagai entry. "
            "Jika ada beberapa label/garis kuning, tentukan direction dulu: untuk SHORT gunakan level kuning paling atas/upper entry zone; untuk LONG gunakan level kuning paling bawah/lower entry zone. "
            "Garis/label merah biasanya STOP LOSS. Garis/label hijau biasanya TAKE PROFIT bertingkat. "
            "Jika entry_zone tidak bisa didapat, baru fallback ke entry_price tunggal. "
            "Gunakan Exchange state (Binance) untuk memahami pair yang benar-benar sedang open position / open order. "
            "Tag [OPEN]/[CLOSED] condong new_signal, [CANCEL] condong cancel."
        )

    def _infer_action_hint(self, text: str) -> str:
        raw = text or ""
        t = raw.upper()
        lowered = raw.lower()
        if "[CANCEL]" in t or re.search(r"\b(cancel(?:ed|led)?|batal(?:kan)?|dibatal(?:kan)?|kami\s+cancel)\b", lowered):
            return "cancel"
        if "[OPEN]" in t or "[CLOSED]" in t:
            return "new_signal"
        if re.search(r"\b(cut\s*loss|cutloss|cl)\b", lowered):
            return "cutloss"
        if re.search(r"\b(sl\+|stop\s*loss)(?!\w).*\b(be|bep|break\s*even|breakeven|modal)\b", lowered):
            return "set_sl_breakeven"
        if re.search(r"\b(persempit|geser|pindah|naikkan|naikin|turunkan|turunin|ubah|update|set)\b.{0,40}\b(sl\+|stop\s*loss)(?!\w)", lowered):
            return "update_sl"
        if re.search(r"\b(sl\+|stop\s*loss)(?!\w).{0,40}\b(di|ke|jadi|to|at)\b", lowered):
            return "update_sl"
        return "unknown"

    def _collect_images(self, context: MessageContext) -> list:
        images = []
        if context.current_message.image_data:
            images.append(("Current message image", context.current_message.image_data))
        if context.current_message.reply_image_data:
            images.append(("Reply context image", context.current_message.reply_image_data))
        return images

    def _build_gemini_content(self, prompt: str, images: list) -> Any:
        if not images:
            return prompt
        content = [prompt]
        for image_part in images:
            if isinstance(image_part, tuple) and len(image_part) == 2:
                label, image_data = image_part
            else:
                label, image_data = "Image", image_part
            try:
                with Image.open(io.BytesIO(image_data)) as img:
                    content.append(f"{label}:")
                    content.append(img.copy())
            except Exception:
                continue
        return content if len(content) > 1 else prompt

    def _extract_pair_from_text(self, text: Optional[str]) -> Optional[str]:
        if not text:
            return None
        upper = text.upper()
        usdt_match = re.search(r"\b([A-Z0-9]{2,20})\s*/?\s*USDT\b", upper)
        if usdt_match:
            return f"{usdt_match.group(1)}USDT"
        for token in re.findall(r"\b[A-Z0-9]{2,15}\b", upper):
            if token in self._PAIR_STOPWORDS or token.isdigit():
                continue
            return f"{token}USDT"
        return None

    def _extract_stop_loss_from_text(self, text: Optional[str]) -> Optional[Decimal]:
        if not text:
            return None
        patterns = (
            r"\bSL\+?(?!\w)[^\d+-]{0,30}([0-9]+(?:[.,][0-9]+)?(?:\s*[KMB])?)",
            r"\bstop\s*loss\b[^\d+-]{0,30}([0-9]+(?:[.,][0-9]+)?(?:\s*[KMB])?)",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            return self._parse_numeric_token(match.group(1))
        return None

    def _parse_numeric_token(self, token: Optional[str]) -> Optional[Decimal]:
        if not token:
            return None
        raw = str(token).strip().upper().replace(" ", "")
        multiplier = Decimal("1")
        suffix = raw[-1:] if raw else ""
        if suffix in {"K", "M", "B"}:
            raw = raw[:-1]
            multiplier = {
                "K": Decimal("1000"),
                "M": Decimal("1000000"),
                "B": Decimal("1000000000"),
            }[suffix]
        try:
            value = Decimal(raw.replace(",", "."))
        except Exception:
            return None
        return value * multiplier

    def _has_now_market_override(self, text: Optional[str]) -> bool:
        if not text:
            return False
        lowered = text.lower()
        # Trigger tegas untuk "entry now now now" dan variasi sejenis.
        if re.search(r"\b(now)\b(?:\W+\b(now)\b){1,}", lowered):
            return True
        if re.search(r"\b(entry|masuk)\b.{0,20}\bnow\b", lowered):
            return True
        if re.search(r"\bnow\b", lowered):
            return True
        return False

    def _apply_current_text_guard(self, context: MessageContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        action_hint = self._infer_action_hint(context.current_message.text)
        guarded_actions = {"cancel", "update_sl", "set_sl_breakeven", "cutloss"}
        if action_hint not in guarded_actions and not self._has_now_market_override(context.current_message.text):
            return payload

        guarded = dict(payload)
        if action_hint in guarded_actions:
            guarded["action"] = action_hint
        pair = guarded.get("pair")
        if not pair:
            pair = self._extract_pair_from_text(context.current_message.text)
        if not pair:
            pair = self._extract_pair_from_text(context.current_message.reply_text)
        if pair:
            guarded["pair"] = pair

        if action_hint == "update_sl":
            stop_loss = self._extract_stop_loss_from_text(context.current_message.text)
            if stop_loss is not None:
                guarded["stop_loss"] = str(stop_loss)
        if self._has_now_market_override(context.current_message.text):
            guarded["order_type"] = "market"
        return guarded

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
            f"{self._log_prefix()} "
            f"message_id={context.current_message.message_id} "
            f"group={context.current_message.group_id} "
            f"images={len(images)} "
            f"history={len(context.history)}"
        )
        payload = await self._call_gemini(prompt, images)
        print(f"{self._log_prefix()} Gemini parsed payload={payload}")
        guarded_payload = self._apply_current_text_guard(context, payload)
        if guarded_payload != payload:
            print(f"{self._log_prefix()} Text guard adjusted payload={guarded_payload}")
        payload = guarded_payload
        action = self._validate_and_build_action(payload)
        if not action:
            print(f"{self._log_prefix()} Invalid payload -> skip message_db_id={message_db_id}")
            await self.db.update_message_gemini_response(message_db_id, payload, "skip")
            return None
        print(
            f"{self._log_prefix()} "
            f"message_db_id={message_db_id} action={action.action.value} "
            f"pair={action.pair} "
            f"risk={action.risk_level.value}"
        )
        await self.db.update_message_gemini_response(message_db_id, payload, action.action.value)
        return action
