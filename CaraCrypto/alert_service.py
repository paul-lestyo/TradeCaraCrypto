# Tujuan
# Service notifikasi WhatsApp via WuzAPI.
# Caller
# Trade engine, watcher, listener.
# Dependensi
# aiohttp.
# Main Functions
# `send_alert` dan helper notifikasi event.
# Side Effects
# HTTP POST ke endpoint WuzAPI.

from __future__ import annotations

import asyncio
from typing import Optional, Any
from decimal import Decimal

import aiohttp

from .config import AlertConfig
from .models import Direction


class AlertService:
    def __init__(self, config: AlertConfig):
        self.config = config

    async def send_alert(self, message: str) -> None:
        url = f"{self.config.base_url}/chat/send/text"
        headers = {
            "accept": "application/json",
            "token": self.config.token,
            "Content-Type": "application/json",
        }
        body = {"Phone": self.config.phone, "Body": message}
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, json=body, timeout=15) as resp:
                        if resp.status < 400:
                            return
            except Exception:
                pass
            if attempt < 2:
                await asyncio.sleep(10)

    async def notify_new_order(self, pair: str, direction: str, price: str, order_type: str) -> None:
        await self.send_alert(
            "ORDER\n"
            f"pair={pair}\n"
            f"side={direction}\n"
            f"type={order_type}\n"
            f"price={price}"
        )

    async def notify_new_order_detail(
        self,
        pair: str,
        direction: str,
        order_type: str,
        entry_price: str,
        quantity: str,
        leverage: int,
        margin_used_usd: str,
        stop_loss: Optional[str],
        take_profit: Optional[str],
        reason: str,
        entry_zone: Optional[str] = None,
        entry_raw: Optional[str] = None,
        entry_final: Optional[str] = None,
    ) -> None:
        is_long = direction.lower() == "long"
        emoji = "🟢" if is_long else "🔴"

        def to_dec(val: Any) -> Decimal:
            if val is None:
                return Decimal("0")
            try:
                cleaned = str(val).replace("$", "").strip()
                return Decimal(cleaned)
            except Exception:
                return Decimal("0")

        is_double = "/" in quantity and "/" in entry_price

        if is_double:
            q_parts = [to_dec(x) for x in quantity.split("/")]
            e_parts = [to_dec(x) for x in entry_price.split("/")]
            qty_1, qty_2 = q_parts[0], q_parts[1]
            entry_1, entry_2 = e_parts[0], e_parts[1]

            pos_1 = qty_1 * entry_1
            pos_2 = qty_2 * entry_2
            margin_1 = pos_1 / Decimal(leverage)
            margin_2 = pos_2 / Decimal(leverage)

            total_pos = pos_1 + pos_2
            total_margin = margin_1 + margin_2

            t1_str, t2_str = "limit", "limit"
            if ":" in order_type:
                try:
                    t1_str, t2_str = order_type.split(":")[1].split("/")
                except Exception:
                    pass
            t1_abbr = "MKT" if "market" in t1_str.lower() else "LMT"
            t2_abbr = "MKT" if "market" in t2_str.lower() else "LMT"

            tp_price = to_dec(take_profit)
            sl_price = to_dec(stop_loss)

            if is_long:
                pnl_tp_1 = (tp_price - entry_1) * qty_1
                pnl_tp_all = (tp_price - entry_1) * qty_1 + (tp_price - entry_2) * qty_2
                pnl_sl_all = (sl_price - entry_1) * qty_1 + (sl_price - entry_2) * qty_2
            else:
                pnl_tp_1 = (entry_1 - tp_price) * qty_1
                pnl_tp_all = (entry_1 - tp_price) * qty_1 + (entry_2 - tp_price) * qty_2
                pnl_sl_all = (entry_1 - sl_price) * qty_1 + (entry_2 - sl_price) * qty_2

            s1_sign = "+" if pnl_tp_1 >= 0 else "-"
            s2_sign = "+" if pnl_tp_all >= 0 else "-"
            s3_sign = "+" if pnl_sl_all >= 0 else "-"

            message = (
                f"{emoji} {pair} {direction.upper()} (Double Entry)\n"
                f"① {t1_abbr} @ {entry_1:.2f}\n"
                f"Pos ${pos_1:.2f} | Margin ${margin_1:.2f}\n"
                f"② {t2_abbr} @ {entry_2:.2f}\n"
                f"Pos ${pos_2:.2f} | Margin ${margin_2:.2f}\n\n"
                f"💼 Total Pos ${total_pos:.2f}\n"
                f"Margin ${total_margin:.2f} ({leverage}x)\n\n"
                f"🎯 TP {tp_price:.2f}\n"
                f"🛑 SL {sl_price:.2f}\n\n"
                f"📊 Scenarios\n"
                f"① E1 → TP {s1_sign}${abs(pnl_tp_1):.2f}\n"
                f"② E1 + E2 → TP {s2_sign}${abs(pnl_tp_all):.2f}\n"
                f"③ E1 + E2 → SL {s3_sign}${abs(pnl_sl_all):.2f}"
            )
        else:
            ot_lower = order_type.lower()
            if "market" in ot_lower:
                ot_abbr = "MKT"
            elif "limit" in ot_lower:
                ot_abbr = "LMT"
            else:
                ot_abbr = order_type.upper()

            total_qty = to_dec(quantity)
            avg_entry = to_dec(entry_final or entry_price)
            pos_val = total_qty * avg_entry

            tp_line = "🎯 TP -"
            if take_profit:
                tp_dec = to_dec(take_profit)
                if avg_entry > 0 and total_qty > 0:
                    if is_long:
                        tp_roe = ((tp_dec - avg_entry) / avg_entry) * Decimal(leverage) * 100
                        tp_pnl = (tp_dec - avg_entry) * total_qty
                    else:
                        tp_roe = ((avg_entry - tp_dec) / avg_entry) * Decimal(leverage) * 100
                        tp_pnl = (avg_entry - tp_dec) * total_qty
                    tp_roe_sign = "+" if tp_roe >= 0 else ""
                    tp_pnl_sign = "+" if tp_pnl >= 0 else "-"
                    tp_line = f"🎯 TP {tp_dec:.2f} | {tp_roe_sign}{tp_roe:.2f}% | {tp_pnl_sign}${abs(tp_pnl):.2f}"
                else:
                    tp_line = f"🎯 TP {tp_dec:.2f}"

            sl_line = "🛑 SL -"
            if stop_loss:
                sl_dec = to_dec(stop_loss)
                if avg_entry > 0 and total_qty > 0:
                    if is_long:
                        sl_roe = ((sl_dec - avg_entry) / avg_entry) * Decimal(leverage) * 100
                        sl_pnl = (sl_dec - avg_entry) * total_qty
                    else:
                        sl_roe = ((avg_entry - sl_dec) / avg_entry) * Decimal(leverage) * 100
                        sl_pnl = (avg_entry - sl_dec) * total_qty
                    sl_roe_sign = "+" if sl_roe >= 0 else ""
                    sl_pnl_sign = "+" if sl_pnl >= 0 else "-"
                    sl_line = f"🛑 SL {sl_dec:.2f} | {sl_roe_sign}{sl_roe:.2f}% | {sl_pnl_sign}${abs(sl_pnl):.2f}"
                else:
                    sl_line = f"🛑 SL {sl_dec:.2f}"

            try:
                entry_display = f"{to_dec(entry_price):.2f}"
            except Exception:
                entry_display = entry_price

            message = (
                f"{emoji} {pair} {direction.upper()}\n"
                f"{ot_abbr} @ {entry_display}\n\n"
                f"💼 Pos ${pos_val:.2f}\n"
                f"Margin ${margin_used_usd} ({leverage}x)\n\n"
                f"{tp_line}\n"
                f"{sl_line}"
            )

        await self.send_alert(message)

    async def notify_order_filled(self, pair: str, order_type: str, price: Optional[str] = None) -> None:
        suffix = f" @ {price}" if price else ""
        await self.send_alert(f"FILLED\npair={pair}\ntype={order_type}\nprice={price if price else '-'}")

    async def notify_tp_sl_set(self, pair: str, tp: Optional[str], sl: Optional[str], source: str) -> None:
        await self.send_alert(f"PROTECTION\npair={pair}\nsource={source}\ntp={tp}\nsl={sl}")

    async def notify_modification(self, pair: str, action_type: str, details: str) -> None:
        await self.send_alert(f"MODIFICATION\npair={pair}\naction={action_type}\ndetail={details}")

    async def notify_tp_hit(self, pair: str, tp_level: str) -> None:
        await self.send_alert(f"TP HIT\npair={pair}\nlevel={tp_level}")

    async def notify_sl_hit(self, pair: str, sl_level: str) -> None:
        await self.send_alert(f"SL HIT\npair={pair}\nlevel={sl_level}")

    async def notify_error(self, source: str, error: str) -> None:
        await self.send_alert(f"ERROR\nsource={source}\ndetail={error}")

    def _calculate_pnl_percent(self, entry: float, close: float, direction: Direction) -> float:
        if direction == Direction.LONG:
            return ((close - entry) / entry) * 100.0
        return ((entry - close) / entry) * 100.0

    def calculate_pnl_percent(self, entry: float, close: float, direction: Direction) -> float:
        return self._calculate_pnl_percent(entry, close, direction)

    async def notify_risk_limit(self, message: str) -> None:
        await self.send_alert(f"RISK LIMIT\ndetail={message}")

    async def notify_closed(self, pair: str, reason: str) -> None:
        await self.send_alert(f"CLOSED\npair={pair}\nreason={reason}")
