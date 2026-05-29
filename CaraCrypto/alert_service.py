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
from typing import Optional

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
        await self.send_alert(
            "ORDER DETAIL\n"
            f"pair={pair}\n"
            f"side={direction}\n"
            f"type={order_type}\n"
            f"entry={entry_price}\n"
            f"entry_zone={entry_zone if entry_zone is not None else '-'}\n"
            f"entry_raw={entry_raw if entry_raw is not None else '-'}\n"
            f"entry_final={entry_final if entry_final is not None else entry_price}\n"
            f"qty={quantity}\n"
            f"lev={leverage}x\n"
            f"margin_used~${margin_used_usd}\n"
            f"sl={stop_loss}\n"
            f"tp={take_profit}\n"
            f"reason={reason}"
        )

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
