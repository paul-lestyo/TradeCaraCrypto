# Tujuan
# Monitor harga dan user data stream Binance untuk alert dan trigger TP/SL pasca fill.
# Caller
# __main__ startup dan trade_engine saat limit order terpasang.
# Dependensi
# asyncio, position_manager, trade_engine.
# Main Functions
# subscribe/unsubscribe/register_pending_order/handle_order_update.
# Side Effects
# Menjalankan loop async monitoring.

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Dict

from .alert_service import AlertService
from .models import Direction, TradeAction
from .position_manager import PositionManager


class PriceWatcher:
    def __init__(self, alert_service: AlertService, position_manager: PositionManager):
        self.alert_service = alert_service
        self.position_manager = position_manager
        self.trade_engine = None
        self._running = False
        self._subscriptions = set()
        self._pending_limit_orders: Dict[str, TradeAction] = {}

    async def _safe_alert(self, method: str, *args) -> None:
        fn = getattr(self.alert_service, method, None)
        if callable(fn):
            await fn(*args)

    async def start(self) -> None:
        self._running = True
        print("[PriceWatcher] Started")
        asyncio.create_task(self._start_user_data_stream())
        while self._running:
            await self._watch_price()
            await asyncio.sleep(1)

    async def stop(self) -> None:
        self._running = False
        print("[PriceWatcher] Stopped")

    async def subscribe(self, pair: str, action: TradeAction | None = None) -> None:
        self._subscriptions.add(pair)
        print(f"[PriceWatcher] subscribe pair={pair} active={sorted(self._subscriptions)}")
        if action:
            await self._safe_alert(
                "send_alert",
                "WATCHER START\n"
                f"pair={pair}\n"
                f"action={action.action.value}\n"
                f"side={action.direction.value if action.direction else None}\n"
                f"type={action.order_type.value if action.order_type else None}\n"
                f"entry={action.entry_price}\n"
                f"sl={action.stop_loss}\n"
                f"tp={action.take_profit_levels}\n"
                f"risk={action.risk_level.value}",
            )
        else:
            await self._safe_alert(
                "send_alert",
                f"WATCHER START\npair={pair}\nactive={sorted(self._subscriptions)}",
            )

    async def unsubscribe(self, pair: str) -> None:
        self._subscriptions.discard(pair)
        print(f"[PriceWatcher] unsubscribe pair={pair} active={sorted(self._subscriptions)}")

    def register_pending_order(self, order_id: str, action: TradeAction) -> None:
        self._pending_limit_orders[order_id] = action
        print(
            "[PriceWatcher] register_pending_order "
            f"order_id={order_id} pair={action.pair} action={action.action.value} "
            f"pending_count={len(self._pending_limit_orders)}"
        )

    async def _watch_price(self) -> None:
        for pair in list(self._subscriptions):
            pos = self.position_manager.get_position(pair)
            if not pos:
                continue
            current_price = pos.entry_price
            for tp in pos.tp_levels:
                if self._check_tp_level_reached(pos.direction, current_price, tp):
                    await self.alert_service.notify_tp_hit(pair, str(tp))
            if pos.current_sl is not None and self._check_sl_reached(pos.direction, current_price, pos.current_sl):
                await self.alert_service.notify_sl_hit(pair, str(pos.current_sl))

    def _check_tp_level_reached(self, direction: Direction, price: Decimal, tp_level: Decimal) -> bool:
        if direction == Direction.LONG:
            return price >= tp_level
        return price <= tp_level

    def _check_sl_reached(self, direction: Direction, price: Decimal, sl_level: Decimal) -> bool:
        if direction == Direction.LONG:
            return price <= sl_level
        return price >= sl_level

    async def _start_user_data_stream(self) -> None:
        print("[PriceWatcher] User data stream loop started")
        while self._running:
            await asyncio.sleep(5)

    async def _handle_order_update(self, order_id: str, order_type: str, status: str, pair: str) -> None:
        print(
            "[PriceWatcher] order_update "
            f"order_id={order_id} type={order_type} status={status} pair={pair}"
        )
        if order_id in self._pending_limit_orders and order_type == "LIMIT" and status == "FILLED":
            action = self._pending_limit_orders.pop(order_id)
            pos = self.position_manager.get_position(action.pair or "")
            if pos and self.trade_engine:
                print(f"[PriceWatcher] limit filled -> set_tp_sl_orders pair={action.pair}")
                await self._safe_alert("notify_order_filled", pair, "limit", str(pos.entry_price))
                await self.trade_engine._set_tp_sl_orders(pos)
            if pos:
                final_tp = None
                if pos.tp_levels:
                    if self.trade_engine and hasattr(self.trade_engine, "_get_final_tp_level"):
                        final_tp = self.trade_engine._get_final_tp_level(pos.direction, pos.tp_levels)
                    else:
                        final_tp = max(pos.tp_levels) if pos.direction == Direction.LONG else min(pos.tp_levels)
                await self._safe_alert(
                    "notify_tp_sl_set",
                    pair,
                    str(final_tp) if final_tp is not None else None,
                    str(pos.current_sl) if pos.current_sl is not None else None,
                    "watcher_limit_fill",
                )
        if order_type in {"TAKE_PROFIT_MARKET", "STOP_MARKET"} and status == "FILLED":
            close_type = "TP" if order_type == "TAKE_PROFIT_MARKET" else "SL"
            print(f"[PriceWatcher] protection order filled -> close_type={close_type} pair={pair}")
            await self._handle_position_closed(pair, close_type)

    async def _handle_position_closed(self, pair: str, close_type: str) -> None:
        print(f"[PriceWatcher] handle_position_closed pair={pair} close_type={close_type}")
        await self.unsubscribe(pair)
        if self.trade_engine and hasattr(self.trade_engine, "_cleanup_protection_orders"):
            try:
                await self.trade_engine._cleanup_protection_orders(pair)
                print(f"[PriceWatcher] cleanup_protection_orders done pair={pair}")
            except Exception:
                pass
        await self.position_manager.remove_position(pair)
        await self._safe_alert("notify_closed", pair, close_type)

    async def handle_order_update(self, order_id: str, order_type: str, status: str, pair: str) -> None:
        await self._handle_order_update(order_id, order_type, status, pair)
