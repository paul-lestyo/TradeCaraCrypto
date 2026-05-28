# Tujuan
# Engine eksekusi aksi trading ke Binance Futures.
# Caller
# __main__ setelah parser menghasilkan TradeAction.
# Dependensi
# python-binance, position_manager, alert_service.
# Main Functions
# `execute_action`, validasi order executable, dan handler per jenis aksi.
# Side Effects
# Menempatkan/cancel/close order di Binance.

from __future__ import annotations

from collections.abc import Sequence as SequenceABC
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional, Sequence

from .alert_service import AlertService
from .config import DEFAULT_LEVERAGE, LEVERAGE_MAP, MARGIN_MODE, RiskConfig
from .database import Database
from .models import Direction, GeminiAction, OrderType, RiskLevel, RunningPosition, TradeAction
from .position_manager import PositionManager


class TradeEngine:
    def __init__(
        self,
        binance_client: Any,
        db: Database,
        alert_service: AlertService,
        position_manager: PositionManager,
        risk_config: RiskConfig,
    ):
        self.client = binance_client
        self.db = db
        self.alert_service = alert_service
        self.position_manager = position_manager
        self.risk_config = risk_config
        self.price_watcher = None
        self._queued_actions = []

    async def _safe_alert(self, method: str, *args) -> None:
        fn = getattr(self.alert_service, method, None)
        if callable(fn):
            await fn(*args)

    def get_leverage(self, pair: str) -> int:
        return LEVERAGE_MAP.get(pair, DEFAULT_LEVERAGE)

    def _normalize_pair(self, pair: str) -> str:
        return str(pair or "").strip().upper().replace("/", "").replace(" ", "")

    def _create_futures_order(self, **kwargs) -> Any:
        if hasattr(self.client, "new_order"):
            return self.client.new_order(**kwargs)
        if hasattr(self.client, "futures_create_order"):
            return self.client.futures_create_order(**kwargs)
        raise AttributeError("Binance client has no futures order creation method")

    def _extract_order_id(self, response: Any, fallback: str) -> str:
        if isinstance(response, dict):
            for key in ("orderId", "clientOrderId", "origClientOrderId"):
                value = response.get(key)
                if value is not None:
                    return str(value)
        return fallback

    async def _get_market_reference_price(self, pair: str) -> Optional[Decimal]:
        methods = (
            ("mark_price", ("markPrice", "price")),
            ("futures_mark_price", ("markPrice", "price")),
            ("ticker_price", ("price",)),
            ("futures_symbol_ticker", ("price",)),
            ("get_symbol_ticker", ("price",)),
        )
        for method_name, price_keys in methods:
            fn = getattr(self.client, method_name, None)
            if not callable(fn):
                continue
            try:
                response = fn(symbol=pair)
            except Exception:
                continue
            candidates = []
            if isinstance(response, dict):
                candidates.extend(response.get(key) for key in price_keys)
            elif isinstance(response, SequenceABC) and not isinstance(response, (str, bytes)):
                for item in response:
                    if isinstance(item, dict) and item.get("symbol") == pair:
                        candidates.extend(item.get(key) for key in price_keys)
            else:
                candidates.append(response)
            for candidate in candidates:
                if candidate is None:
                    continue
                try:
                    price = Decimal(str(candidate))
                except Exception:
                    continue
                if price > 0:
                    return price
        return None

    async def _set_margin_mode_cross(self, pair: str) -> None:
        try:
            if hasattr(self.client, "change_margin_type"):
                self.client.change_margin_type(symbol=pair, marginType=MARGIN_MODE)
            elif hasattr(self.client, "futures_change_margin_type"):
                self.client.futures_change_margin_type(symbol=pair, marginType=MARGIN_MODE)
        except Exception:
            # Biasanya error kalau already CROSS; aman diabaikan.
            pass

    async def _set_leverage(self, pair: str, leverage: int) -> int:
        try:
            if hasattr(self.client, "change_leverage"):
                resp = self.client.change_leverage(symbol=pair, leverage=leverage)
            elif hasattr(self.client, "futures_change_leverage"):
                resp = self.client.futures_change_leverage(symbol=pair, leverage=leverage)
            else:
                return leverage
            return int(resp.get("leverage", leverage))
        except Exception:
            return leverage

    def _calculate_position_size(self, entry_price: Decimal, leverage: int, risk_level: RiskLevel) -> Decimal:
        # Placeholder balance 1000 USDT sampai endpoint account di-wire.
        balance = Decimal("1000")
        margin_pct = Decimal(str(self.risk_config.trade_margin_percent)) / Decimal("100")
        base_margin = balance * margin_pct
        if risk_level == RiskLevel.HIGH:
            base_margin *= Decimal(str(self.risk_config.high_risk_multiplier))
        qty = (base_margin * Decimal(leverage)) / entry_price
        return max(qty, Decimal("0"))

    async def _check_risk_limits(self, pair: str, entry_price: Decimal, risk_level: RiskLevel, leverage: int) -> bool:
        account_balance = Decimal("1000")
        margin = account_balance * Decimal(str(self.risk_config.trade_margin_percent)) / Decimal("100")
        if risk_level == RiskLevel.HIGH:
            margin *= Decimal(str(self.risk_config.high_risk_multiplier))
        position_notional = margin * Decimal(leverage)
        max_position_notional = account_balance * Decimal(str(self.risk_config.max_position_size_percent)) / Decimal("100")
        if position_notional > max_position_notional:
            await self._safe_alert("notify_risk_limit", "position size exceeds max_position_size_percent")
            return False

        day_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        daily_loss = await self.db.get_daily_loss(day_start)
        daily_limit = account_balance * Decimal(str(self.risk_config.daily_loss_limit_percent)) / Decimal("100")
        if daily_loss >= daily_limit:
            await self._safe_alert("notify_risk_limit", "daily loss limit reached")
            return False

        if margin > account_balance:
            await self._safe_alert("notify_risk_limit", "insufficient balance for required margin")
            return False

        if pair and self.position_manager.has_position(pair):
            return False
        if len(self.position_manager.get_running_pairs()) >= self.risk_config.max_concurrent_positions:
            await self._safe_alert("notify_risk_limit", "max concurrent positions reached")
            self._queued_actions.append({"pair": pair, "queued_at": datetime.now(timezone.utc).isoformat()})
            return False
        return True

    async def _place_market_order(self, action: TradeAction, qty: Decimal) -> str:
        side = "BUY" if action.direction == Direction.LONG else "SELL"
        fallback = f"market-{action.pair}-{int(datetime.now(timezone.utc).timestamp())}"
        if action.pair:
            response = self._create_futures_order(symbol=action.pair, side=side, type="MARKET", quantity=str(qty))
            return self._extract_order_id(response, fallback)
        return fallback

    async def _place_limit_order(self, action: TradeAction, qty: Decimal) -> str:
        side = "BUY" if action.direction == Direction.LONG else "SELL"
        fallback = f"limit-{action.pair}-{int(datetime.now(timezone.utc).timestamp())}"
        if action.pair and action.entry_price is not None:
            response = self._create_futures_order(
                symbol=action.pair,
                side=side,
                type="LIMIT",
                quantity=str(qty),
                price=str(action.entry_price),
                timeInForce="GTC",
            )
            return self._extract_order_id(response, fallback)
        return fallback

    async def execute_action(self, action: TradeAction, message_db_id: Optional[int] = None) -> bool:
        if action.action == GeminiAction.SKIP:
            return False
        if action.action == GeminiAction.NEW_SIGNAL:
            return await self._handle_new_signal(action, message_db_id)
        elif action.action == GeminiAction.RE_ENTRY:
            return await self._handle_re_entry(action, message_db_id)
        elif action.action == GeminiAction.UPDATE_SL:
            await self._handle_update_sl(action, message_db_id)
            return True
        elif action.action == GeminiAction.SET_SL_BREAKEVEN:
            await self._handle_set_sl_breakeven(action, message_db_id)
            return True
        elif action.action == GeminiAction.TP_PARTIAL:
            await self._handle_tp_partial(action, message_db_id)
            return True
        elif action.action == GeminiAction.CUTLOSS:
            await self._handle_cutloss(action, message_db_id)
            return True
        elif action.action == GeminiAction.CANCEL:
            await self._handle_cancel(action, message_db_id)
            return True
        elif action.action == GeminiAction.REVERSE:
            return await self._handle_reverse(action, message_db_id)
        return False

    async def _handle_new_signal(self, action: TradeAction, message_db_id: Optional[int]) -> bool:
        if not action.pair or not action.direction:
            await self._safe_alert("notify_error", "trade_engine_new_signal", "missing pair or direction")
            return False
        action.pair = self._normalize_pair(action.pair)
        if not action.pair:
            await self._safe_alert("notify_error", "trade_engine_new_signal", "empty pair after normalization")
            return False
        order_type = action.order_type or OrderType.MARKET
        if action.entry_price is None:
            if order_type == OrderType.MARKET:
                action.entry_price = await self._get_market_reference_price(action.pair)
            if action.entry_price is None:
                await self._safe_alert(
                    "notify_error",
                    "trade_engine_new_signal",
                    f"missing entry_price for {order_type.value} order pair={action.pair}",
                )
                return False
        leverage = self.get_leverage(action.pair)
        if not await self._check_risk_limits(action.pair, action.entry_price, action.risk_level, leverage):
            return False
        await self._set_margin_mode_cross(action.pair)
        leverage = await self._set_leverage(action.pair, leverage)
        qty = self._calculate_position_size(action.entry_price, leverage, action.risk_level)
        if qty <= 0:
            return False
        margin_used = Decimal("1000") * Decimal(str(self.risk_config.trade_margin_percent)) / Decimal("100")
        if action.risk_level == RiskLevel.HIGH:
            margin_used *= Decimal(str(self.risk_config.high_risk_multiplier))
        order_id = ""
        try:
            if order_type == OrderType.LIMIT:
                order_id = await self._place_limit_order(action, qty)
            else:
                order_id = await self._place_market_order(action, qty)
        except Exception as exc:
            await self._safe_alert(
                "notify_error",
                "trade_engine_order",
                f"pair={action.pair} type={order_type.value} err={exc}",
            )
            return False
        pos = RunningPosition(
            pair=action.pair,
            direction=action.direction,
            entry_price=action.entry_price,
            current_sl=action.stop_loss,
            tp_levels=action.take_profit_levels or [],
            leverage=leverage,
            order_id=order_id,
            quantity=qty,
            opened_at=datetime.now(timezone.utc),
            message_db_id=message_db_id,
        )
        await self.position_manager.add_position(pos)
        if order_type == OrderType.LIMIT:
            if self.price_watcher:
                self.price_watcher.register_pending_order(order_id, action)
        else:
            await self._safe_alert("notify_order_filled", action.pair, order_type.value, str(action.entry_price))
            await self._set_tp_sl_orders(pos)
        await self._safe_alert("notify_new_order", action.pair, action.direction.value, str(action.entry_price), order_type.value)
        final_tp = self._get_final_tp_level(pos.direction, pos.tp_levels) if pos.tp_levels else None
        await self._safe_alert(
            "notify_new_order_detail",
            action.pair,
            action.direction.value,
            order_type.value,
            str(action.entry_price),
            str(qty),
            leverage,
            f"{margin_used:.2f}",
            str(pos.current_sl) if pos.current_sl is not None else None,
            str(final_tp) if final_tp is not None else None,
            action.action.value,
        )
        return True

    async def _set_tp_sl_orders(self, pos: RunningPosition) -> None:
        final_tp: Optional[Decimal] = None
        if pos.tp_levels:
            final_tp = self._get_final_tp_level(pos.direction, pos.tp_levels)
            await self._place_take_profit_market_order(pos.pair, pos.direction, final_tp)
        if pos.current_sl is not None:
            await self._place_stop_market_order(pos.pair, pos.direction, pos.current_sl)
        await self._safe_alert(
            "notify_tp_sl_set",
            pos.pair,
            str(final_tp) if final_tp is not None else None,
            str(pos.current_sl) if pos.current_sl is not None else None,
            "engine",
        )

    def _get_open_orders(self, pair: str) -> Sequence[dict]:
        if hasattr(self.client, "futures_get_open_orders"):
            return self.client.futures_get_open_orders(symbol=pair)
        if hasattr(self.client, "get_open_orders"):
            return self.client.get_open_orders(symbol=pair)
        return []

    def _cancel_order(self, pair: str, order_id: Any) -> None:
        if hasattr(self.client, "futures_cancel_order"):
            self.client.futures_cancel_order(symbol=pair, orderId=order_id)
            return
        if hasattr(self.client, "cancel_order"):
            self.client.cancel_order(symbol=pair, orderId=order_id)

    async def _cancel_existing_close_orders(self, pair: str, order_types: set[str]) -> None:
        try:
            open_orders = self._get_open_orders(pair)
        except Exception:
            return
        for order in open_orders:
            if order.get("type") in order_types:
                try:
                    self._cancel_order(pair, order.get("orderId"))
                except Exception:
                    continue

    async def _cleanup_protection_orders(self, pair: str) -> None:
        await self._cancel_existing_close_orders(
            pair,
            {"STOP_MARKET", "TAKE_PROFIT_MARKET", "STOP", "TAKE_PROFIT"},
        )

    def _get_final_tp_level(self, direction: Direction, tp_levels):
        return max(tp_levels) if direction == Direction.LONG else min(tp_levels)

    async def _place_take_profit_market_order(self, pair: str, direction: Direction, tp_price: Decimal) -> None:
        side = "SELL" if direction == Direction.LONG else "BUY"
        self._create_futures_order(symbol=pair, side=side, type="TAKE_PROFIT_MARKET", stopPrice=str(tp_price), closePosition="true")
        await self._safe_alert("notify_modification", pair, "set_tp", f"final_tp={tp_price}")

    async def _place_stop_market_order(self, pair: str, direction: Direction, sl_price: Decimal) -> None:
        side = "SELL" if direction == Direction.LONG else "BUY"
        self._create_futures_order(symbol=pair, side=side, type="STOP_MARKET", stopPrice=str(sl_price), closePosition="true")
        await self._safe_alert("notify_modification", pair, "set_sl", f"sl={sl_price}")

    async def _handle_update_sl(self, action: TradeAction, message_db_id: Optional[int]) -> None:
        if not action.pair or action.stop_loss is None:
            return
        pos = self.position_manager.get_position(action.pair)
        if not pos:
            return
        await self._cancel_existing_close_orders(action.pair, {"STOP_MARKET"})
        await self._place_stop_market_order(action.pair, pos.direction, action.stop_loss)
        await self.position_manager.update_sl(action.pair, action.stop_loss)
        await self.db.store_modification_log(action.pair, "update_sl", {"stop_loss": str(action.stop_loss)}, message_db_id)

    async def _handle_set_sl_breakeven(self, action: TradeAction, message_db_id: Optional[int]) -> None:
        if not action.pair:
            return
        pos = self.position_manager.get_position(action.pair)
        if not pos:
            return
        await self._cancel_existing_close_orders(action.pair, {"STOP_MARKET"})
        await self._place_stop_market_order(action.pair, pos.direction, pos.entry_price)
        await self.position_manager.update_sl(action.pair, pos.entry_price)
        await self._safe_alert(
            "send_alert",
            f"TP1->SL+\npair={action.pair}\nnew_sl={pos.entry_price}",
        )
        await self.db.store_modification_log(action.pair, "set_sl_breakeven", {"stop_loss": str(pos.entry_price)}, message_db_id)

    async def _handle_tp_partial(self, action: TradeAction, message_db_id: Optional[int]) -> None:
        if not action.pair or not action.close_percentage:
            return
        await self._update_tp_order_quantity(action.pair)
        await self._safe_alert(
            "send_alert",
            f"TP1\npair={action.pair}\npartial_close={action.close_percentage}%",
        )
        await self.db.store_modification_log(action.pair, "tp_partial", {"close_percentage": action.close_percentage}, message_db_id)

    async def _update_tp_order_quantity(self, pair: str) -> None:
        pos = self.position_manager.get_position(pair)
        if not pos:
            return
        if pos.tp_levels:
            await self._place_take_profit_market_order(pair, pos.direction, self._get_final_tp_level(pos.direction, pos.tp_levels))

    async def _handle_cutloss(self, action: TradeAction, message_db_id: Optional[int]) -> None:
        if not action.pair:
            return
        await self._cleanup_protection_orders(action.pair)
        await self.position_manager.remove_position(action.pair)
        await self._safe_alert("notify_closed", action.pair, "cutloss")
        await self.db.store_modification_log(action.pair, "cutloss", {}, message_db_id)

    async def _handle_cancel(self, action: TradeAction, message_db_id: Optional[int]) -> None:
        if not action.pair:
            return
        if self.position_manager.has_position(action.pair):
            await self._cleanup_protection_orders(action.pair)
            await self.position_manager.remove_position(action.pair)
        await self._safe_alert("notify_closed", action.pair, "cancel")
        await self.db.store_modification_log(action.pair, "cancel", {}, message_db_id)

    async def _handle_reverse(self, action: TradeAction, message_db_id: Optional[int]) -> bool:
        if not action.pair:
            return False
        old = self.position_manager.get_position(action.pair)
        if old:
            await self._cleanup_protection_orders(action.pair)
            await self.position_manager.remove_position(action.pair)
            await self._safe_alert("notify_closed", action.pair, "reverse")
            new_direction = Direction.SHORT if old.direction == Direction.LONG else Direction.LONG
            reversed_action = TradeAction(
                action=GeminiAction.NEW_SIGNAL,
                pair=action.pair,
                direction=new_direction,
                entry_price=old.entry_price,
                take_profit_levels=old.tp_levels,
                stop_loss=old.current_sl,
                order_type=action.order_type,
                risk_level=action.risk_level,
            )
            return await self._handle_new_signal(reversed_action, message_db_id)
        return False

    async def _handle_re_entry(self, action: TradeAction, message_db_id: Optional[int]) -> bool:
        return await self._handle_new_signal(action, message_db_id)
