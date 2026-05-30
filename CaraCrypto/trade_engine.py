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

import asyncio
from collections.abc import Sequence as SequenceABC
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Optional, Sequence

from .alert_service import AlertService
from .config import DEFAULT_LEVERAGE, LEVERAGE_MAP, MARGIN_MODE, RiskConfig
from .database import Database
from .models import Direction, GeminiAction, OrderType, RiskLevel, RunningPosition, TradeAction
from .position_manager import PositionManager


class TradeEngine:
    SL_PLUS_BUFFER_BPS = Decimal("10")
    PARTIAL_MEDIUM_PERCENT = 30.0
    PARTIAL_LARGE_PERCENT = 60.0
    PROFIT_LOCK_R = Decimal("0.5")

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
        self._balance_max_attempts = 3
        self._balance_retry_delay_sec = 1.0
        self._symbol_filters: dict[str, dict[str, Decimal]] = {}

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

    def _floor_to_step(self, value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        units = (value / step).to_integral_value(rounding=ROUND_DOWN)
        return units * step

    @staticmethod
    def _decimal_to_str(value: Decimal) -> str:
        # Hindari scientific notation saat kirim ke Binance / assert test.
        text = format(value, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"

    def _extract_symbol_filters(self, payload: Any, pair: str) -> Optional[dict[str, Decimal]]:
        symbols = None
        if isinstance(payload, dict):
            symbols = payload.get("symbols")
        if not isinstance(symbols, SequenceABC) or isinstance(symbols, (str, bytes)):
            return None
        for symbol_info in symbols:
            if not isinstance(symbol_info, dict):
                continue
            if symbol_info.get("symbol") != pair:
                continue
            filters = symbol_info.get("filters")
            if not isinstance(filters, SequenceABC) or isinstance(filters, (str, bytes)):
                continue
            result: dict[str, Decimal] = {}
            for item in filters:
                if not isinstance(item, dict):
                    continue
                filter_type = item.get("filterType")
                if filter_type == "PRICE_FILTER":
                    tick_size = item.get("tickSize")
                    if tick_size is not None:
                        result["tick_size"] = Decimal(str(tick_size))
                if filter_type in {"LOT_SIZE", "MARKET_LOT_SIZE"}:
                    step_size = item.get("stepSize")
                    if step_size is not None:
                        result["step_size"] = Decimal(str(step_size))
            if result:
                return result
        return None

    def _get_exchange_info(self, pair: str) -> Optional[Any]:
        methods = ("exchange_info", "futures_exchange_info", "get_exchange_info")
        for method_name in methods:
            fn = getattr(self.client, method_name, None)
            if not callable(fn):
                continue
            try:
                return fn(symbol=pair)
            except TypeError:
                try:
                    return fn()
                except Exception:
                    continue
            except Exception:
                continue
        return None

    def _get_symbol_filters(self, pair: str) -> dict[str, Decimal]:
        cached = self._symbol_filters.get(pair)
        if cached is not None:
            return cached
        exchange_info = self._get_exchange_info(pair)
        filters = self._extract_symbol_filters(exchange_info, pair) or {}
        self._symbol_filters[pair] = filters
        return filters

    def _normalize_order_inputs(self, pair: str, qty: Decimal, price: Optional[Decimal] = None) -> tuple[Decimal, Optional[Decimal]]:
        filters = self._get_symbol_filters(pair)
        step_size = filters.get("step_size")
        tick_size = filters.get("tick_size")
        normalized_qty = self._floor_to_step(qty, step_size) if step_size is not None else qty
        normalized_price = self._floor_to_step(price, tick_size) if (price is not None and tick_size is not None) else price
        return normalized_qty, normalized_price

    def _resolve_entry_from_zone(self, entry_zone: Optional[Sequence[Decimal]]) -> Optional[Decimal]:
        if not entry_zone or len(entry_zone) < 2:
            return None
        low = min(entry_zone[0], entry_zone[1])
        high = max(entry_zone[0], entry_zone[1])
        return (low + high) / Decimal("2")

    def _resolve_entry_from_zone_percent(self, entry_zone: Optional[Sequence[Decimal]], ratio: Decimal) -> Optional[Decimal]:
        if not entry_zone or len(entry_zone) < 2:
            return None
        low = min(entry_zone[0], entry_zone[1])
        high = max(entry_zone[0], entry_zone[1])
        if high == low:
            return low
        clamped_ratio = min(Decimal("1"), max(Decimal("0"), ratio))
        return low + ((high - low) * clamped_ratio)

    def _compute_sl_plus_from_entry(self, direction: Direction, entry_price: Decimal) -> Decimal:
        buffer_ratio = self.SL_PLUS_BUFFER_BPS / Decimal("10000")
        if direction == Direction.LONG:
            return entry_price * (Decimal("1") + buffer_ratio)
        return entry_price * (Decimal("1") - buffer_ratio)

    async def _compute_r_multiple(self, pos: RunningPosition) -> Optional[Decimal]:
        if pos.current_sl is None:
            return None
        risk = (pos.entry_price - pos.current_sl).copy_abs()
        if risk <= 0:
            return None
        current_price = await self._get_market_reference_price(pos.pair)
        if current_price is None:
            return None
        if pos.direction == Direction.LONG:
            reward = current_price - pos.entry_price
        else:
            reward = pos.entry_price - current_price
        return reward / risk

    def _compute_profit_lock_sl(self, pos: RunningPosition, profit_lock_r: Decimal) -> Optional[Decimal]:
        if pos.current_sl is None:
            return None
        risk = (pos.entry_price - pos.current_sl).copy_abs()
        if risk <= 0:
            return None
        lock_distance = risk * max(Decimal("0"), profit_lock_r)
        if pos.direction == Direction.LONG:
            return pos.entry_price + lock_distance
        return pos.entry_price - lock_distance

    async def _place_partial_close_market_order(
        self, pos: RunningPosition, close_percentage: float
    ) -> tuple[bool, Decimal, Decimal]:
        if close_percentage <= 0:
            return True, Decimal("0"), pos.quantity
        if pos.quantity <= 0:
            return False, Decimal("0"), Decimal("0")
        close_fraction = Decimal(str(close_percentage)) / Decimal("100")
        close_qty_raw = pos.quantity * close_fraction
        normalized_qty, _ = self._normalize_order_inputs(pos.pair, close_qty_raw)
        if normalized_qty <= 0:
            return False, Decimal("0"), pos.quantity
        side = "SELL" if pos.direction == Direction.LONG else "BUY"
        self._create_futures_order(
            symbol=pos.pair,
            side=side,
            type="MARKET",
            quantity=self._decimal_to_str(normalized_qty),
            reduceOnly="true",
        )
        remaining_qty = pos.quantity - normalized_qty
        if remaining_qty < 0:
            remaining_qty = Decimal("0")
        await self.position_manager.update_quantity(pos.pair, remaining_qty)
        return True, normalized_qty, remaining_qty

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

    async def _get_account_balance(self) -> Optional[Decimal]:
        """Ambil saldo USDT (availableBalance) dari Binance Futures USDT-M.

        Mencoba sampai ``self._balance_max_attempts`` kali dengan backoff.
        Mengembalikan ``None`` kalau semua percobaan gagal; caller wajib
        memutuskan langkah lanjut (umumnya: skip eksekusi). Saat percobaan
        terakhir gagal, error terakhir dikirim ke WhatsApp via
        ``notify_error``.
        """
        candidates = (
            "balance",
            "futures_account_balance",
            "account",
            "futures_account",
        )
        last_error: Optional[str] = None
        for attempt in range(1, self._balance_max_attempts + 1):
            attempt_error: Optional[str] = None
            tried_any = False
            for method_name in candidates:
                fn = getattr(self.client, method_name, None)
                if not callable(fn):
                    continue
                tried_any = True
                try:
                    response = fn()
                except TypeError:
                    try:
                        response = fn(asset="USDT")
                    except Exception as exc:
                        attempt_error = f"{method_name}: {exc}"
                        continue
                except Exception as exc:
                    attempt_error = f"{method_name}: {exc}"
                    continue
                balance = self._extract_usdt_balance(response)
                if balance is not None:
                    return balance
                attempt_error = f"{method_name}: USDT balance not found in response"
            if not tried_any:
                attempt_error = "binance client has no balance endpoint"
            last_error = attempt_error or last_error or "unknown error fetching balance"
            if attempt < self._balance_max_attempts:
                await asyncio.sleep(self._balance_retry_delay_sec * attempt)
        await self._safe_alert(
            "notify_error",
            "trade_engine_balance",
            f"failed to fetch USDT balance after {self._balance_max_attempts} attempts: {last_error}",
        )
        return None

    def _extract_usdt_balance(self, response: Any) -> Optional[Decimal]:
        if response is None:
            return None
        if isinstance(response, dict):
            for key in ("availableBalance", "balance", "walletBalance", "totalWalletBalance", "crossWalletBalance"):
                if key in response:
                    value = self._coerce_positive_decimal(response[key])
                    if value is not None:
                        return value
            assets = response.get("assets")
            if isinstance(assets, SequenceABC) and not isinstance(assets, (str, bytes)):
                return self._extract_usdt_balance(list(assets))
        elif isinstance(response, SequenceABC) and not isinstance(response, (str, bytes)):
            for item in response:
                if not isinstance(item, dict):
                    continue
                if item.get("asset") != "USDT":
                    continue
                for key in ("availableBalance", "balance", "walletBalance", "crossWalletBalance"):
                    if key in item:
                        value = self._coerce_positive_decimal(item[key])
                        if value is not None:
                            return value
        return None

    @staticmethod
    def _coerce_positive_decimal(value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            decimal_value = Decimal(str(value))
        except Exception:
            return None
        if decimal_value <= 0:
            return None
        return decimal_value

    def _calculate_position_size(
        self, entry_price: Decimal, leverage: int, risk_level: RiskLevel, balance: Decimal
    ) -> Decimal:
        margin_pct = Decimal(str(self.risk_config.trade_margin_percent)) / Decimal("100")
        base_margin = balance * margin_pct
        if risk_level == RiskLevel.HIGH:
            base_margin *= Decimal(str(self.risk_config.high_risk_multiplier))
        qty = (base_margin * Decimal(leverage)) / entry_price
        return max(qty, Decimal("0"))

    def _extract_running_pairs_from_payload(self, payload: Any) -> set[str]:
        positions: list[dict[str, Any]] = []
        if isinstance(payload, dict):
            candidate = payload.get("positions")
            if isinstance(candidate, SequenceABC) and not isinstance(candidate, (str, bytes)):
                positions = [p for p in candidate if isinstance(p, dict)]
            elif "symbol" in payload and ("positionAmt" in payload or "position_amount" in payload):
                positions = [payload]
        elif isinstance(payload, SequenceABC) and not isinstance(payload, (str, bytes)):
            positions = [p for p in payload if isinstance(p, dict)]
        running_pairs: set[str] = set()
        for pos in positions:
            symbol = self._normalize_pair(pos.get("symbol"))
            if not symbol:
                continue
            raw_amt = pos.get("positionAmt", pos.get("position_amount", pos.get("qty")))
            try:
                amt = Decimal(str(raw_amt))
            except Exception:
                continue
            if amt.copy_abs() > 0:
                running_pairs.add(symbol)
        return running_pairs

    def _get_binance_running_pairs(self) -> Optional[set[str]]:
        methods = (
            "position_risk",
            "futures_position_information",
            "futures_account",
            "account",
        )
        last_error: Optional[str] = None
        for method_name in methods:
            fn = getattr(self.client, method_name, None)
            if not callable(fn):
                continue
            try:
                payload = fn()
            except TypeError:
                try:
                    payload = fn(recvWindow=5000)
                except Exception as exc:
                    last_error = f"{method_name}: {exc}"
                    continue
            except Exception as exc:
                last_error = f"{method_name}: {exc}"
                continue
            return self._extract_running_pairs_from_payload(payload)
        if last_error:
            return None
        pm_running = getattr(self.position_manager, "get_running_pairs", None)
        if callable(pm_running):
            try:
                return {self._normalize_pair(p) for p in pm_running() if self._normalize_pair(p)}
            except Exception:
                pass
        return set()

    def _get_binance_open_order_pairs(self) -> Optional[set[str]]:
        methods = (
            "get_orders",
            "futures_get_open_orders",
            "get_open_orders",
        )
        last_error: Optional[str] = None
        for method_name in methods:
            fn = getattr(self.client, method_name, None)
            if not callable(fn):
                continue
            try:
                payload = fn()
            except TypeError:
                try:
                    payload = fn(recvWindow=5000)
                except Exception as exc:
                    last_error = f"{method_name}: {exc}"
                    continue
            except Exception as exc:
                last_error = f"{method_name}: {exc}"
                continue
            if not isinstance(payload, SequenceABC) or isinstance(payload, (str, bytes)):
                return set()
            pairs: set[str] = set()
            for item in payload:
                if not isinstance(item, dict):
                    continue
                symbol = self._normalize_pair(item.get("symbol"))
                if symbol:
                    pairs.add(symbol)
            return pairs
        if last_error:
            return None
        return set()

    def get_exchange_context_state(self) -> dict[str, Any]:
        running_pairs = self._get_binance_running_pairs()
        open_order_pairs = self._get_binance_open_order_pairs()
        return {
            "running_pairs": sorted(running_pairs) if running_pairs is not None else None,
            "open_order_pairs": sorted(open_order_pairs) if open_order_pairs is not None else None,
        }

    async def _check_risk_limits(
        self,
        pair: str,
        entry_price: Decimal,
        risk_level: RiskLevel,
        leverage: int,
        account_balance: Optional[Decimal] = None,
    ) -> bool:
        if account_balance is None:
            account_balance = await self._get_account_balance()
            if account_balance is None:
                return False
        margin = account_balance * Decimal(str(self.risk_config.trade_margin_percent)) / Decimal("100")
        if risk_level == RiskLevel.HIGH:
            margin *= Decimal(str(self.risk_config.high_risk_multiplier))

        day_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        daily_loss = await self.db.get_daily_loss(day_start)
        daily_limit = account_balance * Decimal(str(self.risk_config.daily_loss_limit_percent)) / Decimal("100")
        if daily_loss >= daily_limit:
            await self._safe_alert("notify_risk_limit", "daily loss limit reached")
            return False

        if margin > account_balance:
            await self._safe_alert("notify_risk_limit", "insufficient balance for required margin")
            return False

        running_pairs = self._get_binance_running_pairs()
        if running_pairs is None:
            await self._safe_alert("notify_error", "trade_engine_risk", "cannot verify running positions from Binance API")
            return False

        if pair and pair in running_pairs:
            await self._safe_alert("notify_risk_limit", f"pair already running: {pair}")
            return False
        if len(running_pairs) >= self.risk_config.max_concurrent_positions:
            await self._safe_alert("notify_risk_limit", "max concurrent positions reached")
            self._queued_actions.append({"pair": pair, "queued_at": datetime.now(timezone.utc).isoformat()})
            return False
        return True

    async def _place_market_order(self, action: TradeAction, qty: Decimal) -> str:
        side = "BUY" if action.direction == Direction.LONG else "SELL"
        fallback = f"market-{action.pair}-{int(datetime.now(timezone.utc).timestamp())}"
        if action.pair:
            normalized_qty, _ = self._normalize_order_inputs(action.pair, qty)
            response = self._create_futures_order(
                symbol=action.pair,
                side=side,
                type="MARKET",
                quantity=self._decimal_to_str(normalized_qty),
            )
            return self._extract_order_id(response, fallback)
        return fallback

    async def _place_limit_order(self, action: TradeAction, qty: Decimal) -> str:
        side = "BUY" if action.direction == Direction.LONG else "SELL"
        fallback = f"limit-{action.pair}-{int(datetime.now(timezone.utc).timestamp())}"
        if action.pair and action.entry_price is not None:
            normalized_qty, normalized_price = self._normalize_order_inputs(action.pair, qty, action.entry_price)
            response = self._create_futures_order(
                symbol=action.pair,
                side=side,
                type="LIMIT",
                quantity=self._decimal_to_str(normalized_qty),
                price=self._decimal_to_str(normalized_price if normalized_price is not None else action.entry_price),
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
        order_type = OrderType.MARKET
        market_price = await self._get_market_reference_price(action.pair)
        zone_low = None
        zone_high = None
        entry_raw = None
        if action.entry_zone and len(action.entry_zone) >= 2:
            zone_low = min(action.entry_zone[0], action.entry_zone[1])
            zone_high = max(action.entry_zone[0], action.entry_zone[1])
            entry_raw = self._resolve_entry_from_zone_percent(action.entry_zone, Decimal("0.7"))

        if zone_low is not None and zone_high is not None and entry_raw is not None:
            if market_price is not None and market_price <= zone_high:
                order_type = OrderType.MARKET
                action.entry_price = market_price
            else:
                order_type = OrderType.LIMIT
                _, normalized_entry = self._normalize_order_inputs(action.pair, Decimal("1"), entry_raw)
                action.entry_price = normalized_entry if normalized_entry is not None else entry_raw
        else:
            if action.entry_price is not None:
                if market_price is not None and market_price <= action.entry_price:
                    order_type = OrderType.MARKET
                    action.entry_price = market_price
                else:
                    order_type = OrderType.LIMIT
            else:
                order_type = OrderType.MARKET
                action.entry_price = market_price

        if action.entry_price is None:
            await self._safe_alert(
                "notify_error",
                "trade_engine_new_signal",
                f"missing entry_price/entry_zone for computed {order_type.value} order pair={action.pair}",
            )
            return False
        leverage = self.get_leverage(action.pair)
        account_balance = await self._get_account_balance()
        if account_balance is None:
            return False
        if not await self._check_risk_limits(
            action.pair, action.entry_price, action.risk_level, leverage, account_balance
        ):
            return False
        await self._set_margin_mode_cross(action.pair)
        leverage = await self._set_leverage(action.pair, leverage)
        qty = self._calculate_position_size(action.entry_price, leverage, action.risk_level, account_balance)
        if qty <= 0:
            return False
        margin_used = account_balance * Decimal(str(self.risk_config.trade_margin_percent)) / Decimal("100")
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
        if order_type == OrderType.LIMIT:
            add_pending = getattr(self.position_manager, "add_pending_position", None)
            if callable(add_pending):
                await add_pending(pos)
            else:
                await self.position_manager.add_position(pos)
            if self.price_watcher:
                self.price_watcher.register_pending_order(order_id, action)
        else:
            await self.position_manager.add_position(pos)
            await self._safe_alert("notify_order_filled", action.pair, order_type.value, str(action.entry_price))
            await self._set_tp_sl_orders(pos)
        final_tp = self._get_final_tp_level(pos.direction, pos.tp_levels) if pos.tp_levels else None
        detail_qty, detail_entry = self._normalize_order_inputs(
            action.pair,
            qty,
            action.entry_price if order_type == OrderType.LIMIT else None,
        )
        _, detail_sl = self._normalize_order_inputs(action.pair, Decimal("1"), pos.current_sl) if pos.current_sl is not None else (Decimal("1"), None)
        _, detail_tp = self._normalize_order_inputs(action.pair, Decimal("1"), final_tp) if final_tp is not None else (Decimal("1"), None)
        await self._safe_alert(
            "notify_new_order_detail",
            action.pair,
            action.direction.value,
            order_type.value,
            str(detail_entry if detail_entry is not None else action.entry_price),
            str(detail_qty),
            leverage,
            f"{margin_used:.2f}",
            str(detail_sl) if detail_sl is not None else None,
            str(detail_tp) if detail_tp is not None else None,
            action.action.value,
            str([str(x) for x in action.entry_zone]) if action.entry_zone else None,
            str(entry_raw) if entry_raw is not None else None,
            str(detail_entry if detail_entry is not None else action.entry_price),
        )
        return True

    def _has_open_position_on_exchange(self, pair: str) -> bool:
        running_pairs = self._get_binance_running_pairs()
        if running_pairs is None:
            return False
        return pair in running_pairs

    async def _resolve_open_position(self, pair: str) -> Optional[RunningPosition]:
        pos = self.position_manager.get_position(pair)
        if pos:
            return pos
        if not self._has_open_position_on_exchange(pair):
            return None
        promote_pending = getattr(self.position_manager, "promote_pending_position", None)
        if callable(promote_pending):
            return await promote_pending(pair)
        return self.position_manager.get_position(pair)

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
        _, normalized_price = self._normalize_order_inputs(pair, Decimal("1"), tp_price)
        self._create_futures_order(
            symbol=pair,
            side=side,
            type="TAKE_PROFIT_MARKET",
            stopPrice=self._decimal_to_str(normalized_price if normalized_price is not None else tp_price),
            closePosition="true",
        )
        await self._safe_alert("notify_modification", pair, "set_tp", f"final_tp={tp_price}")

    async def _place_stop_market_order(self, pair: str, direction: Direction, sl_price: Decimal) -> None:
        side = "SELL" if direction == Direction.LONG else "BUY"
        _, normalized_price = self._normalize_order_inputs(pair, Decimal("1"), sl_price)
        self._create_futures_order(
            symbol=pair,
            side=side,
            type="STOP_MARKET",
            stopPrice=self._decimal_to_str(normalized_price if normalized_price is not None else sl_price),
            closePosition="true",
        )
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
        pos = await self._resolve_open_position(action.pair)
        if not pos:
            await self._safe_alert(
                "notify_error",
                "trade_engine_set_sl_breakeven",
                f"skip pair={action.pair} reason=no_open_position",
            )
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
        if not action.pair:
            return
        pos = await self._resolve_open_position(action.pair)
        if not pos:
            await self._safe_alert(
                "notify_error",
                "trade_engine_tp_partial",
                f"skip pair={action.pair} reason=no_open_position",
            )
            return
        r_multiple = await self._compute_r_multiple(pos)
        if r_multiple is None:
            await self._safe_alert(
                "notify_error",
                "trade_engine_tp_partial",
                f"skip pair={action.pair} reason=cannot_compute_rr",
            )
            return
        new_sl: Optional[Decimal] = None
        close_percentage = 0.0
        if r_multiple < Decimal("0.5"):
            close_percentage = 0.0
            new_sl = self._compute_sl_plus_from_entry(pos.direction, pos.entry_price)
        elif r_multiple < Decimal("1.5"):
            close_percentage = self.PARTIAL_MEDIUM_PERCENT
            new_sl = pos.entry_price
        else:
            close_percentage = self.PARTIAL_LARGE_PERCENT
            new_sl = self._compute_profit_lock_sl(pos, self.PROFIT_LOCK_R) or pos.entry_price

        partial_ok, closed_qty, remaining_qty = await self._place_partial_close_market_order(pos, close_percentage)
        if not partial_ok:
            await self._safe_alert(
                "notify_error",
                "trade_engine_tp_partial",
                f"skip pair={action.pair} reason=partial_close_failed",
            )
            return

        await self._update_tp_order_quantity(action.pair)
        if new_sl is not None:
            await self._cancel_existing_close_orders(action.pair, {"STOP_MARKET"})
            await self._place_stop_market_order(action.pair, pos.direction, new_sl)
            await self.position_manager.update_sl(action.pair, new_sl)
        await self._safe_alert(
            "send_alert",
            "TP_PARTIAL_EXECUTED\n"
            f"pair={action.pair}\n"
            f"rr={r_multiple:.2f}\n"
            f"partial_close={close_percentage}%\n"
            f"closed_qty={self._decimal_to_str(closed_qty)}\n"
            f"remaining_qty={self._decimal_to_str(remaining_qty)}",
        )
        await self.db.store_modification_log(
            action.pair,
            "tp_partial",
            {
                "rr_multiple": str(r_multiple),
                "close_percentage": close_percentage,
                "sl_plus_buffer_bps": str(self.SL_PLUS_BUFFER_BPS),
                "new_sl": str(new_sl) if new_sl is not None else None,
            },
            message_db_id,
        )

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
        else:
            has_pending = getattr(self.position_manager, "has_pending_position", None)
            remove_pending = getattr(self.position_manager, "remove_pending_position", None)
            if callable(has_pending) and callable(remove_pending) and has_pending(action.pair):
                await remove_pending(action.pair)
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
