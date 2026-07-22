# Tujuan
# Test dispatch dasar dan helper trade engine.
# Caller
# pytest.
# Dependensi
# CaraCrypto.trade_engine, CaraCrypto.models.
# Main Functions
# Validasi property task 11.6, seleksi entry/margin/proteksi, adapter Binance, dan recovery plan.
# Side Effects
# Tidak ada.

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from CaraCrypto.models import Direction, GeminiAction, OrderType, RiskLevel, RunningPosition, TradeAction
from CaraCrypto.trade_engine import TradeEngine


class _Client:
    def __init__(self):
        self.orders = []
        self.canceled = []
        self.algo_orders = []
        self.canceled_algo = []
        self.positions = []

    def change_margin_type(self, **_):
        return {}

    def change_leverage(self, **kwargs):
        return {"leverage": kwargs.get("leverage", 50)}

    def new_order(self, **kwargs):
        self.orders.append(kwargs)
        return {"orderId": 1000 + len(self.orders)}

    def futures_get_open_orders(self, **_):
        return [{"orderId": 10, "type": "STOP_MARKET"}, {"orderId": 11, "type": "TAKE_PROFIT_MARKET"}]

    def futures_cancel_order(self, **kwargs):
        self.canceled.append(kwargs)
        return {"status": "CANCELED"}

    def futures_get_open_algo_orders(self, **_):
        return self.algo_orders

    def futures_cancel_algo_order(self, **kwargs):
        self.canceled_algo.append(kwargs)
        return {"status": "CANCELED"}

    def balance(self, **_):
        return [{"asset": "USDT", "availableBalance": "1000"}]

    def futures_exchange_info(self, **_):
        return {"symbols": []}

    def mark_price(self, **_):
        return {"markPrice": "100"}

    def futures_leverage_bracket(self, **_):
        return []

    def position_risk(self, **_):
        return self.positions


class _PythonBinanceClient:
    def __init__(self):
        self.orders = []
        self.canceled = []
        self.margin_calls = []
        self.leverage_calls = []

    def futures_change_margin_type(self, **kwargs):
        self.margin_calls.append(kwargs)
        return {}

    def futures_change_leverage(self, **kwargs):
        self.leverage_calls.append(kwargs)
        return {"leverage": kwargs.get("leverage", 50)}

    def futures_create_order(self, **kwargs):
        self.orders.append(kwargs)
        return {"orderId": 2000 + len(self.orders)}

    def futures_get_open_orders(self, **_):
        return []

    def futures_cancel_order(self, **kwargs):
        self.canceled.append(kwargs)
        return {"status": "CANCELED"}

    def futures_account_balance(self, **_):
        return [{"asset": "USDT", "availableBalance": "1000"}]


class _DB:
    def __init__(self):
        self.logs = []
        self.trade_plans = {}

    async def store_modification_log(self, pair, action_type, details, message_id):
        self.logs.append((pair, action_type, details, message_id))

    async def get_daily_loss(self, *_):
        return Decimal("0")

    async def get_latest_trade_plan_message(self, pair):
        return self.trade_plans.get(pair)


class _Alert:
    def __init__(self):
        self.errors = []
        self.modifications = []
        self.order_details = []
        self.protections = []
        self.closed = []
        self.sent_alerts = []

    async def send_alert(self, msg: str):
        self.sent_alerts.append(msg)

    async def notify_new_order(self, *_):
        return None

    async def notify_new_order_detail(self, *args):
        self.order_details.append(args)

    async def notify_order_filled(self, *_):
        return None

    async def notify_tp_sl_set(self, *args):
        self.protections.append(args)

    async def notify_modification(self, *args):
        self.modifications.append(args)

    async def notify_risk_limit(self, *_):
        return None

    async def notify_error(self, *args):
        self.errors.append(args)

    async def notify_closed(self, *args):
        self.closed.append(args)


class _PM:
    def __init__(self):
        self.m = {}
        self.pending = {}

    async def add_position(self, p):
        self.m[p.pair] = p

    async def add_pending_position(self, p):
        if p.pair in self.m:
            p.pair = f"{p.pair}_{p.direction.value.upper()}"
        self.pending[p.pair] = p

    async def remove_position(self, pair):
        self.m.pop(pair, None)
        self.pending.pop(pair, None)

    async def remove_pending_position(self, pair):
        self.pending.pop(pair, None)
        for suffix in ["_SHORT", "_LONG"]:
            self.pending.pop(f"{pair}{suffix}", None)

    async def update_sl(self, pair, sl):
        if pair in self.m:
            self.m[pair].current_sl = sl

    async def update_quantity(self, pair, qty):
        if pair in self.m:
            self.m[pair].quantity = qty

    def has_position(self, pair):
        return pair in self.m

    def get_position(self, pair):
        return self.m.get(pair)

    def get_pending_position(self, pair):
        if pair in self.pending:
            return self.pending[pair]
        for suffix in ["_SHORT", "_LONG"]:
            if f"{pair}{suffix}" in self.pending:
                return self.pending[f"{pair}{suffix}"]
        return None

    def has_pending_position(self, pair):
        if pair in self.pending:
            return True
        for suffix in ["_SHORT", "_LONG"]:
            if f"{pair}{suffix}" in self.pending:
                return True
        return False

    async def promote_pending_position(self, pair):
        pos = self.pending.pop(pair, None)
        if pos:
            pos.pair = pair.split("_")[0]
            self.m[pos.pair] = pos
        return pos

    def get_running_pairs(self):
        return list(self.m.keys())


def _engine(client=None):
    risk = SimpleNamespace(
        trade_margin_percent=1.0,
        high_risk_multiplier=0.5,
        max_concurrent_positions=5,
        daily_loss_limit_percent=5.0,
    )
    return TradeEngine(client or _Client(), _DB(), _Alert(), _PM(), risk)


def test_fixed_leverage_property():
    e = _engine()
    assert e.get_leverage("BTCUSDT") == 75
    assert e.get_leverage("ETHUSDT") == 50
    assert e.get_leverage("XRPUSDT") == 20


def test_final_tp_level_property():
    e = _engine()
    assert e._get_final_tp_level(Direction.LONG, [Decimal("10"), Decimal("12")]) == Decimal("12")
    assert e._get_final_tp_level(Direction.SHORT, [Decimal("10"), Decimal("12")]) == Decimal("10")


def test_usdt_balance_prefers_available_over_wallet_property():
    e = _engine()
    balance = e._extract_usdt_balance(
        [{"asset": "USDT", "availableBalance": "102.3", "balance": "200"}]
    )
    assert balance == Decimal("102.3")


def test_python_binance_order_adapter_property():
    client = _PythonBinanceClient()
    e = _engine(client)
    response = e._create_futures_order(symbol="BTCUSDT", side="BUY", type="MARKET", quantity="0.1")
    assert response["orderId"] == 2001
    assert client.orders[0]["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_python_binance_margin_and_leverage_adapter_property():
    client = _PythonBinanceClient()
    e = _engine(client)
    await e._set_margin_mode_cross("BTCUSDT")
    leverage = await e._set_leverage("BTCUSDT", 125)
    assert client.margin_calls[0]["symbol"] == "BTCUSDT"
    assert client.margin_calls[0]["marginType"] == "CROSSED"
    assert client.leverage_calls[0]["leverage"] == 125
    assert leverage == 125


@pytest.mark.asyncio
async def test_set_leverage_clamps_to_pair_max_from_brackets_property():
    class _ClientWithMaxLeverage(_Client):
        def futures_leverage_bracket(self, **kwargs):
            if kwargs.get("symbol") == "DUSDT":
                return [{"symbol": "DUSDT", "brackets": [{"initialLeverage": 20}]}]
            return []

    e = _engine(_ClientWithMaxLeverage())
    leverage = await e._set_leverage("DUSDT", 50)
    assert leverage == 20


@pytest.mark.asyncio
async def test_reverse_action_property():
    e = _engine()
    await e.position_manager.add_position(
        RunningPosition("BTCUSDT", Direction.LONG, Decimal("100"), Decimal("90"), [Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow())
    )
    await e.execute_action(TradeAction(action=GeminiAction.REVERSE, pair="BTCUSDT", order_type=OrderType.MARKET, risk_level=RiskLevel.NORMAL))
    pos = e.position_manager.get_position("BTCUSDT")
    assert pos is not None
    assert pos.direction == Direction.SHORT


@pytest.mark.asyncio
async def test_sl_breakeven_replaces_old_sl_order():
    e = _engine()
    e.client.positions = [{"symbol": "BTCUSDT", "positionAmt": "0.1", "entryPrice": "100"}]
    await e.position_manager.add_position(
        RunningPosition("BTCUSDT", Direction.LONG, Decimal("100"), Decimal("90"), [Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow())
    )
    await e.execute_action(TradeAction(action=GeminiAction.SET_SL_BREAKEVEN, pair="BTCUSDT", risk_level=RiskLevel.NORMAL))
    # old STOP_MARKET cancelled
    assert any(c.get("orderId") == 10 for c in e.client.canceled)
    # new STOP_MARKET created at entry price
    stop_orders = [o for o in e.client.orders if o.get("type") == "STOP_MARKET"]
    assert stop_orders
    assert stop_orders[-1].get("stopPrice") == "100"


@pytest.mark.asyncio
async def test_cancel_running_position_places_reduce_only_market_close_property():
    e = _engine()
    e.client.positions = [
        {
            "symbol": "PLUMEUSDT",
            "positionAmt": "100",
            "entryPrice": "0.1",
            "leverage": "50",
        }
    ]
    await e.position_manager.add_position(
        RunningPosition("PLUMEUSDT", Direction.LONG, Decimal("0.1"), Decimal("0.09"), [Decimal("0.12")], 50, "1", Decimal("100"), datetime.utcnow())
    )
    await e.execute_action(TradeAction(action=GeminiAction.CANCEL, pair="PLUMEUSDT", risk_level=RiskLevel.NORMAL), message_db_id=3)
    close_orders = [o for o in e.client.orders if o.get("type") == "MARKET" and o.get("reduceOnly") == "true"]
    assert close_orders
    assert close_orders[-1]["symbol"] == "PLUMEUSDT"
    assert close_orders[-1]["side"] == "SELL"
    assert close_orders[-1]["quantity"] == "100"
    assert not e.position_manager.has_position("PLUMEUSDT")
    assert e.alert_service.closed == [("PLUMEUSDT", "cancel")]


@pytest.mark.asyncio
async def test_cancel_recovers_exchange_position_before_market_close_property():
    e = _engine()
    e.client.positions = [
        {
            "symbol": "PLUMEUSDT",
            "positionAmt": "250",
            "entryPrice": "0.1",
            "leverage": "50",
        }
    ]
    await e.execute_action(TradeAction(action=GeminiAction.CANCEL, pair="PLUMEUSDT", risk_level=RiskLevel.NORMAL), message_db_id=3)
    close_orders = [o for o in e.client.orders if o.get("type") == "MARKET" and o.get("reduceOnly") == "true"]
    assert close_orders[-1]["symbol"] == "PLUMEUSDT"
    assert close_orders[-1]["side"] == "SELL"
    assert close_orders[-1]["quantity"] == "250"
    assert e.alert_service.closed == [("PLUMEUSDT", "cancel")]


@pytest.mark.asyncio
async def test_cancel_without_position_does_not_send_closed_property():
    e = _engine()
    await e.execute_action(TradeAction(action=GeminiAction.CANCEL, pair="PLUMEUSDT", risk_level=RiskLevel.NORMAL), message_db_id=3)
    assert e.client.orders == []
    assert e.alert_service.closed == []
    assert e.alert_service.errors == [
        ("trade_engine_cancel", "skip pair=PLUMEUSDT reason=no_open_position")
    ]


@pytest.mark.asyncio
async def test_tp_partial_sets_sl_plus_buffer_property():
    e = _engine()
    e.client.positions = [{"symbol": "BTCUSDT", "positionAmt": "0.1", "entryPrice": "100"}]
    await e.position_manager.add_position(
        RunningPosition("BTCUSDT", Direction.LONG, Decimal("100"), Decimal("90"), [Decimal("95"), Decimal("99")], 50, "1", Decimal("0.1"), datetime.utcnow())
    )
    await e.execute_action(TradeAction(action=GeminiAction.TP_PARTIAL, pair="BTCUSDT", risk_level=RiskLevel.NORMAL))
    stop_orders = [o for o in e.client.orders if o.get("type") == "STOP_MARKET"]
    assert stop_orders
    assert stop_orders[-1].get("stopPrice") == "95"


@pytest.mark.asyncio
async def test_tp_partial_recovers_exchange_position_after_restart_property():
    e = _engine()
    e.client.positions = [
        {
            "symbol": "FIGHTUSDT",
            "positionAmt": "1000",
            "entryPrice": "0.004044",
            "leverage": "50",
        }
    ]
    await e.execute_action(
        TradeAction(
            action=GeminiAction.TP_PARTIAL,
            pair="FIGHTUSDT",
            direction=Direction.LONG,
            entry_price=Decimal("0.004044"),
            take_profit_levels=[Decimal("0.00416"), Decimal("0.00435")],
            stop_loss=Decimal("0.003777"),
            risk_level=RiskLevel.HIGH,
        ),
        message_db_id=9,
    )
    pos = e.position_manager.get_position("FIGHTUSDT")
    assert pos is not None
    assert pos.quantity == Decimal("300.0")
    assert not e.alert_service.errors
    partial_orders = [o for o in e.client.orders if o.get("reduceOnly") == "true"]
    assert partial_orders
    assert partial_orders[-1]["symbol"] == "FIGHTUSDT"


@pytest.mark.asyncio
async def test_startup_reconcile_removes_db_position_missing_on_exchange_property():
    e = _engine()
    await e.position_manager.add_position(
        RunningPosition("FIGHTUSDT", Direction.LONG, Decimal("100"), Decimal("90"), [Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow())
    )
    result = await e.reconcile_positions_with_exchange()
    assert result["removed"] == ["FIGHTUSDT"]
    assert not e.position_manager.has_position("FIGHTUSDT")
    assert ("FIGHTUSDT", "startup_reconcile_closed", {"source": "binance_absent"}, None) in e.db.logs


@pytest.mark.asyncio
async def test_startup_reconcile_recovers_exchange_position_missing_in_db_property():
    e = _engine()
    e.client.positions = [
        {
            "symbol": "FIGHTUSDT",
            "positionAmt": "1000",
            "entryPrice": "0.004044",
            "leverage": "50",
        }
    ]
    result = await e.reconcile_positions_with_exchange()
    pos = e.position_manager.get_position("FIGHTUSDT")
    assert result["recovered"] == ["FIGHTUSDT"]
    assert pos is not None
    assert pos.quantity == Decimal("1000")
    assert pos.entry_price == Decimal("0.004044")


@pytest.mark.asyncio
async def test_startup_reconcile_recovers_trade_plan_from_message_json_property():
    e = _engine()
    e.client.positions = [
        {
            "symbol": "FIGHTUSDT",
            "positionAmt": "1000",
            "entryPrice": "0.004044",
            "leverage": "50",
        }
    ]
    e.db.trade_plans["FIGHTUSDT"] = {
        "id": 42,
        "extracted_data": {
            "action": "new_signal",
            "pair": "FIGHTUSDT",
            "direction": "long",
            "take_profit_levels": ["0.0045", "0.0050", "0.0039"],
            "stop_loss": "0.0038",
        },
    }
    result = await e.reconcile_positions_with_exchange()
    pos = e.position_manager.get_position("FIGHTUSDT")
    assert result["recovered"] == ["FIGHTUSDT"]
    assert pos is not None
    assert pos.message_db_id == 42
    assert pos.current_sl == Decimal("0.0038")
    assert pos.tp_levels == [Decimal("0.0045"), Decimal("0.0050")]
    assert e.db.logs[-1][3] == 42


@pytest.mark.asyncio
async def test_order_without_entry_uses_market_reference_property():
    e = _engine()
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="PROMPTUSDT",
            direction=Direction.LONG,
            order_type=OrderType.LIMIT,
            stop_loss=Decimal("90"),
            take_profit_levels=[Decimal("110")],
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["type"] == "MARKET"
    assert e.position_manager.has_position("PROMPTUSDT")


@pytest.mark.asyncio
async def test_margin_used_detail_uses_available_balance_and_effective_qty_property():
    class _ClientWithWalletBalance(_Client):
        def balance(self, **_):
            return [{"asset": "USDT", "availableBalance": "102", "balance": "200"}]

    e = _engine(_ClientWithWalletBalance())
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="PROMPTUSDT",
            direction=Direction.LONG,
            stop_loss=Decimal("90"),
            take_profit_levels=[Decimal("110")],
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert e.client.orders[0]["quantity"] == "0.102"
    assert e.alert_service.order_details[-1][6] == "0.51"


@pytest.mark.asyncio
async def test_limit_order_normalizes_pair_and_stores_real_order_id_property():
    e = _engine()
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="JELLY/USDT",
            direction=Direction.LONG,
            order_type=OrderType.LIMIT,
            entry_price=Decimal("0.01"),
            stop_loss=Decimal("0.009"),
            take_profit_levels=[Decimal("0.012")],
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["symbol"] == "JELLYUSDT"
    assert e.position_manager.get_pending_position("JELLYUSDT").order_id == "1001"


@pytest.mark.asyncio
async def test_limit_order_normalizes_price_and_quantity_by_symbol_filters_property():
    class _ClientWithFilters(_Client):
        def futures_exchange_info(self, **_):
            return {
                "symbols": [
                    {
                        "symbol": "BRETTUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                        ],
                    }
                ]
            }

    e = _engine(_ClientWithFilters())
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="BRETTUSDT",
            direction=Direction.LONG,
            order_type=OrderType.LIMIT,
            entry_price=Decimal("0.123456"),
            stop_loss=Decimal("0.1"),
            take_profit_levels=[Decimal("0.15")],
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["quantity"] == "426"
    assert e.client.orders[0]["price"] == "0.1234"


@pytest.mark.asyncio
async def test_short_entry_zone_limit_when_market_below_cheapest_area_property():
    class _ClientWithFilters(_Client):
        def futures_exchange_info(self, **_):
            return {
                "symbols": [
                    {
                        "symbol": "BRETTUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                        ],
                    }
                ]
            }

        def mark_price(self, **_):
            return {"markPrice": "0.005"}

    e = _engine(_ClientWithFilters())
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="BRETTUSDT",
            direction=Direction.SHORT,
            order_type=OrderType.LIMIT,
            entry_zone=[Decimal("0.0060"), Decimal("0.0080")],
            stop_loss=Decimal("0.0090"),
            take_profit_levels=[Decimal("0.0050")],
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["type"] == "LIMIT"
    assert e.client.orders[0]["price"] == "0.006"
    assert e.client.orders[1]["type"] == "LIMIT"
    assert e.client.orders[1]["price"] == "0.008"
    assert e.position_manager.get_pending_position("BRETTUSDT").entry_price == Decimal("0.0060")


@pytest.mark.asyncio
async def test_long_entry_zone_limit_when_market_above_cheapest_area_property():
    class _ClientWithFilters(_Client):
        def futures_exchange_info(self, **_):
            return {
                "symbols": [
                    {
                        "symbol": "BRETTUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                        ],
                    }
                ]
            }

        def mark_price(self, **_):
            return {"markPrice": "0.009"}

    e = _engine(_ClientWithFilters())
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="BRETTUSDT",
            direction=Direction.LONG,
            order_type=OrderType.LIMIT,
            entry_zone=[Decimal("0.0060"), Decimal("0.0080")],
            stop_loss=Decimal("0.0050"),
            take_profit_levels=[Decimal("0.0100")],
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["type"] == "LIMIT"
    assert e.client.orders[0]["price"] == "0.008"
    assert e.client.orders[1]["type"] == "LIMIT"
    assert e.client.orders[1]["price"] == "0.006"
    assert e.position_manager.get_pending_position("BRETTUSDT").entry_price == Decimal("0.0080")


@pytest.mark.asyncio
async def test_entry_zone_market_when_price_inside_or_below_area_property():
    class _ClientWithFilters(_Client):
        def futures_exchange_info(self, **_):
            return {
                "symbols": [
                    {
                        "symbol": "BRETTUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                        ],
                    }
                ]
            }

        def mark_price(self, **_):
            return {"markPrice": "0.0075"}

    e = _engine(_ClientWithFilters())
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="BRETTUSDT",
            direction=Direction.LONG,
            entry_zone=[Decimal("0.0060"), Decimal("0.0080")],
            stop_loss=Decimal("0.0050"),
            take_profit_levels=[Decimal("0.0100")],
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["type"] == "MARKET"
    assert e.client.orders[1]["type"] == "LIMIT"
    assert e.client.orders[1]["price"] == "0.006"
    assert e.position_manager.get_position("BRETTUSDT") is not None


@pytest.mark.asyncio
async def test_short_entry_zone_market_when_price_inside_or_above_area_property():
    class _ClientWithFilters(_Client):
        def futures_exchange_info(self, **_):
            return {
                "symbols": [
                    {
                        "symbol": "BRETTUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                        ],
                    }
                ]
            }

        def mark_price(self, **_):
            return {"markPrice": "0.0075"}

    e = _engine(_ClientWithFilters())
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="BRETTUSDT",
            direction=Direction.SHORT,
            entry_zone=[Decimal("0.0060"), Decimal("0.0080")],
            stop_loss=Decimal("0.0090"),
            take_profit_levels=[Decimal("0.0050")],
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["type"] == "MARKET"
    assert e.client.orders[1]["type"] == "LIMIT"
    assert e.client.orders[1]["price"] == "0.008"
    assert e.position_manager.get_position("BRETTUSDT") is not None


@pytest.mark.asyncio
async def test_order_type_market_override_forces_market_even_when_zone_would_limit_property():
    class _ClientWithFilters(_Client):
        def futures_exchange_info(self, **_):
            return {
                "symbols": [
                    {
                        "symbol": "BRETTUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                        ],
                    }
                ]
            }

        def mark_price(self, **_):
            return {"markPrice": "0.005"}

    e = _engine(_ClientWithFilters())
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="BRETTUSDT",
            direction=Direction.SHORT,
            order_type=OrderType.MARKET,
            entry_zone=[Decimal("0.0060"), Decimal("0.0080")],
            stop_loss=Decimal("0.0090"),
            take_profit_levels=[Decimal("0.0050")],
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["type"] == "MARKET"


@pytest.mark.asyncio
async def test_initial_tp_sl_logs_without_modification_whatsapp_property():
    e = _engine()
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="PROMPTUSDT",
            direction=Direction.LONG,
            take_profit_levels=[Decimal("110")],
            stop_loss=Decimal("90"),
            risk_level=RiskLevel.NORMAL,
        ),
        message_db_id=42,
    )
    assert accepted is True
    assert e.alert_service.modifications == []
    assert e.alert_service.protections == [("PROMPTUSDT", "110", "90", "engine")]
    assert ("PROMPTUSDT", "set_tp", {"final_tp": "110"}, 42) in e.db.logs
    assert ("PROMPTUSDT", "set_sl", {"stop_loss": "90"}, 42) in e.db.logs


@pytest.mark.asyncio
async def test_set_tp_sl_cleans_existing_algo_protection_orders_property():
    e = _engine()
    e.client.algo_orders = [
        {"algoId": 88, "type": "TAKE_PROFIT_MARKET"},
        {"clientAlgoId": "sl-client-1", "origType": "STOP_MARKET"},
    ]
    pos = RunningPosition(
        "PROMPTUSDT",
        Direction.LONG,
        Decimal("100"),
        Decimal("90"),
        [Decimal("110")],
        50,
        "entry-1",
        Decimal("0.2"),
        datetime.utcnow(),
        message_db_id=42,
    )
    await e._set_tp_sl_orders(pos)
    assert {"symbol": "PROMPTUSDT", "algoId": 88} in e.client.canceled_algo
    assert {"symbol": "PROMPTUSDT", "clientAlgoId": "sl-client-1"} in e.client.canceled_algo
    assert [o["type"] for o in e.client.orders[-2:]] == ["TAKE_PROFIT_MARKET", "STOP_MARKET"]


@pytest.mark.asyncio
async def test_strict_sl_tp_existence_guard_property():
    e = _engine()
    # Missing SL
    accepted_no_sl = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="PROMPTUSDT",
            direction=Direction.LONG,
            take_profit_levels=[Decimal("110")],
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted_no_sl is False
    assert "trade plan no SL TP" in e.alert_service.sent_alerts

    # Missing TP
    e.alert_service.sent_alerts.clear()
    accepted_no_tp = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="PROMPTUSDT",
            direction=Direction.LONG,
            stop_loss=Decimal("90"),
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted_no_tp is False
    assert "trade plan no SL TP" in e.alert_service.sent_alerts


@pytest.mark.asyncio
async def test_double_entry_size_and_routing_property():
    class _ClientWithFilters(_Client):
        def futures_exchange_info(self, **_):
            return {
                "symbols": [
                    {
                        "symbol": "BRETTUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                        ],
                    }
                ]
            }

        def mark_price(self, **_):
            return {"markPrice": "0.0075"}

    e = _engine(_ClientWithFilters())
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="BRETTUSDT",
            direction=Direction.LONG,
            entry_zone=[Decimal("0.0060"), Decimal("0.0080")],
            stop_loss=Decimal("0.0050"),
            take_profit_levels=[Decimal("0.0100")],
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    # Entry 1 risk: 20% of 10 = 2. Qty = 2 / 0.003 = 666.66 -> 666
    # Entry 2 risk: 80% of 10 = 8. Qty = 8 / 0.001 = 8000 -> 8000
    # Mark price = 0.0075.
    # Entry 1 (0.0080): market_price (0.0075) <= entry_1_price (0.0080) -> MARKET
    # Entry 2 (0.0060): market_price (0.0075) <= entry_2_price (0.0060) -> LIMIT
    
    assert len(e.client.orders) == 4
    assert e.client.orders[0]["type"] == "MARKET"
    assert e.client.orders[0]["quantity"] == "666"
    assert e.client.orders[1]["type"] == "LIMIT"
    assert e.client.orders[1]["price"] == "0.006"
    assert e.client.orders[1]["quantity"] == "8000"
    assert e.client.orders[2]["type"] == "TAKE_PROFIT_MARKET"
    assert e.client.orders[3]["type"] == "STOP_MARKET"


@pytest.mark.asyncio
async def test_tp_partial_double_entry_flow_entry2_filled_tp1_property():
    e = _engine()
    e.client.positions = [{"symbol": "BTCUSDT", "positionAmt": "0.5", "entryPrice": "92"}]
    e.db.trade_plans["BTCUSDT"] = {
        "id": 1,
        "extracted_data": {
            "entry_zone": ["90", "100"],
            "direction": "LONG",
        }
    }
    def mock_get_open_orders(symbol=None):
        return []
    e.client.futures_get_open_orders = mock_get_open_orders

    pos = RunningPosition(
        "BTCUSDT", Direction.LONG, Decimal("92"), Decimal("85"), [Decimal("100"), Decimal("110")], 50, "1", Decimal("0.5"), datetime.utcnow()
    )
    await e.position_manager.add_position(pos)

    def mock_mark_price(symbol=None):
        return {"markPrice": "100"}
    e.client.mark_price = mock_mark_price

    await e.execute_action(TradeAction(action=GeminiAction.TP_PARTIAL, pair="BTCUSDT", risk_level=RiskLevel.NORMAL))
    
    market_orders = [o for o in e.client.orders if o.get("type") == "MARKET" and o.get("reduceOnly") == "true"]
    assert market_orders
    assert market_orders[-1].get("quantity") == "0.35"
    
    stop_orders = [o for o in e.client.orders if o.get("type") == "STOP_MARKET"]
    assert stop_orders
    assert stop_orders[-1].get("stopPrice") == "92.092"
    assert pos.last_tp_partial_index_applied == 0


@pytest.mark.asyncio
async def test_tp_partial_double_entry_flow_entry2_filled_tp2_property():
    e = _engine()
    e.client.positions = [{"symbol": "BTCUSDT", "positionAmt": "0.25", "entryPrice": "92"}]
    e.db.trade_plans["BTCUSDT"] = {
        "id": 1,
        "extracted_data": {
            "entry_zone": ["90", "100"],
            "direction": "LONG",
        }
    }
    def mock_get_open_orders(symbol=None):
        return []
    e.client.futures_get_open_orders = mock_get_open_orders

    pos = RunningPosition(
        "BTCUSDT", Direction.LONG, Decimal("92"), Decimal("92.092"), [Decimal("100"), Decimal("110")], 50, "1", Decimal("0.25"), datetime.utcnow(),
        last_tp_partial_index_applied=0
    )
    await e.position_manager.add_position(pos)

    def mock_mark_price(symbol=None):
        return {"markPrice": "110"}
    e.client.mark_price = mock_mark_price

    await e.execute_action(TradeAction(action=GeminiAction.TP_PARTIAL, pair="BTCUSDT", risk_level=RiskLevel.NORMAL))
    
    market_orders = [o for o in e.client.orders if o.get("type") == "MARKET" and o.get("reduceOnly") == "true"]
    assert market_orders
    assert market_orders[-1].get("quantity") == "0.175"
    
    stop_orders = [o for o in e.client.orders if o.get("type") == "STOP_MARKET"]
    assert stop_orders
    assert stop_orders[-1].get("stopPrice") == "100"
    assert pos.last_tp_partial_index_applied == 1


@pytest.mark.asyncio
async def test_tp_partial_double_entry_flow_entry2_not_filled_tp1_property():
    e = _engine()
    e.client.positions = [{"symbol": "BTCUSDT", "positionAmt": "0.1", "entryPrice": "100"}]
    e.db.trade_plans["BTCUSDT"] = {
        "id": 1,
        "extracted_data": {
            "entry_zone": ["90", "100"],
            "direction": "LONG",
        }
    }
    def mock_get_open_orders(symbol=None):
        return [{"orderId": 999, "type": "LIMIT", "price": "90"}]
    e.client.futures_get_open_orders = mock_get_open_orders

    pos = RunningPosition(
        "BTCUSDT", Direction.LONG, Decimal("100"), Decimal("85"), [Decimal("100"), Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow()
    )
    await e.position_manager.add_position(pos)

    def mock_mark_price(symbol=None):
        return {"markPrice": "100"}
    e.client.mark_price = mock_mark_price

    await e.execute_action(TradeAction(action=GeminiAction.TP_PARTIAL, pair="BTCUSDT", risk_level=RiskLevel.NORMAL))
    
    market_orders = [o for o in e.client.orders if o.get("type") == "MARKET" and o.get("reduceOnly") == "true"]
    assert not market_orders
    assert pos.last_tp_partial_index_applied == -1


@pytest.mark.asyncio
async def test_tp2_tolerance_resolution_property():
    e = _engine()
    e.risk_config.tp_tolerance_percent = 2.0
    pos = RunningPosition(
        "BTCUSDT", Direction.LONG, Decimal("100"), Decimal("85"), [Decimal("110"), Decimal("120")], 50, "1", Decimal("0.1"), datetime.utcnow()
    )
    # TP1 (index 0) has 0% tolerance
    assert e._resolve_reached_tp_index(pos, Decimal("108")) == -1
    assert e._resolve_reached_tp_index(pos, Decimal("110")) == 0

    # TP2 (index 1) has 2% tolerance (threshold: 120 * 0.98 = 117.6)
    assert e._resolve_reached_tp_index(pos, Decimal("117")) == 0
    assert e._resolve_reached_tp_index(pos, Decimal("118")) == 1
    assert e._resolve_reached_tp_index(pos, Decimal("120")) == 1


@pytest.mark.asyncio
async def test_opposite_direction_risk_exempt_and_tp_adjust_property():
    e = _engine()
    active_pos = RunningPosition(
        "TUSDT", Direction.LONG, Decimal("100"), Decimal("85"), [Decimal("110"), Decimal("120")], 50, "1", Decimal("0.1"), datetime.utcnow()
    )
    await e.position_manager.add_position(active_pos)
    e.client.positions = [{"symbol": "TUSDT", "positionAmt": "0.1", "entryPrice": "100"}]

    e.client.balance = lambda **_: [{"asset": "USDT", "availableBalance": "1000"}]
    allowed = await e._check_risk_limits(
        "TUSDT", Decimal("100"), RiskLevel.NORMAL, 20, account_balance=Decimal("1000"), direction=Direction.SHORT
    )
    assert allowed is True

    action = TradeAction(
        action=GeminiAction.NEW_SIGNAL,
        pair="TUSDT",
        direction=Direction.SHORT,
        order_type=OrderType.LIMIT,
        entry_price=Decimal("105"),
        stop_loss=Decimal("115"),
        take_profit_levels=[Decimal("95")],
        risk_level=RiskLevel.NORMAL
    )

    tp_updated = []
    async def mock_update_tp(pair, tp_levels):
        active_pos.tp_levels = tp_levels
        tp_updated.append((pair, tp_levels))
    e.position_manager.update_tp = mock_update_tp

    accepted = await e._handle_new_signal(action, message_db_id=99)
    assert accepted is True
    assert "TUSDT_SHORT" in e.position_manager.pending
    assert active_pos.tp_levels == [Decimal("105"), Decimal("105")]
    assert ("TUSDT", [Decimal("105"), Decimal("105")]) in tp_updated


@pytest.mark.asyncio
async def test_tp_partial_forced_bypass_price_check_property():
    e = _engine()
    e.client.positions = [{"symbol": "BTCUSDT", "positionAmt": "0.1", "entryPrice": "100"}]
    pos = RunningPosition(
        "BTCUSDT", Direction.LONG, Decimal("100"), Decimal("90"), 
        [Decimal("105"), Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow()
    )
    await e.position_manager.add_position(pos)
    
    async def mock_get_price(pair):
        return Decimal("101")
    e._get_market_reference_price = mock_get_price
    
    action = TradeAction(
        action=GeminiAction.TP_PARTIAL,
        pair="BTCUSDT",
        risk_level=RiskLevel.NORMAL,
        raw_response={"is_force_tp_partial": True}
    )
    
    success = await e.execute_action(action)
    assert success is True
    
    close_orders = [o for o in e.client.orders if o.get("type") == "MARKET" and o.get("reduceOnly") == "true"]
    assert close_orders
    stop_orders = [o for o in e.client.orders if o.get("type") == "STOP_MARKET"]
    assert stop_orders
    assert stop_orders[-1].get("stopPrice") == "105"
    assert pos.last_tp_partial_index_applied == 1


@pytest.mark.asyncio
async def test_set_tp_sl_orders_handles_immediate_trigger_sl_failure_property():
    e = _engine()
    pos = RunningPosition(
        "BTCUSDT", Direction.LONG, Decimal("100"), Decimal("90"), 
        [Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow()
    )
    await e.position_manager.add_position(pos)
    
    from binance.exceptions import BinanceAPIException
    import requests
    
    class _MockResponse:
        status_code = 400
        text = '{"code": -2021, "msg": "Order would immediately trigger."}'
        headers = {}
        
    def mock_create_order(**kwargs):
        if kwargs.get("type") == "STOP_MARKET":
            r = requests.Response()
            r.status_code = 400
            r._content = b'{"code": -2021, "msg": "Order would immediately trigger."}'
            raise BinanceAPIException(r, 400, r.text)
        order = {"orderId": 999, "symbol": kwargs.get("symbol"), "type": kwargs.get("type")}
        e.client.orders.append(order)
        return order
        
    e.client.new_order = mock_create_order
    
    await e._set_tp_sl_orders(pos)
    
    assert e.position_manager.get_position("BTCUSDT") is None
    close_orders = [o for o in e.client.orders if o.get("type") == "MARKET"]
    assert close_orders
    assert any("immediately triggered" in msg for msg in e.alert_service.sent_alerts)


@pytest.mark.asyncio
async def test_tp_partial_single_entry_flow_tp1_property():
    e = _engine()
    e.client.positions = [{"symbol": "BTCUSDT", "positionAmt": "0.1", "entryPrice": "100"}]
    e.db.trade_plans["BTCUSDT"] = {
        "id": 1,
        "extracted_data": {
            "entry_price": "100",
            "direction": "LONG",
        }
    }
    pos = RunningPosition(
        "BTCUSDT", Direction.LONG, Decimal("100"), Decimal("85"), [Decimal("105"), Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow()
    )
    await e.position_manager.add_position(pos)

    def mock_mark_price(symbol=None):
        return {"markPrice": "105"}
    e.client.mark_price = mock_mark_price

    await e.execute_action(TradeAction(action=GeminiAction.TP_PARTIAL, pair="BTCUSDT", risk_level=RiskLevel.NORMAL))

    market_orders = [o for o in e.client.orders if o.get("type") == "MARKET" and o.get("reduceOnly") == "true"]
    assert market_orders
    assert market_orders[0]["quantity"] == "0.07"
    stop_orders = [o for o in e.client.orders if o.get("type") == "STOP_MARKET"]
    assert stop_orders
    assert stop_orders[0]["stopPrice"] == "100.1"
    assert pos.last_tp_partial_index_applied == 0


@pytest.mark.asyncio
async def test_handle_cutloss_places_full_close_market_order_property():
    e = _engine()
    e.client.positions = [{"symbol": "BTCUSDT", "positionAmt": "0.1", "entryPrice": "100"}]
    pos = RunningPosition(
        "BTCUSDT", Direction.LONG, Decimal("100"), Decimal("85"), [Decimal("105"), Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow()
    )
    await e.position_manager.add_position(pos)

    def mock_get_open_orders(symbol=None):
        return [{"orderId": 101, "type": "STOP_MARKET"}, {"orderId": 102, "type": "TAKE_PROFIT_MARKET"}]
    e.client.futures_get_open_orders = mock_get_open_orders

    # Execute cutloss
    await e.execute_action(TradeAction(action=GeminiAction.CUTLOSS, pair="BTCUSDT", risk_level=RiskLevel.NORMAL))

    # Assert that positions and protection orders are cleaned up, and a market order was placed to close the position
    assert e.position_manager.get_position("BTCUSDT") is None
    assert any(o["orderId"] == 101 for o in e.client.canceled)
    assert any(o["orderId"] == 102 for o in e.client.canceled)

    market_orders = [o for o in e.client.orders if o.get("type") == "MARKET" and o.get("reduceOnly") == "true"]
    assert market_orders
    assert market_orders[0]["quantity"] == "0.1"
    assert e.alert_service.closed[-1] == ("BTCUSDT", "cutloss")



