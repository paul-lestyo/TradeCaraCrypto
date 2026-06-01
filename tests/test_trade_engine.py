# Tujuan
# Test dispatch dasar dan helper trade engine.
# Caller
# pytest.
# Dependensi
# CaraCrypto.trade_engine, CaraCrypto.models.
# Main Functions
# Validasi property task 11.6, seleksi entry/margin/proteksi, dan adapter Binance.
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

    def balance(self, **_):
        return [{"asset": "USDT", "availableBalance": "1000"}]

    def futures_exchange_info(self, **_):
        return {"symbols": []}

    def mark_price(self, **_):
        return {"markPrice": "100"}

    def futures_leverage_bracket(self, **_):
        return []


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

    async def store_modification_log(self, pair, action_type, details, message_id):
        self.logs.append((pair, action_type, details, message_id))

    async def get_daily_loss(self, *_):
        return Decimal("0")


class _Alert:
    def __init__(self):
        self.errors = []
        self.modifications = []
        self.order_details = []
        self.protections = []

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


class _PM:
    def __init__(self):
        self.m = {}
        self.pending = {}

    async def add_position(self, p):
        self.m[p.pair] = p

    async def add_pending_position(self, p):
        self.pending[p.pair] = p

    async def remove_position(self, pair):
        self.m.pop(pair, None)
        self.pending.pop(pair, None)

    async def remove_pending_position(self, pair):
        self.pending.pop(pair, None)

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
        return self.pending.get(pair)

    def has_pending_position(self, pair):
        return pair in self.pending

    async def promote_pending_position(self, pair):
        pos = self.pending.pop(pair, None)
        if pos:
            self.m[pair] = pos
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
    assert e.get_leverage("BTCUSDT") == 125
    assert e.get_leverage("ETHUSDT") == 100
    assert e.get_leverage("XRPUSDT") == 50


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
async def test_tp_partial_sets_sl_plus_buffer_property():
    e = _engine()
    await e.position_manager.add_position(
        RunningPosition("BTCUSDT", Direction.LONG, Decimal("100"), Decimal("90"), [Decimal("110")], 50, "1", Decimal("0.1"), datetime.utcnow())
    )
    await e.execute_action(TradeAction(action=GeminiAction.TP_PARTIAL, pair="BTCUSDT", risk_level=RiskLevel.NORMAL))
    stop_orders = [o for o in e.client.orders if o.get("type") == "STOP_MARKET"]
    assert stop_orders
    assert stop_orders[-1].get("stopPrice") == "100.1"


@pytest.mark.asyncio
async def test_order_without_entry_uses_market_reference_property():
    e = _engine()
    accepted = await e.execute_action(
        TradeAction(
            action=GeminiAction.NEW_SIGNAL,
            pair="PROMPTUSDT",
            direction=Direction.LONG,
            order_type=OrderType.LIMIT,
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
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["quantity"] == "0.51"
    assert e.alert_service.order_details[-1][6] == "1.02"


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
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["quantity"] == "4050"
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
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["type"] == "LIMIT"
    assert e.client.orders[0]["price"] == "0.0064"
    assert e.position_manager.get_pending_position("BRETTUSDT").entry_price == Decimal("0.0064")


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
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["type"] == "MARKET"
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
            risk_level=RiskLevel.NORMAL,
        )
    )
    assert accepted is True
    assert e.client.orders[0]["type"] == "MARKET"
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
