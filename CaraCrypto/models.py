# Tujuan
# Model domain utama untuk signal, action, dan state posisi.
# Caller
# Semua module service.
# Dependensi
# dataclasses, enum.
# Main Functions
# Struktur data lintas modul.
# Side Effects
# Tidak ada.

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional


class Direction(str, Enum):
    LONG = "long"
    SHORT = "short"


class GeminiAction(str, Enum):
    NEW_SIGNAL = "new_signal"
    UPDATE_SL = "update_sl"
    SET_SL_BREAKEVEN = "set_sl_breakeven"
    TP_PARTIAL = "tp_partial"
    CANCEL = "cancel"
    REVERSE = "reverse"
    RE_ENTRY = "re_entry"
    CUTLOSS = "cutloss"
    SKIP = "skip"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class RiskLevel(str, Enum):
    NORMAL = "normal"
    HIGH = "high"


class OrderStatus(str, Enum):
    PENDING = "pending"
    FILLED = "filled"
    CLOSED = "closed"


@dataclass
class RawSignalMessage:
    text: str
    group_id: int
    message_id: int
    topic_id: Optional[int] = None
    image_data: Optional[bytes] = None
    reply_text: Optional[str] = None
    reply_image_data: Optional[bytes] = None
    reply_to_message_id: Optional[int] = None
    is_edit: bool = False
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class TradeAction:
    action: GeminiAction
    pair: Optional[str] = None
    direction: Optional[Direction] = None
    order_type: Optional[OrderType] = None
    entry_price: Optional[Decimal] = None
    take_profit_levels: Optional[List[Decimal]] = None
    stop_loss: Optional[Decimal] = None
    risk_level: RiskLevel = RiskLevel.NORMAL
    close_percentage: Optional[float] = None
    raw_response: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RunningPosition:
    pair: str
    direction: Direction
    entry_price: Decimal
    current_sl: Optional[Decimal]
    tp_levels: List[Decimal]
    leverage: int
    order_id: str
    quantity: Decimal
    opened_at: datetime
    message_db_id: Optional[int] = None


@dataclass
class PositionState:
    running_positions: List[RunningPosition]
    running_pairs: List[str]
    closed_today: List[str]
    allowed_running: List[str]


@dataclass
class MessageContext:
    current_message: RawSignalMessage
    history: List[Dict[str, Any]]
    position_state: PositionState


@dataclass
class OrderResult:
    success: bool
    order_id: Optional[str] = None
    status: Optional[OrderStatus] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RiskCheckResult:
    allowed: bool
    reason: Optional[str] = None
    should_queue: bool = False


VALID_TRANSITIONS = {
    OrderStatus.PENDING: {OrderStatus.FILLED, OrderStatus.CLOSED},
    OrderStatus.FILLED: {OrderStatus.CLOSED},
    OrderStatus.CLOSED: set(),
}
