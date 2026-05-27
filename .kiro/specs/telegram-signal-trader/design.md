# Design Document

## Overview

Dokumen ini menjelaskan arsitektur teknis untuk sistem Telegram Signal Trader — sebuah pipeline otomatis end-to-end yang membaca signal trading dari multiple Telegram groups dan forum topics (Caracrypto), mem-parsing menggunakan Gemini AI dengan pendekatan **single-call processing** (extract + classify dalam 1 request), mengelola state posisi melalui Position_Manager, dan mengeksekusi berbagai trading action di Binance Futures.

Arsitektur dirancang berdasarkan analisis 300 pesan nyata dari grup Caracrypto yang menunjukkan:
- Signal menggunakan teks informal bahasa Indonesia + chart image
- Entry/TP/SL levels ada di CHART IMAGES, bukan teks
- Reply chain adalah mekanisme utama penyampaian signal lengkap
- Tag [OPEN] dan [CLOSED] = buka posisi baru, [CANCEL] = batalkan
- "NOW" = market order, "antri/limit/kuning" = limit order
- "High risk" = setengah ukuran posisi

**Key Design Principles:**
1. **Single Gemini Call** — ONE request extracts data AND classifies action simultaneously
2. **Raw message saved FIRST** — every message goes to DB before Gemini (chat history preserved)
3. **Full Gemini response saved** — audit trail for every decision (skip or signal)
4. **Fixed leverage rules** — BTCUSDT=x125, ETHUSDT=x100, all others=x50 (NOT from signal)
5. **CROSS margin always** — all trades use CROSS margin mode
6. Position_Manager provides real-time context for accurate classification
7. Context_Builder assembles complete payload (history + positions + images)
8. Trade_Engine executes Gemini's decision without fallback logic

Seluruh aplikasi berjalan di dalam Docker containers (app + postgres) menggunakan docker-compose.

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ Signal_Listener │────▶│ Context_Builder  │────▶│  Signal_Parser   │
│   (Telethon)    │     │ (Payload Assembly)│     │(Gemini Single-Call)│
│ Multi-Group +   │     │ History + Positions│    │Extract + Classify │
│ Forum Topics +  │     │ + Images          │    └────────┬─────────┘
│ Reply Chain     │     └──────────────────┘             │
└─────────────────┘                                       ▼
                        ┌──────────────────┐     ┌──────────────────┐
┌─────────────────┐     │ Position_Manager │◀────│    Database      │
│  Alert_Service  │     │ (State Tracking) │     │  (PostgreSQL)    │
│    (WuzAPI)     │     │ Running/Closed/  │     │  Docker Container│
└────────▲────────┘     │ Allowed_Running  │     └────────┬─────────┘
         │              └──────────────────┘              │
         │              ┌──────────────────┐              ▼
         └──────────────│  Price_Watcher   │◀────┌──────────────────┐
                        │  (WebSocket)     │     │  Trade_Engine    │
                        └──────────────────┘     │(Binance Futures) │
                                                  │ 10 Action Types  │
                                                  │ Fixed Leverage   │
                                                  │ CROSS Margin     │
                                                  └──────────────────┘
```

### Processing Flow (Single Gemini Call)

```
Message arrives from Telegram
        │
        ▼
┌─────────────────────────────┐
│ 1. INSERT row in messages   │  ← Chat history ALWAYS preserved
│    (text, group_id, etc.)   │     processed_at = NULL
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│ 2. If reply → lookup prev   │  ← Copy reply_text + reply_extracted_data
│    row by reply_to_msg_id   │     from the replied message row
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│ 3. Build context payload    │  ← history + positions + images
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│ 4. ONE Gemini API call      │  ← text + image + reply_text +
│    Extract + Classify       │     reply_image + position context
│    → Single JSON response   │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│ 5. UPDATE row: extracted_   │  ← Full response saved (audit trail)
│    data, gemini_action,     │     Even for "skip" actions
│    processed_at             │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│ 6. If action != "skip"      │
│    → Trade_Engine executes  │  ← Leverage from fixed rules
│    → CROSS margin always    │
└─────────────────────────────┘
```

### Component Communication Flow

1. **Signal_Listener** → receives Telegram messages (text + images + reply chain) → pushes `RawSignalMessage` to `signal_queue`
2. **Database** → immediately inserts row in `messages` table (BEFORE any Gemini processing)
3. **Context_Builder** → queries Position_Manager + message history → assembles `MessageContext` payload
4. **Signal_Parser** → receives `MessageContext` → ONE Gemini call: extract data + classify action → produces `TradeAction` → UPDATEs the messages row with `extracted_data`, `gemini_action`, `processed_at`
5. **Position_Manager** → tracks running positions, closed_today, allowed_running (in-memory) → provides context to Context_Builder → updates state on trade execution
6. **Trade_Engine** → receives `TradeAction` → applies fixed leverage rules → sets CROSS margin → dispatches to correct handler (10 action types) → executes on Binance Futures
7. **Price_Watcher** → subscribes to WebSocket for open positions → detects TP/SL level reached → sends alerts via Alert_Service → subscribes to Binance User Data Stream → detects limit order fills → triggers Trade_Engine to set position TP/SL → detects position closures (TP/SL hit) → unsubscribes + updates Position_Manager
8. **Alert_Service** → called by other modules → sends WhatsApp notifications via WuzAPI

## Components and Interfaces

### 1. Signal_Listener

**Responsibility:** Connect to Telegram using user account and listen for messages from multiple configured premium groups and forum topics, including reply chain context and attached images. Also handles message edit events.

**Library:** Telethon (asyncio-native Telegram client)

**Key Behaviors:**
- Connects using user account credentials (api_id, api_hash, phone_number)
- Listens to ALL groups in `TELEGRAM_GROUPS` list
- Filters messages by forum topic using `TELEGRAM_FORUM_TOPICS` dict
- Downloads images from current message AND replied-to message
- Resolves reply chain: extracts reply_text and reply_image
- Forwards complete `RawSignalMessage` to signal_queue
- Handles message edit events (see Message Edit Handling Rules below)
- Implements exponential backoff reconnection (max 5 retries)

```python
@dataclass
class RawSignalMessage:
    """Complete message data from Telegram including reply chain."""
    text: str
    image_data: Optional[bytes] = None
    reply_text: Optional[str] = None
    reply_image_data: Optional[bytes] = None
    group_id: int = 0
    topic_id: Optional[int] = None
    message_id: int = 0
    reply_to_message_id: Optional[int] = None
    is_edit: bool = False
    timestamp: datetime = field(default_factory=datetime.utcnow)
```

```python
class SignalListener:
    def __init__(self, config: TelegramConfig, signal_queue: asyncio.Queue):
        self.config = config
        self.signal_queue = signal_queue
        self.client: TelegramClient = None
        self.max_retries = 5

    async def start(self) -> None: ...
    async def _handle_new_message(self, event) -> None: ...
    async def _handle_message_edit(self, event) -> None: ...
    def _should_process_message(self, group_id: int, topic_id: Optional[int]) -> bool: ...
    async def _extract_media(self, message) -> Optional[bytes]: ...
    async def _resolve_reply_chain(self, event) -> Tuple[Optional[str], Optional[bytes], Optional[int]]: ...
    async def _reconnect_with_backoff(self) -> None: ...
```

#### Message Edit Handling Rules

Telegram can send "message edited" events when admin edits a previously sent message. The system handles edits as follows:

**Common edit pattern:** `[OPEN] PAIR. Direction.` → edited to `[CLOSED] PAIR. Direction.` (admin marks position as entered)

**Rules:**
1. **[OPEN] → [CLOSED] edit: SKIP processing** — If a `message_id` already exists in the `messages` table AND the edit changes [OPEN] to [CLOSED], do NOT re-process. The trade was already executed when the original [OPEN] message was received.
2. **[OPEN] → [CANCEL] or [CLOSED] → [CANCEL] edit: PROCESS as cancel** — This IS actionable. Treat as a cancel signal and process it (cancel the pending/open order).
3. **Any other edit type: UPDATE text only** — Update the `text` field in the existing row but do NOT re-process through Gemini.

```python
async def _handle_message_edit(self, event) -> None:
    """Handle Telegram message edit events."""
    message_id = event.message.id
    new_text = event.message.text
    
    # Lookup existing row in messages table
    existing = await self.db.get_message_by_telegram_id(message_id, group_id)
    if not existing:
        return  # Unknown message, ignore edit
    
    old_text = existing.text
    
    # Rule 1: [OPEN] → [CLOSED] = skip (trade already executed)
    if "[OPEN]" in (old_text or "") and "[CLOSED]" in (new_text or ""):
        await self.db.update_message_text(existing.id, new_text)
        return
    
    # Rule 2: → [CANCEL] = actionable, process as cancel
    if "[CANCEL]" in (new_text or ""):
        await self.db.update_message_text(existing.id, new_text)
        # Forward as new message for Gemini processing
        raw_msg = RawSignalMessage(text=new_text, message_id=message_id, is_edit=True, ...)
        await self.signal_queue.put(raw_msg)
        return
    
    # Rule 3: Any other edit = update text only, no re-processing
    await self.db.update_message_text(existing.id, new_text)
```

### 2. Position_Manager

**Responsibility:** Track all running positions, closed_today pairs, and allowed_running pairs. Provides real-time state context for Gemini classification. `allowed_running` is tracked **in-memory only** (Python set) — it does NOT persist to database. On restart, it starts empty (acceptable because it's volatile short-lived data).

**Key Behaviors:**
- Maintains in-memory state synced with database (for running_positions)
- Tracks: running positions (pair, direction, entry_price, SL, TP levels, order_status)
- Tracks: closed_today (pairs closed in current trading day) — in-memory
- Tracks: allowed_running (pairs mentioned but without complete trade plan yet) — **in-memory only, NOT persisted**
- Updates state on every trade execution, modification, and closure
- Recovers running_positions from database on system restart
- Resets closed_today at configurable daily reset time (default 00:00 UTC)

```python
@dataclass
class RunningPosition:
    """A position currently active on Binance Futures."""
    pair: str
    direction: Direction
    entry_price: Decimal
    current_sl: Decimal
    tp_levels: List[Decimal]
    leverage: int
    order_id: str
    quantity: Decimal
    opened_at: datetime
    gemini_response_id: int
```

```python
class PositionManager:
    def __init__(self, db: Database):
        self.db = db
        self._running_positions: Dict[str, RunningPosition] = {}
        self._closed_today: Set[str] = set()
        self._allowed_running: Set[str] = set()  # In-memory only, NOT persisted

    async def initialize(self) -> None:
        """Load running_positions from database on startup. allowed_running starts empty."""
        positions = await self.db.get_running_positions()
        for pos in positions:
            self._running_positions[pos.pair] = pos
        # allowed_running is NOT loaded from DB — starts empty on restart

    async def add_position(self, position: RunningPosition) -> None: ...
    async def remove_position(self, pair: str) -> None: ...
    async def update_sl(self, pair: str, new_sl: Decimal) -> None: ...
    async def update_tp(self, pair: str, tp_levels: List[Decimal]) -> None: ...
    def add_to_allowed_running(self, pair: str) -> None:
        """Add pair to allowed_running set (in-memory only)."""
        self._allowed_running.add(pair)
    def remove_from_allowed_running(self, pair: str) -> None:
        """Remove pair from allowed_running set (in-memory only)."""
        self._allowed_running.discard(pair)

    def get_running_positions(self) -> List[RunningPosition]: ...
    def get_running_pairs(self) -> List[str]: ...
    def get_closed_today(self) -> List[str]: ...
    def get_allowed_running(self) -> List[str]: ...
    def has_position(self, pair: str) -> bool: ...
    def get_position(self, pair: str) -> Optional[RunningPosition]: ...

    def get_context_state(self) -> PositionState:
        """Return complete state for Context_Builder."""
        return PositionState(
            running_positions=self.get_running_positions(),
            running_pairs=self.get_running_pairs(),
            closed_today=self.get_closed_today(),
            allowed_running=self.get_allowed_running(),
        )
```

### 3. Context_Builder

**Responsibility:** Assemble the complete `MessageContext` payload that Gemini needs for accurate extraction and classification. Combines current message, reply chain, message history, and position state.

**Key Behaviors:**
- Queries Position_Manager for current state
- Retrieves last 10 messages from same topic from database
- Combines current message data (text + image + reply_text + reply_image)
- Produces structured `MessageContext` for Signal_Parser

```python
@dataclass
class PositionState:
    """Current position state from Position_Manager."""
    running_positions: List[RunningPosition]
    running_pairs: List[str]
    closed_today: List[str]
    allowed_running: List[str]

@dataclass
class MessageContext:
    """Complete context payload sent to Gemini AI."""
    current_message: RawSignalMessage
    history: List[Dict[str, Any]]  # Last 10 messages from same topic
    position_state: PositionState

class ContextBuilder:
    def __init__(self, position_manager: PositionManager, db: Database):
        self.position_manager = position_manager
        self.db = db

    async def build_context(self, raw_message: RawSignalMessage) -> MessageContext:
        """Assemble complete context for Gemini processing."""
        history = await self.db.get_recent_messages(
            group_id=raw_message.group_id,
            topic_id=raw_message.topic_id,
            limit=10
        )
        position_state = self.position_manager.get_context_state()
        return MessageContext(
            current_message=raw_message,
            history=history,
            position_state=position_state,
        )
```

### 4. Signal_Parser (Single Gemini Call)

**Responsibility:** Process messages through Gemini AI using a **single API call** that extracts trading data AND classifies the action simultaneously. Inserts message row to DB before calling Gemini, and updates the same row with Gemini response after.

**Library:** google-generativeai (Gemini API client with multimodal support)

**Key Behaviors:**
- **Single method: `parse_and_classify()`** — ONE Gemini call does everything
- Sends multimodal content (text + images + reply context + position context) to Gemini
- Gemini returns a single JSON with both extracted data AND action classification
- Message row INSERT to DB BEFORE Gemini call (chat history)
- Same row UPDATE with `extracted_data`, `gemini_action`, `processed_at` AFTER Gemini call (audit trail)
- Does NOT extract leverage (leverage is fixed per pair in Trade_Engine)
- Validates extracted data before producing TradeAction
- Retries Gemini API errors up to 3 times with 5-second intervals

```python
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

@dataclass
class TradeAction:
    """Output from Signal_Parser — the action to execute."""
    action: GeminiAction
    pair: Optional[str] = None
    direction: Optional[Direction] = None
    entry_price: Optional[Decimal] = None
    take_profit_levels: Optional[List[Decimal]] = None
    stop_loss: Optional[Decimal] = None
    order_type: Optional[OrderType] = None
    risk_level: RiskLevel = RiskLevel.NORMAL
    close_percentage: Optional[Decimal] = None  # For tp_partial
    new_sl: Optional[Decimal] = None  # For update_sl
    reasoning: Optional[str] = None  # Gemini's reasoning
    message_db_id: Optional[int] = None  # FK to messages table
    source_group_id: Optional[int] = None
    source_topic_id: Optional[int] = None
    # NOTE: No leverage field — leverage is determined by Trade_Engine
```

```python
class SignalParser:
    def __init__(self, config: GeminiConfig, db: Database):
        self.config = config
        self.db = db
        self.model = None
        self.max_retries = 3
        self.retry_interval = 5

    async def parse_and_classify(self, context: MessageContext) -> Optional[TradeAction]:
        """Single Gemini call: extract data + classify action in one request.
        
        Flow:
        1. Message row already INSERTed to DB by caller (signal processor)
        2. Build single prompt with all context
        3. Call Gemini once → get extraction + classification
        4. UPDATE message row with extracted_data, gemini_action, processed_at
        5. Validate and return TradeAction
        """
        prompt = self._build_prompt(context)
        images = self._collect_images(context)
        
        response = await self._call_gemini(prompt, images)
        
        # UPDATE the messages row with Gemini response (audit trail)
        await self.db.update_message_gemini_response(
            message_id=context.current_message.db_id,
            extracted_data=response,
            gemini_action=response.get("action", "skip")
        )
        
        return self._validate_and_build_action(response, context)

    def _build_prompt(self, context: MessageContext) -> str:
        """Build single prompt that asks Gemini to extract AND classify."""
        ...

    def _collect_images(self, context: MessageContext) -> List[bytes]:
        """Collect all available images (current + reply)."""
        images = []
        if context.current_message.image_data:
            images.append(context.current_message.image_data)
        if context.current_message.reply_image_data:
            images.append(context.current_message.reply_image_data)
        return images

    async def _call_gemini(self, prompt: str, images: List[bytes]) -> dict:
        """Call Gemini API with retry logic (max 3 retries, 5s interval)."""
        ...

    def _validate_and_build_action(self, response: dict, context: MessageContext) -> Optional[TradeAction]:
        """Validate Gemini response and build TradeAction. Returns None if invalid."""
        ...
```

#### Gemini Single-Call Prompt

```
You are a crypto trading signal processor for the Caracrypto Telegram group.
Your job is to BOTH classify the message type AND extract relevant trading data in ONE response.
The group uses informal Indonesian language. Trading levels (entry, TP, SL) are often in CHART IMAGES.

## Action Types:
- "new_signal": A new trading signal to open a position. Indicators: [OPEN] tag, [CLOSED] tag, mentions a pair with entry direction
- "update_sl": Update stop loss of an existing position. Indicators: "SL baru", "geser SL", "pindah SL"
- "set_sl_breakeven": Move SL to entry price. Indicators: "SL+", "set SL+", "SL breakeven", "SL di entry"
- "tp_partial": Take partial profit. Indicators: "TP sebagian", "close 50%", "ambil profit", "TP1 hit"
- "cancel": Cancel a signal/order. Indicators: [CANCEL] tag, "cancel", "batalkan", "skip"
- "reverse": Close current and open opposite. Indicators: "reverse", "balik arah"
- "re_entry": Re-enter a previously closed position. Indicators: "re-entry", "masuk lagi", "entry ulang"
- "cutloss": Close position at loss. Indicators: "cutloss", "cut loss", "CL", "close rugi"
- "skip": Not a trading signal. Indicators: commentary, analysis, jokes, questions, general discussion

## Context:
Running positions: {running_positions}
Running pairs: {running_pairs}
Closed today: {closed_today}
Allowed running (waiting for chart): {allowed_running}

## Recent message history (last 10):
{history}

## Current message:
Text: {current_text}
Reply to: {reply_text}
Has image: {has_image}
Has reply image: {has_reply_image}

## Classification Rules:
1. If message has [OPEN] or [CLOSED] tag → "new_signal"
2. If message has [CANCEL] tag → "cancel"
3. If message mentions a pair NOT in running_positions and provides direction → likely "new_signal"
4. If message mentions a pair IN running_positions with SL modification → "update_sl" or "set_sl_breakeven"
5. If message is general commentary/analysis without actionable instruction → "skip"
6. If pair is in allowed_running and this message completes the signal (has chart) → "new_signal"

## Extraction Rules (for non-skip actions):
1. PAIR: The trading pair (append "USDT" if not present). Examples: BTC→BTCUSDT, SOL→SOLUSDT
2. DIRECTION: "LONG" or "SHORT". Look for: "buy/long/beli" = LONG, "sell/short/jual" = SHORT
3. ENTRY_PRICE: From chart image (horizontal line at entry level). If text says "NOW" → null (market order)
4. TAKE_PROFIT_LEVELS: From chart image (TP1, TP2, TP3 lines). List from nearest to farthest.
5. STOP_LOSS: From chart image (SL line, usually red)
6. ORDER_TYPE: "market" if text contains "NOW"/"entry NOW". "limit" if text contains "antri"/"limit"/"kuning"/"tunggu kuning"
7. RISK_LEVEL: "high" if text contains "high risk"/"buat yg berani2 aja"/"hati2". Otherwise "normal"
8. CLOSE_PERCENTAGE: For tp_partial, default 50 if not specified
9. NEW_SL: For update_sl, the new stop loss price

## Image Analysis Instructions:
- Look for horizontal lines on the chart indicating price levels
- Entry is usually marked with a colored zone or line
- TP levels are above entry (LONG) or below entry (SHORT)
- SL is below entry (LONG) or above entry (SHORT)
- Read the price values from the Y-axis at each line level

## IMPORTANT: Do NOT extract leverage. Leverage is handled separately.

Respond with a SINGLE JSON object:
{
  "action": "new_signal|update_sl|set_sl_breakeven|tp_partial|cancel|reverse|re_entry|cutloss|skip",
  "pair": "BTCUSDT",
  "direction": "LONG|SHORT",
  "entry_price": 76500.0,
  "take_profit_levels": [77000, 78000, 79000],
  "stop_loss": 75000.0,
  "order_type": "market|limit",
  "risk_level": "normal|high",
  "close_percentage": 50,
  "new_sl": 76000.0,
  "image_type": "trade|profit|null",
  "reasoning": "Brief explanation of classification decision"
}

Notes:
- Include only fields relevant to the action type
- For "skip": only include "action" and "reasoning"
- For "update_sl": include "action", "pair", "new_sl", "reasoning"
- For "set_sl_breakeven": include "action", "pair", "reasoning"
- For "tp_partial": include "action", "pair", "close_percentage", "reasoning"
- For "cancel": include "action", "pair", "direction", "reasoning"
- For "cutloss": include "action", "pair", "reasoning"
- For "reverse": include "action", "pair", "direction" (new direction), "reasoning"
- For "new_signal"/"re_entry": include all extraction fields + "reasoning"
- "image_type": "trade" = chart with entry/TP/SL lines (actionable), "profit" = profit/loss screenshot (informational), null = no image
```

#### Gemini Response Schema

```json
{
  "action": "new_signal|update_sl|set_sl_breakeven|tp_partial|cancel|reverse|re_entry|cutloss|skip",
  "pair": "BTCUSDT",
  "direction": "LONG|SHORT",
  "entry_price": 76500.0,
  "take_profit_levels": [77000, 78000, 79000],
  "stop_loss": 75000.0,
  "order_type": "market|limit",
  "risk_level": "normal|high",
  "close_percentage": 50,
  "new_sl": 76000.0,
  "image_type": "trade|profit|null",
  "reasoning": "..."
}
```

**Required fields per action type:**

| Action | Required Fields |
|--------|----------------|
| new_signal | action, pair, direction, entry_price (null for market), take_profit_levels, stop_loss, order_type, risk_level, image_type, reasoning |
| update_sl | action, pair, new_sl, reasoning |
| set_sl_breakeven | action, pair, reasoning |
| tp_partial | action, pair, close_percentage, reasoning |
| cancel | action, pair, direction, reasoning |
| reverse | action, pair, direction (new), reasoning |
| re_entry | action, pair, direction, entry_price, take_profit_levels, stop_loss, order_type, risk_level, reasoning |
| cutloss | action, pair, reasoning |
| skip | action, reasoning |

### 5. Trade_Engine (10 Action Types + Fixed Leverage + Position TP/SL + Binance Filter Compliance)

**Responsibility:** Execute trading actions on Binance Futures based on Gemini AI's single-call response. Applies fixed leverage rules (capped by Binance max leverage), CROSS margin mode, and full Binance filter compliance (tick size, step size, min notional). Uses position TP/SL (TAKE_PROFIT_MARKET + STOP_MARKET with closePosition=true) as safety-net. Dispatches to the correct handler based on `TradeAction.action`.

**Library:** python-binance (AsyncClient for Binance Futures)

**Key Behaviors:**
- Dispatches TradeAction to correct handler based on action type
- 10 action handlers: market_order, limit_order, modify_sl, set_sl_breakeven, tp_partial, cutloss, cancel_pending, cancel_filled, reverse, re_entry
- **ALWAYS sets CROSS margin mode** before placing orders
- **Applies fixed leverage from LEVERAGE_MAP, capped by Binance max leverage** (NOT from signal)
- **Exchange Info Cache:** Queries Binance `GET /fapi/v1/exchangeInfo` on startup, caches tick_size, step_size, min_notional, max_leverage per pair, refreshes every 24 hours
- **Binance Filter Compliance:** All prices rounded to tick_size, all quantities rounded to step_size, min notional enforced
- **Position TP/SL:** Uses type=TAKE_PROFIT_MARKET and type=STOP_MARKET with closePosition=true
- **Market order flow:** Market order fills → immediately set position TP/SL
- **Limit order flow:** Limit order placed → register with Price_Watcher → wait for fill event → then set TP/SL
- **Nullable TP/SL:** Handles null take_profit_levels and/or null stop_loss gracefully
- **Position sizing:** Fixed margin per trade (TRADE_MARGIN_PERCENT% of balance), with high_risk_multiplier for high risk signals
- Enforces risk management rules before execution
- Updates Position_Manager state after execution
- Sends alerts on all executions and errors
- **Does NOT contain any classification, order type decision, or leverage extraction logic**

```python
# Fixed leverage configuration — NOT extracted from signals
LEVERAGE_MAP = {
    "BTCUSDT": 125,
    "ETHUSDT": 100,
    # All other pairs default to 50
}
DEFAULT_LEVERAGE = 50

@dataclass
class ExchangePairInfo:
    """Cached exchange info for a single trading pair."""
    tick_size: Decimal       # Price precision (e.g., 0.10 for BTCUSDT)
    step_size: Decimal       # Quantity precision (e.g., 0.001 for BTCUSDT)
    min_notional: Decimal    # Minimum order value (e.g., 5 USDT)
    max_leverage: int        # Maximum allowed leverage for this pair

class ExchangeInfoCache:
    """Cache for Binance exchange info (tick size, step size, min notional, max leverage per pair).
    
    Queries Binance GET /fapi/v1/exchangeInfo on startup and refreshes every 24 hours.
    Provides methods to retrieve filter values per pair.
    """
    
    def __init__(self, client: AsyncClient):
        self.client = client
        self._cache: Dict[str, ExchangePairInfo] = {}
        self._last_refresh: Optional[datetime] = None
        self._refresh_interval = timedelta(hours=24)
    
    async def initialize(self) -> None:
        """Load exchange info from Binance on startup."""
        await self._refresh_cache()
    
    async def _refresh_cache(self) -> None:
        """Query GET /fapi/v1/exchangeInfo and parse filters for all pairs."""
        exchange_info = await self.client.futures_exchange_info()
        for symbol_info in exchange_info.get("symbols", []):
            pair = symbol_info["symbol"]
            tick_size = Decimal("0.01")  # default
            step_size = Decimal("0.001")  # default
            min_notional = Decimal("5")  # default
            
            for f in symbol_info.get("filters", []):
                if f["filterType"] == "PRICE_FILTER":
                    tick_size = Decimal(f["tickSize"])
                elif f["filterType"] == "LOT_SIZE":
                    step_size = Decimal(f["stepSize"])
                elif f["filterType"] == "MIN_NOTIONAL":
                    min_notional = Decimal(f.get("notional", "5"))
            
            # Get max leverage from Binance leverage brackets API
            max_leverage = int(symbol_info.get("maxLeverage", 125))
            
            self._cache[pair] = ExchangePairInfo(
                tick_size=tick_size,
                step_size=step_size,
                min_notional=min_notional,
                max_leverage=max_leverage,
            )
        self._last_refresh = datetime.utcnow()
    
    async def _ensure_fresh(self) -> None:
        """Refresh cache if older than 24 hours."""
        if self._last_refresh is None or datetime.utcnow() - self._last_refresh > self._refresh_interval:
            await self._refresh_cache()
    
    def get_tick_size(self, pair: str) -> Decimal:
        """Get tick size (price precision) for a pair."""
        info = self._cache.get(pair)
        return info.tick_size if info else Decimal("0.01")
    
    def get_step_size(self, pair: str) -> Decimal:
        """Get step size (quantity precision) for a pair."""
        info = self._cache.get(pair)
        return info.step_size if info else Decimal("0.001")
    
    def get_min_notional(self, pair: str) -> Decimal:
        """Get minimum notional value for a pair."""
        info = self._cache.get(pair)
        return info.min_notional if info else Decimal("5")
    
    def get_max_leverage(self, pair: str) -> int:
        """Get maximum allowed leverage for a pair."""
        info = self._cache.get(pair)
        return info.max_leverage if info else 125

class TradeEngine:
    def __init__(self, config: BinanceConfig, risk_config: RiskConfig,
                 db: Database, position_manager: PositionManager,
                 alert_service: AlertService, price_watcher: "PriceWatcher" = None):
        self.config = config
        self.risk_config = risk_config
        self.db = db
        self.position_manager = position_manager
        self.alert_service = alert_service
        self.price_watcher = price_watcher
        self.client: AsyncClient = None
        self.leverage_map = LEVERAGE_MAP
        self.default_leverage = DEFAULT_LEVERAGE
        self.exchange_info_cache: ExchangeInfoCache = None  # Initialized in start()

    async def start(self) -> None:
        """Initialize Binance client and exchange info cache."""
        self.client = await AsyncClient.create(self.config.api_key, self.config.api_secret)
        self.exchange_info_cache = ExchangeInfoCache(self.client)
        await self.exchange_info_cache.initialize()

    def get_leverage(self, pair: str) -> int:
        """Get leverage for a pair, capped by Binance max leverage.
        
        Formula: actual_leverage = min(configured_leverage, pair_max_leverage)
        - configured_leverage comes from LEVERAGE_MAP (125 for BTCUSDT, 100 for ETHUSDT, 50 default)
        - pair_max_leverage comes from exchange_info_cache
        """
        configured = self.leverage_map.get(pair, self.default_leverage)
        max_allowed = self.exchange_info_cache.get_max_leverage(pair)
        return min(configured, max_allowed)

    async def execute_action(self, action: TradeAction) -> Optional[OrderResult]:
        """Dispatch to correct handler based on action type."""
        handlers = {
            GeminiAction.NEW_SIGNAL: self._handle_new_signal,
            GeminiAction.UPDATE_SL: self._handle_update_sl,
            GeminiAction.SET_SL_BREAKEVEN: self._handle_set_sl_breakeven,
            GeminiAction.TP_PARTIAL: self._handle_tp_partial,
            GeminiAction.CUTLOSS: self._handle_cutloss,
            GeminiAction.CANCEL: self._handle_cancel,
            GeminiAction.REVERSE: self._handle_reverse,
            GeminiAction.RE_ENTRY: self._handle_re_entry,
        }
        handler = handlers.get(action.action)
        if handler:
            return await handler(action)
        return None

    async def _handle_new_signal(self, action: TradeAction) -> Optional[OrderResult]:
        """Place market or limit order based on action.order_type.
        
        TP/SL Strategy (Position TP/SL):
        - Uses type=TAKE_PROFIT_MARKET and type=STOP_MARKET with closePosition=true
        - These are POSITION TP/SL (attached to position), NOT separate open orders
        - Position TP/SL can ONLY be set AFTER the position is filled
        - Market order: fills instantly → immediately set position TP/SL
        - Limit order: register with Price_Watcher → wait for fill event → then set TP/SL
        - Mid-trade TP partial and SL+ are admin-driven (via Telegram messages)
        
        Binance Filter Compliance:
        - All prices rounded to tick_size via _round_price()
        - All quantities rounded to step_size via _round_quantity()
        - Min notional enforced in _calculate_position_size()
        - Leverage capped by max leverage in get_leverage()
        """
        risk_check = await self._check_risk_limits(action)
        if not risk_check.allowed:
            await self.alert_service.notify_risk_limit(action, risk_check.reason)
            return None

        size = await self._calculate_position_size(action)
        leverage = self.get_leverage(action.pair)

        # Always CROSS margin + capped leverage
        await self._set_margin_mode_cross(action.pair)
        await self._set_leverage(action.pair, leverage)

        # Round entry price for limit orders
        if action.entry_price:
            action.entry_price = self._round_price(action.pair, action.entry_price)

        if action.order_type == OrderType.MARKET:
            result = await self._place_market_order(action, size)
            if result:
                # Market order fills instantly → immediately set position TP/SL
                await self._set_tp_sl_orders(action, result)
                await self.position_manager.add_position(
                    RunningPosition.from_action(action, result, leverage)
                )
        else:
            result = await self._place_limit_order(action, size)
            if result:
                # Limit order NOT filled yet → register with Price_Watcher for fill detection
                # Do NOT set TP/SL now (position doesn't exist yet)
                await self.price_watcher.register_pending_order(result.order_id, action)
                await self.position_manager.add_position(
                    RunningPosition.from_action(action, result, leverage)
                )

        return result

    async def _set_tp_sl_orders(self, action: TradeAction, result: OrderResult) -> None:
        """Place position TP/SL orders using TAKE_PROFIT_MARKET and STOP_MARKET with closePosition=true.
        
        - TP: type=TAKE_PROFIT_MARKET, closePosition=true, at FINAL TP level
        - SL: type=STOP_MARKET, closePosition=true, at stop_loss price
        - These are POSITION TP/SL — attached to the position, not separate open orders
        - Handles nullable TP/SL gracefully:
          - take_profit_levels null/empty → skip TP order
          - stop_loss null → skip SL order
          - both null → skip entirely (pure admin-driven management)
        """
        # Handle nullable TP
        if action.take_profit_levels:
            final_tp = self._get_final_tp_level(action)
            await self._place_take_profit_market_order(action.pair, action.direction, final_tp)
        
        # Handle nullable SL
        if action.stop_loss:
            await self._place_stop_market_order(action.pair, action.direction, action.stop_loss)

    def _get_final_tp_level(self, action: TradeAction) -> Decimal:
        """Get the LAST/final TP level from the list.
        For LONG: highest TP (last in ascending list)
        For SHORT: lowest TP (last in descending list, which is the smallest value)
        """
        if not action.take_profit_levels:
            raise ValueError("No take_profit_levels provided")
        if action.direction == Direction.LONG:
            return max(action.take_profit_levels)
        else:
            return min(action.take_profit_levels)

    async def _handle_update_sl(self, action: TradeAction) -> Optional[OrderResult]: ...
    async def _handle_set_sl_breakeven(self, action: TradeAction) -> Optional[OrderResult]: ...
    async def _handle_tp_partial(self, action: TradeAction) -> Optional[OrderResult]:
        """Close AI-determined percentage of position at market price.
        
        After partial close, updates the TP safety-net order on Binance
        to reflect the reduced position quantity.
        """
        position = self.position_manager.get_position(action.pair)
        if not position:
            return None
        
        close_qty = position.quantity * (action.close_percentage / Decimal("100"))
        result = await self._close_partial_market(action.pair, action.direction, close_qty)
        
        if result:
            remaining_qty = position.quantity - close_qty
            # Update the TP safety-net order quantity on Binance
            if remaining_qty > 0:
                await self._update_tp_order_quantity(action.pair, position.direction, remaining_qty)
            # Log modification
            await self.db.store_modification_log(
                pair=action.pair,
                action_type="tp_partial",
                details={"close_percentage": str(action.close_percentage), "close_qty": str(close_qty)}
            )
        return result

    async def _update_tp_order_quantity(self, pair: str, direction: Direction, new_quantity: Decimal) -> None:
        """Update the TP safety-net order on Binance to reflect reduced position quantity.
        
        After a tp_partial execution, the remaining position is smaller,
        so the TP order must be updated to match the new quantity.
        Cancels the existing TP order and places a new one with updated quantity.
        """
        ...

    async def _handle_cutloss(self, action: TradeAction) -> Optional[OrderResult]: ...
    async def _handle_cancel(self, action: TradeAction) -> Optional[OrderResult]: ...
    async def _handle_reverse(self, action: TradeAction) -> Optional[OrderResult]: ...
    async def _handle_re_entry(self, action: TradeAction) -> Optional[OrderResult]: ...

    def _calculate_position_size(self, action: TradeAction) -> Decimal:
        """Calculate position size using fixed margin per trade formula.
        
        Formula:
        - margin = balance * TRADE_MARGIN_PERCENT / 100
        - For high risk: margin = margin * high_risk_multiplier
        - quantity = margin * leverage / entry_price
        - Round quantity to step_size
        - If quantity * price < min_notional: quantity = min_notional / price (rounded to step_size)
        """
        balance = await self._get_account_balance()
        margin = balance * self.risk_config.trade_margin_percent / Decimal("100")
        
        if action.risk_level == RiskLevel.HIGH:
            margin = margin * self.risk_config.high_risk_multiplier
        
        leverage = self.get_leverage(action.pair)
        entry_price = action.entry_price or await self._get_current_price(action.pair)
        
        quantity = margin * Decimal(str(leverage)) / entry_price
        quantity = self._round_quantity(action.pair, quantity)
        
        # Enforce min notional
        min_notional = self.exchange_info_cache.get_min_notional(action.pair)
        if quantity * entry_price < min_notional:
            quantity = min_notional / entry_price
            quantity = self._round_quantity(action.pair, quantity)
        
        return quantity

    def _round_price(self, pair: str, price: Decimal) -> Decimal:
        """Round price to the nearest valid tick size for the pair.
        
        Uses cached tick_size from exchange info.
        Result is always a multiple of tick_size.
        """
        tick_size = self.exchange_info_cache.get_tick_size(pair)
        if tick_size == 0:
            return price
        return (price / tick_size).quantize(Decimal("1"), rounding=ROUND_DOWN) * tick_size

    def _round_quantity(self, pair: str, quantity: Decimal) -> Decimal:
        """Round quantity to the nearest valid step size for the pair.
        
        Uses cached step_size from exchange info.
        Result is always a multiple of step_size.
        """
        step_size = self.exchange_info_cache.get_step_size(pair)
        if step_size == 0:
            return quantity
        return (quantity / step_size).quantize(Decimal("1"), rounding=ROUND_DOWN) * step_size

    async def _set_margin_mode_cross(self, symbol: str) -> None:
        """Set margin mode to CROSS for the pair. Always CROSS, never Isolated."""
        ...

    async def _check_risk_limits(self, action: TradeAction) -> RiskCheckResult: ...
    async def _place_market_order(self, action: TradeAction, size: Decimal) -> OrderResult: ...
    async def _place_limit_order(self, action: TradeAction, size: Decimal) -> OrderResult: ...
    async def _get_account_balance(self) -> Decimal: ...
    async def _get_current_price(self, pair: str) -> Decimal: ...
    async def _place_take_profit_market_order(self, pair: str, direction: Direction, tp_price: Decimal) -> None:
        """Place a TAKE_PROFIT_MARKET position order with closePosition=true.
        
        This is a POSITION TP/SL order — it closes the entire position when triggered.
        Uses type=TAKE_PROFIT_MARKET, closePosition=true (no quantity needed).
        Can ONLY be placed after the position is filled.
        Price is rounded to tick_size for Binance compliance.
        """
        side = "SELL" if direction == Direction.LONG else "BUY"
        rounded_price = self._round_price(pair, tp_price)
        await self.client.futures_create_order(
            symbol=pair,
            side=side,
            type="TAKE_PROFIT_MARKET",
            stopPrice=str(rounded_price),
            closePosition="true",
        )

    async def _place_stop_market_order(self, pair: str, direction: Direction, sl_price: Decimal) -> None:
        """Place a STOP_MARKET position order with closePosition=true.
        
        This is a POSITION TP/SL order — it closes the entire position when triggered.
        Uses type=STOP_MARKET, closePosition=true (no quantity needed).
        Can ONLY be placed after the position is filled.
        Price is rounded to tick_size for Binance compliance.
        """
        side = "SELL" if direction == Direction.LONG else "BUY"
        rounded_price = self._round_price(pair, sl_price)
        await self.client.futures_create_order(
            symbol=pair,
            side=side,
            type="STOP_MARKET",
            stopPrice=str(rounded_price),
            closePosition="true",
        )

    async def _place_take_profit_order(self, pair: str, direction: Direction, tp_price: Decimal, quantity: Decimal) -> None:
        """[DEPRECATED — use _place_take_profit_market_order with closePosition=true instead]"""
        ...
    async def _place_stop_loss_order(self, pair: str, direction: Direction, sl_price: Decimal, quantity: Decimal) -> None:
        """[DEPRECATED — use _place_stop_market_order with closePosition=true instead]"""
        ...
    async def _close_partial_market(self, pair: str, direction: Direction, quantity: Decimal) -> Optional[OrderResult]:
        """Close a partial quantity of a position at market price.
        Quantity is rounded to step_size for Binance compliance."""
        rounded_qty = self._round_quantity(pair, quantity)
        ...
    async def _set_leverage(self, symbol: str, leverage: int) -> None: ...
```

### 6. Price_Watcher

**Responsibility:** Monitor real-time prices via Binance Futures WebSocket for open positions. Detects when price reaches TP/SL levels and sends alerts. Subscribe to Binance User Data Stream to detect limit order fills and trigger position TP/SL placement. Also detects position closures (TP/SL hit) and handles cleanup. **Does NOT execute any other trade actions** — mid-trade actions (TP partial, SL+) are driven by admin messages through Telegram.

**Library:** python-binance (BinanceSocketManager)

**Key Behaviors:**
- Subscribes to mark price streams for open positions
- Compares current price against TP levels and SL for **alerting purposes only**
- Sends WhatsApp alerts when TP/SL levels are reached
- **Subscribes to Binance User Data Stream (WebSocket)** for ORDER_TRADE_UPDATE events
- **Maintains `_pending_limit_orders` mapping** of order_id → TradeAction for pending limit orders
- **Detects limit order fills** → triggers Trade_Engine to set position TP/SL
- **Detects position closures** (TP/SL hit via ORDER_TRADE_UPDATE) → unsubscribes + updates Position_Manager
- Does NOT execute TP partial, SL modification, or other mid-trade actions
- Reconnects within 10 seconds on disconnect
- Unsubscribes when positions are closed

```python
class PriceWatcher:
    def __init__(self, db: Database, position_manager: PositionManager,
                 alert_service: AlertService, trade_engine: "TradeEngine"):
        self.db = db
        self.position_manager = position_manager
        self.alert_service = alert_service
        self.trade_engine = trade_engine
        self.subscriptions: Dict[str, asyncio.Task] = {}
        self._pending_limit_orders: Dict[str, TradeAction] = {}  # order_id → TradeAction
        self.client: AsyncClient = None
        self.bm: BinanceSocketManager = None

    async def start(self) -> None:
        """Start price monitoring and User Data Stream listener."""
        await self._start_user_data_stream()
        ...

    async def _start_user_data_stream(self) -> None:
        """Subscribe to Binance User Data Stream (WebSocket).
        
        Listens for ORDER_TRADE_UPDATE events to detect:
        1. Limit order fills → trigger position TP/SL placement
        2. Position closures (TP/SL hit) → cleanup and notify Position_Manager
        """
        ...

    async def subscribe(self, pair: str) -> None: ...
    async def unsubscribe(self, pair: str) -> None: ...
    async def _watch_price(self, pair: str) -> None:
        """Monitor price and send alerts only. Does NOT execute trades."""
        ...

    # --- User Data Stream Handlers ---

    async def register_pending_order(self, order_id: str, trade_action: TradeAction) -> None:
        """Register a pending limit order for fill detection.
        
        Called by Trade_Engine after placing a limit order.
        Stores the order_id → TradeAction mapping so that when the fill event
        arrives via User Data Stream, we can set position TP/SL with the correct params.
        """
        self._pending_limit_orders[order_id] = trade_action

    async def _handle_order_update(self, event: dict) -> None:
        """Handle ORDER_TRADE_UPDATE event from User Data Stream.
        
        Event structure (Binance):
        {
            "e": "ORDER_TRADE_UPDATE",
            "o": {
                "s": "BTCUSDT",       # Symbol
                "i": 12345,           # Order ID
                "X": "FILLED",        # Order status
                "o": "LIMIT",         # Order type
                "S": "BUY",           # Side
                "ap": "76500.00",     # Average price
                "q": "0.001",         # Original quantity
                ...
            }
        }
        
        Actions:
        1. If order_id is in _pending_limit_orders AND status=FILLED AND type=LIMIT:
           → Call Trade_Engine._set_tp_sl_orders() to place position TP/SL
           → Remove from _pending_limit_orders
        2. If order is a TAKE_PROFIT_MARKET or STOP_MARKET that FILLED:
           → Position was closed by TP/SL → call _handle_position_closed()
        """
        order_data = event.get("o", {})
        order_id = str(order_data.get("i", ""))
        order_status = order_data.get("X", "")
        order_type = order_data.get("o", "")
        symbol = order_data.get("s", "")

        # Case 1: Pending limit order filled → set position TP/SL
        if order_id in self._pending_limit_orders and order_status == "FILLED" and order_type == "LIMIT":
            trade_action = self._pending_limit_orders.pop(order_id)
            # Build a minimal OrderResult for _set_tp_sl_orders
            result = OrderResult(
                order_id=order_id,
                order_type=OrderType.LIMIT,
                executed_price=Decimal(order_data.get("ap", "0")),
                quantity=Decimal(order_data.get("q", "0")),
                timestamp=datetime.utcnow(),
            )
            await self.trade_engine._set_tp_sl_orders(trade_action, result)
            await self.alert_service.send_alert(
                f"✅ Limit order filled: {symbol} — Position TP/SL placed"
            )

        # Case 2: Position TP/SL order filled → position closed
        elif order_status == "FILLED" and order_type in ("TAKE_PROFIT_MARKET", "STOP_MARKET"):
            await self._handle_position_closed(symbol, order_type)

    async def _handle_position_closed(self, pair: str, close_type: str) -> None:
        """Handle position closure detected via User Data Stream.
        
        Called when a TAKE_PROFIT_MARKET or STOP_MARKET order fills,
        indicating the position was closed by the safety-net TP/SL.
        
        Actions:
        1. Unsubscribe from price WebSocket for this pair
        2. Remove position from Position_Manager
        3. Send alert notification
        """
        await self.unsubscribe(pair)
        await self.position_manager.remove_position(pair)
        
        if close_type == "TAKE_PROFIT_MARKET":
            await self.alert_service.send_alert(f"🎯 Position closed by TP: {pair}")
        else:
            await self.alert_service.send_alert(f"🛑 Position closed by SL: {pair}")

    # --- Price Monitoring (Alert Only) ---

    def _check_tp_level_reached(self, current_price: Decimal, position: RunningPosition) -> Optional[int]:
        """Check if price reached any TP level. Returns TP index or None.
        Used for ALERTING ONLY — does not trigger any trade execution."""
        for i, tp in enumerate(position.tp_levels):
            if position.direction == Direction.LONG and current_price >= tp:
                return i
            elif position.direction == Direction.SHORT and current_price <= tp:
                return i
        return None

    def _check_sl_reached(self, current_price: Decimal, position: RunningPosition) -> bool:
        """Check if price reached stop loss level.
        Used for ALERTING ONLY — Binance handles actual SL closure via placed order."""
        if position.direction == Direction.LONG:
            return current_price <= position.current_sl
        return current_price >= position.current_sl
```

### 7. Alert_Service

**Responsibility:** Send WhatsApp notifications via WuzAPI REST API.

**Library:** aiohttp (async HTTP client)

**Key Behaviors:**
- Sends POST requests to WuzAPI endpoint
- Formats messages for different event types
- Computes profit/loss percentages
- Retries on error up to 3 times with 10-second intervals

```python
class AlertService:
    def __init__(self, config: AlertConfig):
        self.config = config
        self.endpoint = "https://wuzapi.paulus-lestyo.my.id/chat/send/text"
        self.headers = {
            "accept": "application/json",
            "token": config.wuzapi_token,
            "Content-Type": "application/json",
        }
        self.max_retries = 3
        self.retry_interval = 10

    async def send_alert(self, message: str) -> bool: ...
    async def notify_new_order(self, action: TradeAction, result: OrderResult) -> None: ...
    async def notify_modification(self, action: TradeAction) -> None: ...
    async def notify_tp_hit(self, position: RunningPosition, tp_index: int, price: Decimal) -> None: ...
    async def notify_sl_hit(self, position: RunningPosition, price: Decimal) -> None: ...
    async def notify_risk_limit(self, action: TradeAction, reason: str) -> None: ...
    async def notify_error(self, context: str, error: str) -> None: ...

    def _format_request_body(self, message: str) -> dict:
        return {"Phone": self.config.phone_number, "Body": message}

    def _calculate_pnl_percent(self, entry: Decimal, close: Decimal, direction: Direction) -> Decimal:
        if direction == Direction.LONG:
            return ((close - entry) / entry) * 100
        return ((entry - close) / entry) * 100
```

### 8. Database

**Responsibility:** Persist messages (with Gemini responses merged), position states, and execution logs using PostgreSQL running in Docker.

**Libraries:** SQLAlchemy (async ORM) + asyncpg (async PostgreSQL driver)

**Key Behaviors:**
- Runs as PostgreSQL 16 in Docker container
- **messages table** — single table for both raw message AND Gemini response (merged)
  - INSERT on message arrival (text, group_id, topic_id, message_id, received_at)
  - If reply → lookup previous row, copy reply_text + reply_extracted_data
  - UPDATE after Gemini call (extracted_data, gemini_action, processed_at)
- Tracks order status transitions with validation
- Stores running positions state for recovery
- `allowed_running` is NOT stored in DB (in-memory only in Position_Manager)

**Database has 3 tables total:**
1. `messages` — all messages + Gemini responses (merged)
2. `running_positions` — active positions (for recovery on restart)
3. `modification_logs` — position modification history

```python
class Database:
    def __init__(self, config: DatabaseConfig):
        self.engine: AsyncEngine = None
        self.session_factory: async_sessionmaker = None

    async def connect(self) -> None: ...

    # Messages (INSERT on arrival, UPDATE after Gemini)
    async def store_message(self, raw_message: RawSignalMessage) -> int:
        """INSERT message row immediately on arrival. Returns the DB id for linking.
        Fields set: message_id, group_id, topic_id, text, received_at.
        extracted_data, gemini_action, processed_at are NULL at this point."""
        ...

    async def update_message_gemini_response(self, message_db_id: int, extracted_data: dict, gemini_action: str) -> None:
        """UPDATE message row with Gemini response after processing.
        Sets: extracted_data (full JSON), gemini_action, processed_at."""
        ...

    async def update_message_text(self, message_db_id: int, new_text: str) -> None:
        """UPDATE message text field (used for message edits)."""
        ...

    async def get_message_by_telegram_id(self, message_id: int, group_id: int) -> Optional[MessageModel]:
        """Lookup message by Telegram message_id + group_id."""
        ...

    async def get_recent_messages(self, group_id: int, topic_id: Optional[int], limit: int = 10) -> List[Dict]: ...

    # Reply chain handling
    async def populate_reply_data(self, message_db_id: int, reply_to_message_id: int, group_id: int) -> None:
        """Lookup the replied-to message row, copy its text → reply_text and its extracted_data → reply_extracted_data."""
        ...

    # Execution tracking
    async def update_order_execution(self, message_db_id: int, order_id: str,
                                      executed_price: Decimal, timestamp: datetime,
                                      applied_leverage: int) -> None: ...

    # Position management
    async def get_running_positions(self) -> List[RunningPosition]: ...
    async def store_position(self, position: RunningPosition) -> None: ...
    async def remove_position(self, pair: str) -> None: ...

    # Modification logs
    async def store_modification_log(self, pair: str, action_type: str, details: dict) -> None: ...

    # Risk management
    async def get_daily_loss(self) -> Decimal: ...
```

### Configuration Interfaces

```python
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Dict

@dataclass
class TelegramConfig:
    api_id: int
    api_hash: str
    phone_number: str
    groups: List[int]  # TELEGRAM_GROUPS
    forum_topics: Dict[int, List[int]]  # TELEGRAM_FORUM_TOPICS

@dataclass
class GeminiConfig:
    api_key: str
    model_name: str  # GEMINI_MODEL env var

@dataclass
class BinanceConfig:
    api_key: str
    api_secret: str
    testnet: bool = False

@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    username: str
    password: str

    @property
    def url(self) -> str:
        return f"postgresql+asyncpg://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class AlertConfig:
    wuzapi_token: str
    phone_number: str  # "6281239466830"

@dataclass
class RiskConfig:
    max_concurrent_positions: int = 5
    max_position_size_percent: Decimal = Decimal("5.0")
    daily_loss_limit_percent: Decimal = Decimal("10.0")
    high_risk_multiplier: Decimal = Decimal("0.5")
    trade_margin_percent: Decimal = Decimal("1.0")  # TRADE_MARGIN_PERCENT: % of balance used as margin per trade
```

### Internal Communication Interfaces

```python
from enum import Enum
from typing import List, Optional, Dict, Set
from decimal import Decimal
from datetime import datetime

class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"

class OrderStatus(str, Enum):
    PENDING = "pending"          # Limit order placed, waiting fill
    OPEN = "open"                # Position active
    PARTIALLY_CLOSED = "partially_closed"  # TP partial executed
    FILLED = "filled"            # All TPs hit, fully closed in profit
    CANCELLED = "cancelled"      # Order cancelled before fill
    STOPPED_OUT = "stopped_out"  # SL hit
    CUTLOSS = "cutloss"          # Manually closed at loss
    REVERSED = "reversed"        # Closed and reversed

# Valid status transitions
VALID_TRANSITIONS: Dict[OrderStatus, Set[OrderStatus]] = {
    OrderStatus.PENDING: {OrderStatus.OPEN, OrderStatus.CANCELLED},
    OrderStatus.OPEN: {OrderStatus.PARTIALLY_CLOSED, OrderStatus.FILLED,
                       OrderStatus.CANCELLED, OrderStatus.STOPPED_OUT,
                       OrderStatus.CUTLOSS, OrderStatus.REVERSED},
    OrderStatus.PARTIALLY_CLOSED: {OrderStatus.FILLED, OrderStatus.STOPPED_OUT,
                                    OrderStatus.CUTLOSS, OrderStatus.REVERSED},
    OrderStatus.FILLED: set(),       # terminal
    OrderStatus.CANCELLED: set(),    # terminal
    OrderStatus.STOPPED_OUT: set(),  # terminal
    OrderStatus.CUTLOSS: set(),      # terminal
    OrderStatus.REVERSED: set(),     # terminal
}

@dataclass
class RiskCheckResult:
    allowed: bool
    reason: Optional[str] = None

@dataclass
class OrderResult:
    order_id: str
    order_type: OrderType
    executed_price: Decimal
    quantity: Decimal
    timestamp: datetime
```

## Data Models

### messages Table (single merged table — INSERT on arrival, UPDATE after Gemini)

```python
class MessageModel(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(Integer, nullable=False)  # Telegram message ID
    group_id = Column(BigInteger, nullable=False, index=True)
    topic_id = Column(Integer, nullable=True, index=True)
    text = Column(String, nullable=True)
    extracted_data = Column(JSON, nullable=True)  # Full Gemini response JSON: {action, pair, direction, entry_price, take_profit_levels, stop_loss, order_type, risk_level, image_type, reasoning}
    reply_to_message_id = Column(Integer, nullable=True)  # Telegram msg ID of replied message
    reply_text = Column(String, nullable=True)  # text from replied message
    reply_extracted_data = Column(JSON, nullable=True)  # copy of extracted_data from the replied message row
    gemini_action = Column(String(30), nullable=True)  # denormalized for quick filter: new_signal/skip/update_sl/etc
    received_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)  # null if not yet processed by Gemini
```

**Processing logic:**
1. Message arrives → INSERT row (`message_id`, `group_id`, `topic_id`, `text`, `received_at`)
2. If reply → lookup previous row by `reply_to_message_id`, copy its `text` → `reply_text`, copy its `extracted_data` → `reply_extracted_data`
3. Gemini call → UPDATE row with `extracted_data`, `gemini_action`, `processed_at`

**`extracted_data` JSON example:**
```json
{
  "action": "new_signal",
  "pair": "BTCUSDT",
  "direction": "SHORT",
  "entry_price": 76500.0,
  "take_profit_levels": [75000, 74000, 73000],
  "stop_loss": 77500.0,
  "order_type": "market",
  "risk_level": "normal",
  "image_type": "trade",
  "reasoning": "[CLOSED] tag detected, chart shows short setup"
}
```

**`image_type` values (inside extracted_data JSON):**
- `"trade"` = chart with entry/TP/SL lines (actionable signal)
- `"profit"` = screenshot of profit/loss result (informational)
- `null` = no image or image not relevant

### running_positions Table

```python
class RunningPositionModel(Base):
    __tablename__ = "running_positions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pair = Column(String(20), nullable=False, unique=True, index=True)
    direction = Column(SAEnum(Direction), nullable=False)
    entry_price = Column(Numeric(20, 8), nullable=False)
    current_sl = Column(Numeric(20, 8), nullable=False)
    tp_levels = Column(JSON, nullable=False)
    leverage = Column(Integer, nullable=False)
    order_id = Column(String(50), nullable=False)
    quantity = Column(Numeric(20, 8), nullable=False)
    message_id = Column(Integer, ForeignKey("messages.id"), nullable=False)
    opened_at = Column(DateTime, nullable=False)
```

### modification_logs Table

```python
class ModificationLogModel(Base):
    __tablename__ = "modification_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pair = Column(String(20), nullable=False, index=True)
    action_type = Column(String(30), nullable=False)  # update_sl, set_sl_breakeven, tp_partial, etc.
    details = Column(JSON, nullable=False)
    message_id = Column(Integer, ForeignKey("messages.id"), nullable=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
```

**Note:** `allowed_running` is tracked **in-memory only** by Position_Manager (Python set). It does NOT persist to database. On restart, it starts empty (acceptable because it's volatile short-lived data).

## Error Handling

### Retry Strategy

| Component | Max Retries | Interval | Strategy |
|-----------|-------------|----------|----------|
| Signal_Listener (Telegram) | 5 | Exponential backoff (2^n seconds) | Reconnect, alert on final failure |
| Signal_Parser (Gemini) | 3 | Fixed 5 seconds | Retry same request |
| Alert_Service (WuzAPI) | 3 | Fixed 10 seconds | Retry same request |
| Price_Watcher (WebSocket) | Unlimited | Max 10 seconds | Auto-reconnect |
| Trade_Engine (Binance) | 0 | N/A | Log + alert, no retry (avoid duplicate orders) |

### Error Propagation

```python
class TradingError(Exception):
    """Base exception for trading system."""
    pass

class ParseError(TradingError):
    """Signal could not be parsed by Gemini."""
    pass

class ExtractionError(TradingError):
    """Gemini extraction returned invalid/incomplete data."""
    pass

class OrderExecutionError(TradingError):
    """Order placement failed on Binance."""
    pass

class RiskLimitError(TradingError):
    """Risk management limit exceeded."""
    pass

class PositionNotFoundError(TradingError):
    """Position modification requested but position not found."""
    pass

class ConnectionError(TradingError):
    """External service connection failed."""
    pass
```

### Graceful Degradation

- If Gemini API is down: messages are still saved to DB (INSERT happens first), processing skipped, system continues listening
- If Binance API is down: TradeActions remain unexecuted, alerts are sent
- If WuzAPI is down: alerts are logged locally, trading continues
- If PostgreSQL is down: system halts (critical dependency), logs error
- If Position_Manager state is inconsistent: re-sync from Binance API on next startup

## Application Entry Point

```python
import asyncio
from CaraCrypto.config import load_config
from CaraCrypto.signal_listener import SignalListener
from CaraCrypto.context_builder import ContextBuilder
from CaraCrypto.signal_parser import SignalParser
from CaraCrypto.position_manager import PositionManager
from CaraCrypto.database import Database
from CaraCrypto.trade_engine import TradeEngine
from CaraCrypto.price_watcher import PriceWatcher
from CaraCrypto.alert_service import AlertService

async def main():
    config = load_config()

    # Initialize components
    db = Database(config.database)
    await db.connect()

    alert_service = AlertService(config.alert)
    position_manager = PositionManager(db)
    await position_manager.initialize()  # Load running_positions from DB; allowed_running starts empty

    context_builder = ContextBuilder(position_manager, db)
    signal_parser = SignalParser(config.gemini, db)
    trade_engine = TradeEngine(config.binance, config.risk, db, position_manager, alert_service)
    price_watcher = PriceWatcher(db, position_manager, alert_service, trade_engine)
    trade_engine.price_watcher = price_watcher  # Wire circular reference

    signal_queue = asyncio.Queue()
    signal_listener = SignalListener(config.telegram, signal_queue)

    # Run all components concurrently
    await asyncio.gather(
        signal_listener.start(),
        _process_signals(signal_queue, db, context_builder, signal_parser, trade_engine, price_watcher),
        price_watcher.start(),
    )

async def _process_signals(queue, db, context_builder, parser, engine, watcher):
    while True:
        raw_message = await queue.get()  # RawSignalMessage

        # Step 1: INSERT message row to DB FIRST (chat history)
        raw_message.db_id = await db.store_message(raw_message)

        # Step 2: If reply, populate reply_text and reply_extracted_data from previous row
        if raw_message.reply_to_message_id:
            await db.populate_reply_data(
                raw_message.db_id, raw_message.reply_to_message_id, raw_message.group_id
            )

        # Step 3: Build context with history + positions
        context = await context_builder.build_context(raw_message)

        # Step 4: Single Gemini call (extract + classify)
        # Also UPDATEs the messages row with extracted_data, gemini_action, processed_at
        trade_action = await parser.parse_and_classify(context)

        # Step 5: Execute if not skip
        if trade_action and trade_action.action != GeminiAction.SKIP:
            result = await engine.execute_action(trade_action)
            if result and trade_action.action in (GeminiAction.NEW_SIGNAL, GeminiAction.RE_ENTRY):
                await watcher.subscribe(trade_action.pair)

if __name__ == "__main__":
    asyncio.run(main())
```

## Project Structure

```
CaraCryptoNew/
├── CaraCrypto/
│   ├── __init__.py
│   ├── __main__.py            # Entry point (asyncio.run)
│   ├── config.py              # Configuration loading from env vars + LEVERAGE_MAP
│   ├── models.py              # Domain objects (enums, dataclasses)
│   ├── database.py            # SQLAlchemy models + Database class (3 tables)
│   ├── signal_listener.py     # Telegram listener (Telethon) - multi-group + topics + reply chain + edit handling
│   ├── context_builder.py     # Assembles MessageContext payload for Gemini
│   ├── position_manager.py    # Tracks running positions, closed_today, allowed_running (in-memory)
│   ├── signal_parser.py       # Gemini AI single-call parser (extract + classify in 1 request)
│   ├── trade_engine.py        # Binance Futures execution (10 action types + fixed leverage + CROSS margin)
│   ├── price_watcher.py       # WebSocket price monitoring
│   ├── alert_service.py       # WuzAPI WhatsApp alerts
│   └── exceptions.py          # Custom exceptions
├── tests/
│   ├── __init__.py
│   ├── test_signal_listener.py
│   ├── test_context_builder.py
│   ├── test_position_manager.py
│   ├── test_signal_parser.py
│   ├── test_trade_engine.py
│   ├── test_price_watcher.py
│   ├── test_alert_service.py
│   └── test_database.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

## Dependencies

```
telethon>=1.34.0
google-generativeai>=0.5.0
sqlalchemy[asyncio]>=2.0.0
asyncpg>=0.29.0
python-binance>=1.0.19
aiohttp>=3.9.0
python-dotenv>=1.0.0
pydantic>=2.0.0
Pillow>=10.0.0
```

## Docker Configuration

### Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "CaraCrypto"]
```

### docker-compose.yml

```yaml
version: "3.8"
services:
  app:
    build: .
    container_name: caracrypto-trader
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      - TELEGRAM_API_ID=${TELEGRAM_API_ID}
      - TELEGRAM_API_HASH=${TELEGRAM_API_HASH}
      - TELEGRAM_PHONE=${TELEGRAM_PHONE}
      - BINANCE_API_KEY=${BINANCE_API_KEY}
      - BINANCE_API_SECRET=${BINANCE_API_SECRET}
      - GEMINI_API_KEY=${GEMINI_API_KEY}
      - GEMINI_MODEL=${GEMINI_MODEL}
      - DATABASE_URL=postgresql+asyncpg://trader:${POSTGRES_PASSWORD}@postgres:5432/caracrypto
    volumes:
      - ./sessions:/app/sessions  # Telethon session persistence
    restart: unless-stopped

  postgres:
    image: postgres:16-alpine
    container_name: caracrypto-db
    environment:
      POSTGRES_DB: caracrypto
      POSTGRES_USER: trader
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U trader -d caracrypto"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  pgdata:
```

### Environment Variables (.env.example)

```env
# Telegram
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=your_api_hash_here
TELEGRAM_PHONE=+6281234567890

# Binance
BINANCE_API_KEY=your_binance_api_key
BINANCE_API_SECRET=your_binance_api_secret

# Gemini AI
GEMINI_API_KEY=your_gemini_api_key
GEMINI_MODEL=gemini-2.0-flash

# PostgreSQL
POSTGRES_PASSWORD=your_secure_password

# WuzAPI
WUZAPI_TOKEN=abc
WUZAPI_PHONE=6281239466830

# Risk / Position Sizing
TRADE_MARGIN_PERCENT=1
```

### Python Configuration (config.py)

```python
from typing import List, Dict

# Telegram group configuration (in Python code)
TELEGRAM_GROUPS: List[int] = [
    -1002647537685,
    -1003629502181,
]

TELEGRAM_FORUM_TOPICS: Dict[int, List[int]] = {
    -1002647537685: [4],   # Only topic 4 in this group
    -1003629502181: [2],   # Only topic 2 in this group
}

# Fixed leverage rules — NOT extracted from signals
LEVERAGE_MAP: Dict[str, int] = {
    "BTCUSDT": 125,
    "ETHUSDT": 100,
}
DEFAULT_LEVERAGE: int = 50

# All trades use CROSS margin mode (never Isolated)
MARGIN_MODE = "CROSS"
```

## Testing Strategy

### Unit Tests (Example-Based)
- Signal_Listener: connection setup, message forwarding, reconnection after 5 failures, message edit handling
- Context_Builder: payload assembly with various state combinations
- Position_Manager: state recovery from database on startup
- Signal_Parser: Gemini API retry on error, skip action handling, message row INSERT before Gemini call
- Trade_Engine: error logging and alerting on Binance failures, CROSS margin mode setting
- Price_Watcher: WebSocket reconnection timing, unsubscription on close, alert-only behavior (no trade execution), User Data Stream subscription, limit order fill detection, position closure handling
- Alert_Service: correct headers and endpoint, retry on WuzAPI errors

### Property-Based Tests (100+ iterations)
- Signal_Listener: forum topic filtering logic (group + topic combinations), message edit classification
- Context_Builder: message context payload completeness
- Position_Manager: state consistency after open/close sequences, allowed_running transitions
- Signal_Parser: invalid response rejection, keyword detection (NOW/antri/limit/kuning), tag recognition
- Trade_Engine: action dispatch correctness, risk sizing, cancel behavior (pending vs filled), leverage lookup correctness, single final TP + SL order placement, TP order quantity update after partial close
- Price_Watcher: TP/SL level detection logic (direction-aware), alert-only behavior (no execution), User Data Stream event handling (limit fill → TP/SL placement, position closure detection), pending order registration/removal
- Alert_Service: message body formatting, notification content completeness, PnL calculation
- Database: message storage round-trip, extracted_data JSON round-trip, status transition validation

### Integration Tests (1-3 examples)
- Telegram connection and message reception from configured groups
- Gemini API single-call (text + image + context) with structured JSON response
- Binance Futures order placement with CROSS margin and fixed leverage
- WebSocket subscription and price streaming
- WuzAPI POST request delivery

### Smoke Tests
- Docker containers (app + postgres) start successfully
- Environment variable loading for all components
- PostgreSQL accessibility from app container
- Position_Manager state recovery on restart

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Forum Topic Message Filtering

*For any* message with a group_id and topic_id, and *for any* configuration of TELEGRAM_GROUPS and TELEGRAM_FORUM_TOPICS: the message SHALL be forwarded if and only if (a) the group_id is in TELEGRAM_GROUPS AND (b) either the group has no entry in TELEGRAM_FORUM_TOPICS (all messages pass) OR the topic_id is in the group's topic list.

**Validates: Requirements 1.3, 1.4, 1.5**

### Property 2: Reply Chain Data Completeness

*For any* message that is a reply to another message, the forwarded `RawSignalMessage` SHALL contain the reply_text field populated with the replied-to message text, and if the replied-to message has an image, the reply_image_data field SHALL be non-null.

**Validates: Requirements 1.6, 1.8**

### Property 3: Message Context Payload Completeness

*For any* `RawSignalMessage` and *for any* Position_Manager state, the `MessageContext` produced by Context_Builder SHALL contain: the current message data, a history list of at most 10 messages from the same group+topic, and the complete position state (running_positions, running_pairs, closed_today, allowed_running).

**Validates: Requirements 2.2, 4.7**

### Property 4: Message Persistence Before Gemini

*For any* incoming message, the message row SHALL exist in the `messages` table (with `processed_at = NULL`) BEFORE the Gemini API call is made, and *for any* Gemini response (including "skip"), the same row SHALL be updated with `extracted_data`, `gemini_action`, and `processed_at` set to a non-null timestamp.

**Validates: Requirements 2.1, 2.4**

### Property 5: Tag-Based Classification Hints

*For any* message text containing the tag "[OPEN]" or "[CLOSED]", the classification hint SHALL be "new_signal". *For any* message text containing the tag "[CANCEL]", the classification hint SHALL be "cancel".

**Validates: Requirements 3.1, 3.2, 3.3**

### Property 6: Order Type Keyword Detection

*For any* message text containing "NOW" or "entry NOW" (case-insensitive), the order type hint SHALL be "market". *For any* message text containing "antri", "limit", "kuning", or "tunggu kuning" (case-insensitive), the order type hint SHALL be "limit".

**Validates: Requirements 2.6, 2.7**

### Property 7: Skip Action Produces No Execution

*For any* Gemini response with action="skip", the Signal_Parser SHALL return a TradeAction with action=SKIP, and the Trade_Engine SHALL not execute any order on Binance, regardless of any other fields present in the response.

**Validates: Requirements 2.15**

### Property 8: Invalid Gemini Response Rejection

*For any* Gemini response that does not contain valid structured output (missing required fields for the action type, invalid direction, non-numeric prices, or invalid action value), the Signal_Parser SHALL return None and no TradeAction shall be produced.

**Validates: Requirements 2.18**

### Property 9: Position State Consistency

*For any* sequence of position open and close operations, the Position_Manager SHALL maintain: (a) a pair appears in running_positions if and only if it has an active position, (b) a pair appears in closed_today if and only if it was closed during the current trading day, (c) running_pairs equals the set of pairs in running_positions.

**Validates: Requirements 4.1, 4.2, 4.3**

### Property 10: Allowed Running Lifecycle

*For any* pair added to allowed_running (in-memory set), it SHALL remain in allowed_running until either (a) a complete trade plan is received for that pair (then it is removed and the trade is executed) or (b) it is explicitly cancelled. On system restart, allowed_running starts empty.

**Validates: Requirements 4.4, 4.5, 4.6**

### Property 11: New Signal Action Dispatch

*For any* TradeAction with action=NEW_SIGNAL: if order_type is "market", the Trade_Engine SHALL place a market order; if order_type is "limit", the Trade_Engine SHALL place a limit order at the specified entry_price. The Trade_Engine SHALL NOT apply any independent order type logic.

**Validates: Requirements 5.1, 5.2, 5.15**

### Property 27: Position TP/SL Order Placement with Nullable Handling

*For any* new order placement: (a) if take_profit_levels is non-null and non-empty, the Trade_Engine SHALL place exactly 1 TAKE_PROFIT_MARKET order with closePosition=true at the FINAL TP level (max for LONG, min for SHORT); (b) if stop_loss is non-null, the Trade_Engine SHALL place exactly 1 STOP_MARKET order with closePosition=true at the stop_loss price; (c) if take_profit_levels is null/empty, no TP order SHALL be placed; (d) if stop_loss is null, no SL order SHALL be placed; (e) if both are null, no TP/SL orders SHALL be placed. Position TP/SL SHALL only be placed AFTER the position is filled (immediately for market orders, after fill event for limit orders).

**Validates: Requirements 5.5, 5.6, 5.7, 5.8, 5.9, 5.12, 5.13, 5.14**

### Property 12: Fixed Leverage Application

*For any* TradeAction targeting a trading pair, the Trade_Engine SHALL apply leverage from the fixed LEVERAGE_MAP: 125 for BTCUSDT, 100 for ETHUSDT, and 50 for all other pairs. The leverage SHALL NOT be extracted from the signal message.

**Validates: Requirements 2.16, 5.4**

### Property 13: CROSS Margin Mode Enforcement

*For any* new order placement, the Trade_Engine SHALL set the margin mode to CROSS for the trading pair before placing the order. Isolated margin SHALL never be used.

**Validates: Requirements 5.3**

### Property 14: Cancel Action Dispatch Based on Position State

*For any* TradeAction with action=CANCEL: if the pair has a pending limit order (status=PENDING), the Trade_Engine SHALL delete the pending order; if the pair has a filled/open position, the Trade_Engine SHALL close the position at market price.

**Validates: Requirements 3.4, 3.5, 5.12, 5.13**

### Property 15: Stop Loss Modification Correctness

*For any* TradeAction with action=UPDATE_SL, the Trade_Engine SHALL modify the stop loss to the new_sl price. *For any* TradeAction with action=SET_SL_BREAKEVEN, the Trade_Engine SHALL set the stop loss equal to the position's entry_price.

**Validates: Requirements 5.8, 5.9**

### Property 16: Partial Close Percentage

*For any* TradeAction with action=TP_PARTIAL and close_percentage P (where 0 < P ≤ 100), the Trade_Engine SHALL close exactly P% of the position quantity at market price.

**Validates: Requirements 5.10**

### Property 28: TP Safety-Net Order Update After Partial Close

*For any* tp_partial execution that reduces position quantity from Q to Q_remaining (where Q_remaining > 0), the Trade_Engine SHALL update the TP safety-net order on Binance to reflect the new quantity Q_remaining. The TP price level SHALL remain unchanged (still the final TP level).

**Validates: Requirements 5.25**

### Property 29: Limit Order Fill Triggers Position TP/SL Placement

*For any* limit order placed by Trade_Engine: (a) the order_id and associated TradeAction (with take_profit_levels and stop_loss) SHALL be registered with Price_Watcher immediately after placement; (b) when an ORDER_TRADE_UPDATE event with status=FILLED and type=LIMIT arrives for that order_id, the Price_Watcher SHALL trigger Trade_Engine._set_tp_sl_orders() with the stored TradeAction; (c) the pending order SHALL be removed from the mapping after TP/SL placement.

**Validates: Requirements 5.7, 5.8, 7.10, 7.11, 7.12**

### Property 30: Position Closure Detection via User Data Stream

*For any* ORDER_TRADE_UPDATE event where order_type is TAKE_PROFIT_MARKET or STOP_MARKET and status is FILLED, the Price_Watcher SHALL: (a) unsubscribe from the price WebSocket for that pair, (b) remove the position from Position_Manager, and (c) send an alert notification indicating whether closure was by TP or SL.

**Validates: Requirements 7.8**

### Property 17: Reverse Action Correctness

*For any* TradeAction with action=REVERSE for a pair with an existing position in direction D, the Trade_Engine SHALL (a) close the current position entirely and (b) open a new position in the opposite direction with the correct leverage from LEVERAGE_MAP.

**Validates: Requirements 5.14**

### Property 18: Risk Sizing Based on Signal Level

*For any* TradeAction with risk_level=NORMAL, the position size SHALL be the full configured base size. *For any* TradeAction with risk_level=HIGH, the position size SHALL be base_size × high_risk_multiplier (default 0.5).

**Validates: Requirements 6.1, 6.2, 6.3**

### Property 19: TP/SL Level Detection and Alerting (No Execution)

*For any* RunningPosition with direction D, take_profit_levels, and current_sl, and *for any* current price P: (a) a take-profit level reached SHALL be detected when P >= TP_level (for LONG) or P <= TP_level (for SHORT), and (b) a stop-loss level reached SHALL be detected when P <= current_sl (for LONG) or P >= current_sl (for SHORT). In ALL cases, the Price_Watcher SHALL only send an alert via Alert_Service and SHALL NOT execute any trade action, close any position, or modify any order — Binance handles actual TP/SL closure via the placed safety-net orders.

**Validates: Requirements 7.3, 7.4, 7.5, 7.6**

### Property 20: Alert Message Body Formatting

*For any* message text string, the Alert_Service SHALL format the WuzAPI request body as `{"Phone": "<configured_phone>", "Body": "<message_text>"}` with valid JSON structure and the configured phone number.

**Validates: Requirements 8.3**

### Property 21: Order Notification Content Completeness

*For any* trade execution event: the notification message SHALL contain the pair name, direction, price, and order type. *For any* position modification event: the notification SHALL contain the pair name, action type, and relevant modification details.

**Validates: Requirements 8.4, 8.5, 8.6**

### Property 22: PnL Calculation Correctness

*For any* TP or SL hit event with entry_price E, closing_price C, and direction D: the computed PnL percentage SHALL equal ((C - E) / E) × 100 for LONG positions, and ((E - C) / E) × 100 for SHORT positions.

**Validates: Requirements 8.7, 8.8**

### Property 23: Risk Management Enforcement

*For any* system state and new TradeAction: (a) if open positions count equals max_concurrent_positions, the action SHALL be queued; (b) if position size exceeds max_position_size_percent of balance, the action SHALL be rejected; (c) if accumulated daily losses reach daily_loss_limit_percent of balance, new positions SHALL be refused; (d) if required margin exceeds available balance, the action SHALL be skipped. In all cases, a WhatsApp alert SHALL be triggered.

**Validates: Requirements 9.1, 9.2, 9.3, 9.4, 9.5, 9.6**

### Property 24: Message Storage Round-Trip

*For any* valid RawSignalMessage, storing it in the `messages` table and then retrieving it SHALL produce a record with identical values for all fields: message_id, group_id, topic_id, text, and received_at. After Gemini processing, the same row SHALL contain the correct extracted_data JSON, gemini_action, and processed_at timestamp.

**Validates: Requirements 10.2, 10.3**

### Property 25: Reply Data Population Correctness

*For any* message that is a reply (has reply_to_message_id), after calling `populate_reply_data()`, the message row SHALL have `reply_text` equal to the text of the replied-to message row, and `reply_extracted_data` equal to the `extracted_data` of the replied-to message row (or NULL if the replied-to message has not been processed yet).

**Validates: Requirements 10.2**

### Property 26: Message Edit Classification Correctness

*For any* message edit event where the original text contains "[OPEN]" and the new text contains "[CLOSED]": the system SHALL NOT re-process through Gemini (trade already executed). *For any* edit where the new text contains "[CANCEL]": the system SHALL process it as a cancel signal. *For any* other edit: the system SHALL update the text field only without re-processing.

**Validates: Requirements 3.1, 3.2, 3.3**

### Property 31: Binance Filter Compliance

*For any* trading pair and *for any* order placed by the Trade_Engine: (a) all prices (entry_price, take_profit, stop_loss) SHALL be rounded to a multiple of the pair's tick_size; (b) all quantities SHALL be rounded to a multiple of the pair's step_size; (c) the final order value (quantity × price) SHALL be greater than or equal to the pair's min_notional — if the initial calculated quantity results in a value below min_notional, quantity SHALL be adjusted upward to min_notional / price (rounded to step_size); (d) the actual leverage applied SHALL equal min(configured_leverage, pair_max_leverage) where configured_leverage comes from LEVERAGE_MAP and pair_max_leverage comes from the exchange info cache.

**Validates: Requirements 5.4, 5.26, 5.27, 5.28, 5.29, 5.30, 6.5, 6.6, 12.14**
