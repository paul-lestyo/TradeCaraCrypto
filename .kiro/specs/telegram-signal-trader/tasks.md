# Implementation Plan: Telegram Signal Trader

## Overview

End-to-end automated trading system that reads signals from Telegram (Caracrypto), processes them via Gemini AI single-call (extract + classify), and executes on Binance Futures. Uses position TP/SL (TAKE_PROFIT_MARKET + STOP_MARKET with closePosition=true), Binance User Data Stream for limit order fill detection, and admin-driven mid-trade management.

Implementation language: Python (asyncio-native)

## Tasks

- [x] 1. Project structure, configuration, and domain models
  - [x] 1.1 Create project skeleton and configuration module
    - Create `CaraCrypto/__init__.py`, `CaraCrypto/__main__.py`
    - Create `CaraCrypto/config.py` with `load_config()`, `TelegramConfig`, `GeminiConfig`, `BinanceConfig`, `DatabaseConfig`, `AlertConfig`, `RiskConfig` dataclasses
    - Define `LEVERAGE_MAP` (BTCUSDT=125, ETHUSDT=100), `DEFAULT_LEVERAGE=50`, `MARGIN_MODE="CROSS"`
    - Define `TELEGRAM_GROUPS` and `TELEGRAM_FORUM_TOPICS` configuration
    - Create `.env.example` with all required environment variables
    - _Requirements: 5.3, 5.4, 2.17_

  - [x] 1.2 Create domain models and enums
    - Create `CaraCrypto/models.py` with `Direction`, `GeminiAction`, `OrderType`, `RiskLevel`, `OrderStatus` enums
    - Define `RawSignalMessage`, `TradeAction`, `RunningPosition`, `PositionState`, `MessageContext`, `OrderResult`, `RiskCheckResult` dataclasses
    - Define `VALID_TRANSITIONS` dict for order status state machine
    - _Requirements: 4.1, 5.1, 5.2_

  - [x] 1.3 Create custom exceptions module
    - Create `CaraCrypto/exceptions.py` with `TradingError`, `ParseError`, `ExtractionError`, `OrderExecutionError`, `RiskLimitError`, `PositionNotFoundError`, `ConnectionError`
    - _Requirements: 2.18, 5.23_

  - [x]* 1.4 Write property tests for configuration and models
    - **Property 12: Fixed Leverage Application** — verify LEVERAGE_MAP returns 125 for BTCUSDT, 100 for ETHUSDT, 50 for all others
    - **Property 6: Order Type Keyword Detection** — verify "NOW"/"entry NOW" → market, "antri"/"limit"/"kuning"/"tunggu kuning" → limit
    - **Validates: Requirements 2.6, 2.7, 2.17, 5.4**

- [x] 2. Database layer (PostgreSQL + SQLAlchemy)
  - [x] 2.1 Create database module with SQLAlchemy models
    - Create `CaraCrypto/database.py` with `MessageModel`, `RunningPositionModel`, `ModificationLogModel` SQLAlchemy models
    - `messages` table: id, message_id, group_id, topic_id, text, extracted_data (JSON), reply_to_message_id, reply_text, reply_extracted_data (JSON), gemini_action, received_at, processed_at
    - `running_positions` table: id, pair, direction, entry_price, current_sl, tp_levels (JSON), leverage, order_id, quantity, message_id (FK), opened_at
    - `modification_logs` table: id, pair, action_type, details (JSON), message_id (FK), timestamp
    - _Requirements: 10.2, 10.3, 10.4, 10.5_

  - [x] 2.2 Implement Database class methods
    - Implement `connect()`, `store_message()` (INSERT on arrival), `update_message_gemini_response()` (UPDATE after Gemini)
    - Implement `update_message_text()`, `get_message_by_telegram_id()`, `get_recent_messages()`
    - Implement `populate_reply_data()` — lookup replied-to row, copy text → reply_text, extracted_data → reply_extracted_data
    - Implement `get_running_positions()`, `store_position()`, `remove_position()`
    - Implement `store_modification_log()`, `get_daily_loss()`, `update_order_execution()`
    - _Requirements: 10.2, 10.3, 10.4, 10.5, 10.6, 10.7_

  - [x]* 2.3 Write property tests for database layer
    - **Property 24: Message Storage Round-Trip** — store and retrieve message with identical fields
    - **Property 25: Reply Data Population Correctness** — reply_text and reply_extracted_data copied correctly from replied-to row
    - **Validates: Requirements 10.2, 10.3**

- [x] 3. Docker configuration
  - [x] 3.1 Create Dockerfile and docker-compose.yml
    - Create `Dockerfile` (python:3.11-slim, install requirements, CMD python -m CaraCrypto)
    - Create `docker-compose.yml` with app + postgres:16-alpine services, healthcheck, volumes
    - Create `requirements.txt` with all dependencies (telethon, google-generativeai, sqlalchemy[asyncio], asyncpg, python-binance, aiohttp, python-dotenv, pydantic, Pillow)
    - _Requirements: 10.1_

- [x] 4. Checkpoint - Ensure project structure and database layer work
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Alert Service (WuzAPI WhatsApp)
  - [x] 5.1 Implement AlertService class
    - Create `CaraCrypto/alert_service.py` with `AlertService` class
    - Implement `send_alert()` with POST to `https://wuzapi.paulus-lestyo.my.id/chat/send/text`
    - Headers: accept=application/json, token=abc, Content-Type=application/json
    - Body format: `{"Phone": "6281239466830", "Body": "<message>"}`
    - Implement retry logic (max 3 retries, 10-second intervals)
    - Implement `notify_new_order()`, `notify_modification()`, `notify_tp_hit()`, `notify_sl_hit()`, `notify_risk_limit()`, `notify_error()`
    - Implement `_calculate_pnl_percent()` for LONG and SHORT directions
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9_

  - [x]* 5.2 Write property tests for AlertService
    - **Property 20: Alert Message Body Formatting** — verify JSON body format with correct phone and message
    - **Property 21: Order Notification Content Completeness** — verify notifications contain pair, direction, price, order type
    - **Property 22: PnL Calculation Correctness** — verify ((C-E)/E)*100 for LONG, ((E-C)/E)*100 for SHORT
    - **Validates: Requirements 8.3, 8.4, 8.5, 8.6, 8.7, 8.8**

- [x] 6. Position Manager
  - [x] 6.1 Implement PositionManager class
    - Create `CaraCrypto/position_manager.py` with `PositionManager` class
    - Implement `initialize()` — load running_positions from DB, allowed_running starts empty (in-memory only)
    - Implement `add_position()`, `remove_position()`, `update_sl()`, `update_tp()`
    - Implement `add_to_allowed_running()`, `remove_from_allowed_running()` (in-memory set operations)
    - Implement `get_running_positions()`, `get_running_pairs()`, `get_closed_today()`, `get_allowed_running()`, `has_position()`, `get_position()`
    - Implement `get_context_state()` returning `PositionState`
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8_

  - [x]* 6.2 Write property tests for PositionManager
    - **Property 9: Position State Consistency** — pair in running_positions iff active, pair in closed_today iff closed today, running_pairs = set of pairs in running_positions
    - **Property 10: Allowed Running Lifecycle** — pair stays in allowed_running until trade plan received or cancelled; starts empty on restart
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5, 4.6**

- [x] 7. Context Builder
  - [x] 7.1 Implement ContextBuilder class
    - Create `CaraCrypto/context_builder.py` with `ContextBuilder` class
    - Implement `build_context()` — query Position_Manager for state, retrieve last 10 messages from DB, combine with current message
    - Return `MessageContext` with current_message, history, position_state
    - _Requirements: 2.2, 4.7_

  - [x]* 7.2 Write property tests for ContextBuilder
    - **Property 3: Message Context Payload Completeness** — verify MessageContext contains current message, history ≤ 10, and complete position state
    - **Validates: Requirements 2.2, 4.7**

- [x] 8. Signal Listener (Telegram via Telethon)
  - [x] 8.1 Implement SignalListener class
    - Create `CaraCrypto/signal_listener.py` with `SignalListener` class
    - Implement `start()` — connect to Telegram using user account (api_id, api_hash, phone_number)
    - Implement `_handle_new_message()` — check group_id + topic_id filtering, download images, resolve reply chain, forward RawSignalMessage to signal_queue
    - Implement `_handle_message_edit()` — [OPEN]→[CLOSED] = skip, →[CANCEL] = process as cancel, other = update text only
    - Implement `_should_process_message()` — group in TELEGRAM_GROUPS AND (no topic filter OR topic in list)
    - Implement `_extract_media()`, `_resolve_reply_chain()`
    - Implement `_reconnect_with_backoff()` — exponential backoff up to 5 retries, alert on final failure
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 1.10_

  - [x]* 8.2 Write property tests for SignalListener
    - **Property 1: Forum Topic Message Filtering** — message forwarded iff group in TELEGRAM_GROUPS AND (no topic filter OR topic in list)
    - **Property 2: Reply Chain Data Completeness** — reply messages include reply_text and reply_image_data when available
    - **Property 26: Message Edit Classification Correctness** — [OPEN]→[CLOSED] = no re-process, →[CANCEL] = process as cancel, other = text update only
    - **Validates: Requirements 1.3, 1.4, 1.5, 1.6, 1.8**

- [x] 9. Signal Parser (Gemini AI Single-Call)
  - [x] 9.1 Implement SignalParser class
    - Create `CaraCrypto/signal_parser.py` with `SignalParser` class
    - Implement `parse_and_classify()` — single Gemini call: build prompt, collect images, call API, update DB row, validate and return TradeAction
    - Implement `_build_prompt()` — include action types, context (running positions, history), classification rules, extraction rules, image analysis instructions
    - Implement `_collect_images()` — gather current message image + reply image
    - Implement `_call_gemini()` — retry up to 3 times with 5-second intervals on API error
    - Implement `_validate_and_build_action()` — validate required fields per action type, return None if invalid
    - Handle all 9 action types: new_signal, update_sl, set_sl_breakeven, tp_partial, cancel, reverse, re_entry, cutloss, skip
    - Accept AI-determined close_percentage for tp_partial (no hardcoded default)
    - _Requirements: 2.1, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 2.11, 2.12, 2.13, 2.14, 2.15, 2.16, 2.17, 2.18, 2.19, 2.20, 2.21, 2.22, 2.23_

  - [x]* 9.2 Write property tests for SignalParser
    - **Property 4: Message Persistence Before Gemini** — message row exists with processed_at=NULL before Gemini call, updated after
    - **Property 5: Tag-Based Classification Hints** — [OPEN]/[CLOSED] → new_signal hint, [CANCEL] → cancel hint
    - **Property 7: Skip Action Produces No Execution** — action=skip returns TradeAction with SKIP, no execution
    - **Property 8: Invalid Gemini Response Rejection** — missing/invalid fields → return None
    - **Validates: Requirements 2.1, 2.4, 2.15, 2.18, 3.1, 3.2, 3.3**

- [x] 10. Checkpoint - Ensure parsing pipeline works end-to-end
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. Trade Engine (Binance Futures execution with Position TP/SL)
  - [x] 11.1 Implement TradeEngine core and action dispatch
    - Create `CaraCrypto/trade_engine.py` with `TradeEngine` class
    - Implement `execute_action()` — dispatch to correct handler based on GeminiAction
    - Implement `get_leverage()` — lookup from LEVERAGE_MAP, default 50
    - Implement `_set_margin_mode_cross()` — always CROSS margin
    - Implement `_set_leverage()` — set leverage for pair
    - Implement `_calculate_position_size()` — full size for normal, half for high_risk
    - Implement `_check_risk_limits()` — max concurrent positions, daily loss limit, balance check
    - _Requirements: 5.3, 5.4, 5.23, 5.24, 6.1, 6.2, 6.3, 9.1, 9.2, 9.3, 9.4, 9.5, 9.6_

  - [x] 11.2 Implement new_signal handler (market + limit order flows)
    - Implement `_handle_new_signal()`:
      - Market order: place order → fills instantly → immediately call `_set_tp_sl_orders()` → add to Position_Manager
      - Limit order: place order → register with Price_Watcher (`register_pending_order()`) → add to Position_Manager → do NOT set TP/SL yet
    - Implement `_place_market_order()`, `_place_limit_order()`
    - _Requirements: 5.1, 5.2, 5.6, 5.7_

  - [x] 11.3 Implement position TP/SL placement with nullable handling
    - Implement `_set_tp_sl_orders()`:
      - If take_profit_levels non-null/non-empty → place 1 TAKE_PROFIT_MARKET order with closePosition=true at final TP level (max for LONG, min for SHORT)
      - If stop_loss non-null → place 1 STOP_MARKET order with closePosition=true at stop_loss price
      - If take_profit_levels null/empty → skip TP order
      - If stop_loss null → skip SL order
      - If both null → skip entirely (pure admin-driven)
    - Implement `_get_final_tp_level()` — max(tp_levels) for LONG, min(tp_levels) for SHORT
    - Implement `_place_take_profit_market_order()` — type=TAKE_PROFIT_MARKET, closePosition=true
    - Implement `_place_stop_market_order()` — type=STOP_MARKET, closePosition=true
    - _Requirements: 5.5, 5.6, 5.8, 5.9, 5.10, 5.11, 5.12, 5.13, 5.14_

  - [x] 11.4 Implement modification handlers
    - Implement `_handle_update_sl()` — modify SL to new price
    - Implement `_handle_set_sl_breakeven()` — move SL to entry price
    - Implement `_handle_tp_partial()` — close AI-determined percentage at market, then update TP order quantity
    - Implement `_update_tp_order_quantity()` — cancel existing TP order, place new one with reduced quantity
    - Implement `_handle_cutloss()` — close entire position at market
    - _Requirements: 5.15, 5.16, 5.17, 5.18, 5.25_

  - [x] 11.5 Implement cancel, reverse, and re_entry handlers
    - Implement `_handle_cancel()` — if pending limit order → delete order; if filled position → close at market
    - Implement `_handle_reverse()` — close current position + open opposite direction with correct leverage
    - Implement `_handle_re_entry()` — open new position with provided entry parameters
    - _Requirements: 5.19, 5.20, 5.21, 5.22_

  - [x]* 11.6 Write property tests for TradeEngine
    - **Property 11: New Signal Action Dispatch** — market → place market order, limit → place limit order
    - **Property 27: Position TP/SL Order Placement with Nullable Handling** — verify TP/SL placement logic with all nullable combinations
    - **Property 13: CROSS Margin Mode Enforcement** — always CROSS, never Isolated
    - **Property 14: Cancel Action Dispatch Based on Position State** — pending → delete order, filled → close at market
    - **Property 15: Stop Loss Modification Correctness** — update_sl sets new price, set_sl_breakeven sets entry price
    - **Property 16: Partial Close Percentage** — close exactly P% of position quantity
    - **Property 28: TP Safety-Net Order Update After Partial Close** — TP order updated with reduced quantity after partial close
    - **Property 17: Reverse Action Correctness** — close current + open opposite with correct leverage
    - **Property 18: Risk Sizing Based on Signal Level** — normal = full size, high = half size
    - **Validates: Requirements 5.1-5.25, 6.1-6.3, 9.1-9.6**

- [x] 12. Price Watcher (WebSocket + User Data Stream)
  - [x] 12.1 Implement PriceWatcher core and price monitoring
    - Create `CaraCrypto/price_watcher.py` with `PriceWatcher` class
    - Implement `start()` — start price monitoring and User Data Stream listener
    - Implement `subscribe()`, `unsubscribe()` — manage WebSocket subscriptions per pair
    - Implement `_watch_price()` — monitor price, send alerts only (NO trade execution)
    - Implement `_check_tp_level_reached()` — direction-aware TP detection (LONG: price >= TP, SHORT: price <= TP)
    - Implement `_check_sl_reached()` — direction-aware SL detection (LONG: price <= SL, SHORT: price >= SL)
    - Implement WebSocket reconnection within 10 seconds on disconnect
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7_

  - [x] 12.2 Implement User Data Stream and limit order fill detection
    - Implement `_start_user_data_stream()` — subscribe to Binance User Data Stream WebSocket
    - Implement `register_pending_order(order_id, trade_action)` — store order_id → TradeAction mapping in `_pending_limit_orders`
    - Implement `_handle_order_update(event)`:
      - Case 1: order_id in _pending_limit_orders AND status=FILLED AND type=LIMIT → call Trade_Engine._set_tp_sl_orders(), remove from mapping, send alert
      - Case 2: order_type in (TAKE_PROFIT_MARKET, STOP_MARKET) AND status=FILLED → call _handle_position_closed()
    - Implement `_handle_position_closed(pair, close_type)` — unsubscribe, remove from Position_Manager, send alert (TP or SL)
    - _Requirements: 7.8, 7.9, 7.10, 7.11, 7.12_

  - [x]* 12.3 Write property tests for PriceWatcher
    - **Property 19: TP/SL Level Detection and Alerting (No Execution)** — direction-aware detection, alert only, no trade execution
    - **Property 29: Limit Order Fill Triggers Position TP/SL Placement** — register pending order, fill event triggers _set_tp_sl_orders, removed from mapping after
    - **Property 30: Position Closure Detection via User Data Stream** — TAKE_PROFIT_MARKET/STOP_MARKET fill → unsubscribe + remove position + alert
    - **Validates: Requirements 7.3, 7.4, 7.5, 7.6, 7.8, 7.10, 7.11, 7.12**

- [x] 13. Checkpoint - Ensure trade execution and price monitoring work
  - Ensure all tests pass, ask the user if questions arise.

- [x] 14. Integration wiring and application entry point
  - [x] 14.1 Implement main application entry point
    - Create `CaraCrypto/__main__.py` with `main()` async function
    - Initialize all components: Database, AlertService, PositionManager, ContextBuilder, SignalParser, TradeEngine, PriceWatcher, SignalListener
    - Wire circular reference: `trade_engine.price_watcher = price_watcher`
    - Implement `_process_signals()` loop:
      1. Receive RawSignalMessage from queue
      2. INSERT message row to DB (store_message)
      3. If reply → populate_reply_data
      4. Build context (context_builder.build_context)
      5. Single Gemini call (parser.parse_and_classify)
      6. If action != skip → execute (engine.execute_action)
      7. If new_signal/re_entry → subscribe price watcher
    - Run all components concurrently with asyncio.gather
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

  - [x]* 14.2 Write integration tests for signal processing pipeline
    - Test full flow: message → DB insert → context build → Gemini mock → DB update → trade execution mock
    - Test limit order flow: place limit → register with Price_Watcher → mock fill event → TP/SL placed
    - Test position closure flow: mock TAKE_PROFIT_MARKET fill → unsubscribe + remove position
    - **Validates: Requirements 2.1, 2.4, 5.6, 5.7, 5.8, 7.10**

- [x] 15. Risk management enforcement
  - [x] 15.1 Implement risk management checks in TradeEngine
    - Implement `_check_risk_limits()` fully:
      - Check max concurrent positions (configurable)
      - Check daily loss limit percent of balance
      - Check sufficient account balance
    - Queue new trades when max positions reached + send alert
    - Stop new positions when daily loss limit reached + send alert
    - Skip trade when balance insufficient + send alert
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6_

  - [x]* 15.2 Write property tests for risk management
    - **Property 23: Risk Management Enforcement** — max positions → queue, daily loss → refuse, insufficient balance → skip; all trigger alerts
    - **Validates: Requirements 9.1, 9.2, 9.3, 9.4, 9.5, 9.6**

- [x] 16. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties (30 total)
- Unit tests validate specific examples and edge cases
- Position TP/SL uses type=TAKE_PROFIT_MARKET and type=STOP_MARKET with closePosition=true (not open orders)
- Market order flow: fills instantly → immediately set position TP/SL
- Limit order flow: register with Price_Watcher → wait for fill event via User Data Stream → then set TP/SL
- Nullable TP/SL: skip TP if null, skip SL if null, skip both if both null
- `allowed_running` is in-memory only (Python set), NOT persisted to database

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.3"] },
    { "id": 1, "tasks": ["1.4", "2.1", "3.1"] },
    { "id": 2, "tasks": ["2.2"] },
    { "id": 3, "tasks": ["2.3", "5.1", "6.1"] },
    { "id": 4, "tasks": ["5.2", "6.2", "7.1"] },
    { "id": 5, "tasks": ["7.2", "8.1", "9.1"] },
    { "id": 6, "tasks": ["8.2", "9.2", "11.1"] },
    { "id": 7, "tasks": ["11.2", "11.3"] },
    { "id": 8, "tasks": ["11.4", "11.5"] },
    { "id": 9, "tasks": ["11.6", "12.1"] },
    { "id": 10, "tasks": ["12.2"] },
    { "id": 11, "tasks": ["12.3", "14.1", "15.1"] },
    { "id": 12, "tasks": ["14.2", "15.2"] }
  ]
}
```
