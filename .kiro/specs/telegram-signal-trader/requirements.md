# Requirements Document

## Introduction

Sistem otomatis end-to-end untuk membaca signal trading dari grup premium Telegram (Caracrypto), mem-parsing signal menggunakan Gemini AI dengan pendekatan **single-call processing**, dan mengeksekusi berbagai trading action di Binance Futures. Sistem dirancang berdasarkan analisis 300 pesan nyata dari grup Caracrypto yang menunjukkan bahwa signal BUKAN format standar "Entry: X, TP: Y, SL: Z", melainkan teks informal bahasa Indonesia dengan chart image yang berisi level entry/TP/SL.

Alur pemrosesan single-call:
1. **Message arrives** → INSERT row ke `messages` table segera (text, message_id, group_id, topic_id, received_at). History chat selalu preserved.
2. **Single Gemini call** — kirim message (text + image + reply_text + reply_image + position context) → Gemini mengembalikan KEDUA extracted data DAN action classification dalam SATU response.
3. **UPDATE row** — extracted_data JSON, gemini_action, dan processed_at di-update ke row yang sama di `messages` table (audit trail).
4. **Execute** — jika action bukan "skip", Trade_Engine mengeksekusi.

Keuntungan single-call:
- 1 API call = lebih cepat, lebih murah
- Gemini Flash cukup pintar untuk extract + classify dalam 1 prompt
- Image hanya dikirim sekali
- Code lebih sederhana
- Untuk skip messages: tidak perlu image extraction, tapi history chat tetap di DB
- Full Gemini response selalu tersimpan untuk audit

Sistem harus menangani berbagai tipe pesan: new signal, update SL, set SL+, TP partial, cancel, reverse, re-entry, cutloss, dan commentary. Reply chain (multi-part messages) adalah mekanisme utama penyampaian signal lengkap. Gemini AI menangani SEMUA keputusan trading tanpa fallback logic atau guard code.

**TP/SL Strategy:**
- **Position TP/SL (not open orders):** Sistem menempatkan TP dan SL sebagai POSITION TP/SL (type=TAKE_PROFIT_MARKET dan type=STOP_MARKET dengan closePosition=true), BUKAN sebagai separate conditional open orders. Position TP/SL hanya bisa di-set SETELAH posisi terisi.
- **Market order flow:** Market order → langsung terisi → langsung set position TP/SL.
- **Limit order flow:** Limit order ditempatkan → BELUM terisi → BELUM bisa set TP/SL → sistem listen ke Binance User Data Stream (WebSocket) untuk ORDER_TRADE_UPDATE events → ketika limit order fill terdeteksi → BARU set position TP/SL.
- **Nullable TP/SL:** Jika Gemini extract signal tanpa TP (take_profit_levels null/empty) → hanya set SL. Jika tanpa SL (stop_loss null) → hanya set TP. Jika keduanya null → execute order tanpa TP/SL (pure admin-driven management).
- **At order placement (Binance):** Tepat 1 Take-Profit position order pada level TP TERAKHIR (tertinggi untuk LONG, terendah untuk SHORT) + 1 Stop-Loss position order. Ini adalah safety net — jika bot offline, posisi tetap ter-close di final TP atau SL.
- **Mid-trade management (TP partial + SL+):** Hanya terjadi ketika admin mengirim pesan (misal: "hit TP1", "set SL+", screenshot profit). Gemini mengklasifikasikan pesan → Trade_Engine mengeksekusi partial close atau modifikasi SL. Persentase close ditentukan oleh AI.
- **Price_Watcher:** Memonitor harga untuk deteksi SL hit + mengirim alert. JUGA subscribe ke Binance User Data Stream untuk mendeteksi limit order fill events dan trigger TP/SL placement. TIDAK auto-execute TP partial atau SL+. Semua trade action didorong oleh pesan Telegram dari admin.

Seluruh aplikasi berjalan di dalam Docker containers (app + postgres) menggunakan docker-compose. Semua trade menggunakan CROSS margin mode dengan leverage tetap berdasarkan pair (bukan dari signal), di-cap oleh max leverage pair di Binance.

**Position Sizing Strategy:**
- Fixed margin per trade = TRADE_MARGIN_PERCENT% of account balance (default 1%)
- Position size = (balance * TRADE_MARGIN_PERCENT / 100) * leverage / entry_price
- High risk signals: position size = (balance * TRADE_MARGIN_PERCENT / 100 * high_risk_multiplier) * leverage / entry_price

**Binance Compliance:**
- Tick size compliance: semua harga di-round ke tick size pair
- Step size compliance: semua quantity di-round ke step size (lot size) pair
- Minimum notional compliance: jika trade size < min notional, gunakan min notional
- Max leverage capping: actual_leverage = min(configured_leverage, pair_max_leverage)

Database menggunakan **3 tabel saja**: `messages`, `running_positions`, `modification_logs`. Tabel `messages` adalah single merged table — INSERT on arrival, UPDATE after Gemini. Tidak ada tabel terpisah untuk raw_messages atau gemini_responses. `allowed_running` disimpan **in-memory only** (Python set di Position_Manager), TIDAK di-persist ke database.

## Glossary

- **Signal_Listener**: Modul yang terhubung ke Telegram menggunakan user account (Telethon) untuk membaca pesan dari multiple grup premium dan forum topics, termasuk reply chain context dan message edit events
- **Signal_Parser**: Modul yang menggunakan Google Gemini AI API dengan single-call processing untuk mengekstrak data trading DAN mengklasifikasikan action dalam satu request. Mengirim teks, image, reply context, dan position context ke Gemini, lalu menerima response berisi extracted data + action decision sekaligus
- **Trade_Plan**: Struktur data yang berisi pair, direction, entry prices, take profit levels, stop loss, order type, dan action decision dari Gemini
- **Trade_Engine**: Modul yang mengeksekusi order di Binance Futures berdasarkan keputusan action dari Gemini AI, termasuk market order, limit order, modify SL, close partial, close full, cancel order, dan reverse position. Trade_Engine menentukan leverage berdasarkan pair menggunakan fixed rules (di-cap oleh max leverage Binance), memastikan tick size/step size/min notional compliance
- **Position_Manager**: Modul yang mengelola state posisi yang sedang berjalan (running positions) secara in-memory, termasuk tracking pair, direction, entry price, dan pending limit orders. `allowed_running` di-track in-memory only (Python set), tidak di-persist ke database
- **Price_Watcher**: Modul yang memonitor harga real-time menggunakan Binance Futures WebSocket untuk deteksi SL hit dan mengirim alert. Juga subscribe ke Binance User Data Stream untuk mendeteksi limit order fill events dan trigger penempatan position TP/SL. Price_Watcher TIDAK mengeksekusi trade action lainnya — semua eksekusi didorong oleh pesan admin melalui Telegram
- **Alert_Service**: Modul yang mengirim notifikasi via WhatsApp menggunakan WuzAPI endpoint
- **Database**: PostgreSQL instance yang berjalan di Docker container dengan 3 tabel: `messages` (merged raw message + Gemini response), `running_positions` (posisi aktif untuk recovery), dan `modification_logs` (history modifikasi posisi)
- **Messages_Table**: Single merged table yang menyimpan raw message DAN Gemini response dalam satu row. INSERT on arrival (text, message_id, group_id, topic_id, received_at), UPDATE after Gemini (extracted_data JSON, gemini_action, processed_at)
- **Message_Context**: Kumpulan data kontekstual yang dikirim ke Gemini bersama pesan baru, meliputi: history 10 pesan terakhir, running positions, running pairs, closed today, dan allowed running pairs
- **Reply_Chain**: Mekanisme Telegram dimana pesan baru me-reply pesan sebelumnya, membentuk rangkaian informasi signal yang lengkap (misal: teks signal → reply dengan chart image)
- **Tag_Signal**: Prefix dalam pesan Caracrypto: [OPEN] dan [CLOSED] berarti buka posisi baru (EXECUTE), [CANCEL] berarti batalkan order
- **Market_Order**: Order yang dieksekusi langsung pada harga pasar saat ini, ditandai kata "NOW" atau "entry NOW" dalam pesan
- **Limit_Order**: Order yang ditempatkan pada harga tertentu dari chart image, ditandai kata "antri", "limit", "kuning", atau "tunggu kuning"
- **Gemini_Action**: Keputusan dari Gemini AI berupa salah satu dari: "new_signal", "update_sl", "set_sl_breakeven", "tp_partial", "cancel", "reverse", "re_entry", "cutloss", atau "skip"
- **WuzAPI**: REST API endpoint untuk mengirim pesan WhatsApp di https://wuzapi.paulus-lestyo.my.id
- **Forum_Topic**: Thread spesifik dalam Telegram group yang menggunakan fitur forum/topics
- **Running_Position**: Posisi yang sedang aktif di Binance Futures, di-track oleh Position_Manager
- **Allowed_Running**: Pair yang sudah disebutkan admin tapi belum ada trade plan lengkap (menunggu chart image). Disimpan in-memory only (Python set), tidak di-persist ke database
- **SL_Breakeven**: Memindahkan stop loss ke harga entry (breakeven), sering disebut "SL+" atau "set SL+". Hanya dieksekusi ketika admin mengirim pesan yang mengindikasikan SL+ (bukan otomatis)
- **High_Risk_Signal**: Signal yang ditandai "high risk" atau "buat yg berani2 aja", menggunakan high_risk_multiplier (default 0.5) dikalikan margin normal
- **Cross_Margin**: Mode margin dimana seluruh available balance di akun futures digunakan sebagai collateral untuk semua posisi. Sistem ini SELALU menggunakan Cross Margin untuk semua trade (tidak pernah Isolated Margin)
- **Leverage_Rules**: Aturan leverage tetap berdasarkan pair: default x50 untuk semua pair, BTCUSDT = x125, ETHUSDT = x100. Leverage TIDAK diekstrak dari pesan signal. Actual leverage di-cap oleh max leverage pair di Binance: actual_leverage = min(configured_leverage, pair_max_leverage)
- **Message_Edit**: Event Telegram ketika admin mengedit pesan yang sudah dikirim. Sistem menangani edit berdasarkan perubahan tag: [OPEN]→[CLOSED] = skip, →[CANCEL] = process as cancel, lainnya = update text only
- **Final_TP**: Level take-profit TERAKHIR (tertinggi untuk LONG, terendah untuk SHORT) dari daftar TP levels yang diekstrak Gemini. Hanya level ini yang ditempatkan sebagai position TP order di Binance sebagai safety net
- **Position_TP_SL**: TP dan SL yang di-attach langsung ke posisi (bukan open order terpisah). Menggunakan type=TAKE_PROFIT_MARKET untuk TP dan type=STOP_MARKET untuk SL, dengan closePosition=true. Hanya bisa di-set SETELAH posisi terisi (filled)
- **User_Data_Stream**: Binance WebSocket stream yang mengirim real-time account events termasuk ORDER_TRADE_UPDATE. Digunakan untuk mendeteksi kapan limit order terisi, sehingga position TP/SL bisa ditempatkan setelah fill
- **TP_Partial**: Aksi menutup sebagian posisi saat admin mengirim pesan yang mengindikasikan TP level tertentu tercapai (misal "hit TP1", screenshot profit). Persentase close ditentukan oleh AI, bukan hardcoded
- **Admin_Driven_Execution**: Prinsip bahwa semua mid-trade action (TP partial, SL+) hanya dieksekusi berdasarkan pesan admin di Telegram, bukan otomatis oleh Price_Watcher
- **Tick_Size**: Presisi harga minimum (price precision) untuk suatu trading pair di Binance. Semua harga order (entry, TP, SL) harus di-round ke kelipatan tick size. Didapat dari Binance exchange info endpoint. Contoh: BTCUSDT tick_size = 0.10, artinya harga harus kelipatan 0.10
- **Step_Size**: Presisi quantity minimum (lot size precision) untuk suatu trading pair di Binance. Semua order quantity harus di-round ke kelipatan step size. Didapat dari Binance exchange info endpoint. Contoh: BTCUSDT step_size = 0.001, artinya quantity harus kelipatan 0.001
- **Min_Notional**: Nilai minimum order (quantity × price) yang diizinkan Binance untuk suatu pair. Jika calculated trade size kurang dari min notional, quantity harus dinaikkan agar memenuhi minimum. Contoh: min_notional = 5 USDT
- **Max_Leverage_Cap**: Leverage maksimum yang diizinkan Binance untuk suatu pair. Jika configured leverage (dari LEVERAGE_MAP) melebihi max leverage pair, gunakan max leverage pair. Formula: actual_leverage = min(configured_leverage, pair_max_leverage)
- **Exchange_Info_Cache**: Cache lokal dari Binance exchange info (tick size, step size, min notional, max leverage per pair). Di-refresh setiap 24 jam atau saat startup
- **Trade_Margin_Percent**: Persentase balance yang digunakan sebagai margin per trade (default 1%). Configurable via environment variable TRADE_MARGIN_PERCENT

## Requirements

### Requirement 1: Telegram Signal Listening with Reply Chain Context

**User Story:** As a trader, I want the system to read trading signals from Caracrypto Telegram group including reply chain context and attached images, so that multi-part signals are captured completely.

#### Acceptance Criteria

1. WHEN the Signal_Listener starts, THE Signal_Listener SHALL connect to Telegram using user account credentials (api_id, api_hash, phone_number)
2. WHILE the Signal_Listener is connected, THE Signal_Listener SHALL listen for new messages from all configured group IDs in the TELEGRAM_GROUPS list
3. WHEN a new message is received from a configured group, THE Signal_Listener SHALL check if the message belongs to a monitored forum topic as defined in TELEGRAM_FORUM_TOPICS configuration
4. WHEN a message matches both the configured group AND the configured forum topic, THE Signal_Listener SHALL forward the message to the Signal_Parser
5. WHEN a message is from a configured group that has no forum topic filter defined, THE Signal_Listener SHALL forward all messages from that group to the Signal_Parser
6. WHEN a message is a reply to another message, THE Signal_Listener SHALL include the replied-to message text (reply_text) and whether the replied-to message has an image (reply_has_img) in the forwarded data
7. WHEN a message contains an attached image, THE Signal_Listener SHALL download the image bytes and include the image data in the forwarded message
8. WHEN a replied-to message contains an image, THE Signal_Listener SHALL download that reply image bytes and include the reply image data in the forwarded message
9. IF the Signal_Listener loses connection to Telegram, THEN THE Signal_Listener SHALL attempt reconnection with exponential backoff up to 5 retries
10. IF the Signal_Listener fails to reconnect after 5 retries, THEN THE Alert_Service SHALL send a WhatsApp notification indicating connection failure

### Requirement 2: Single-Call Signal Processing — Extract and Classify in One Gemini Request

**User Story:** As a trader, I want the system to save every message to the database immediately on arrival, then send a single Gemini call that extracts data AND classifies the action simultaneously, and update the same row with the full Gemini response for audit, so that processing is fast, cheap, and fully auditable.

#### Acceptance Criteria

1. WHEN the Signal_Parser receives a message from the Signal_Listener, THE Database SHALL immediately INSERT a row in the messages table with fields: text, message_id, group_id, topic_id, and received_at timestamp (before any Gemini processing)
2. WHEN the message is a reply to another message, THE Database SHALL populate reply_to_message_id, reply_text (copied from the replied message row), and reply_extracted_data (copied from the replied message row) in the same INSERT or immediate UPDATE
3. WHEN the message row is saved, THE Signal_Parser SHALL send a single Gemini API call containing: message text, message image (if present), reply_text (if present), reply_image (if present), and Message_Context (running positions, running pairs, closed today, allowed running, last 10 messages history)
4. WHEN sending the single Gemini call, THE Signal_Parser SHALL instruct Gemini to return BOTH extracted data (pair, direction, entry_price, take_profit_levels, stop_loss, order_type, risk_level) AND action classification (new_signal, update_sl, set_sl_breakeven, tp_partial, cancel, reverse, re_entry, cutloss, or skip) in one structured JSON response
5. WHEN Gemini returns a response, THE Database SHALL UPDATE the same message row with: extracted_data (full JSON response), gemini_action (denormalized action string), and processed_at timestamp, regardless of whether the action is "skip" or a trading signal
6. WHEN Gemini classifies the action as "new_signal", THE Signal_Parser SHALL use the extracted pair, direction, entry_price, take_profit_levels, stop_loss, order_type, and risk_level from the same response
7. WHEN the message text contains "NOW" or "entry NOW", THE Signal_Parser SHALL instruct Gemini to extract the order type as market order
8. WHEN the message text contains "antri", "limit", "kuning", or "tunggu kuning", THE Signal_Parser SHALL instruct Gemini to extract the order type as limit order
9. WHEN Gemini classifies the action as "update_sl", THE Signal_Parser SHALL use the extracted pair and new stop loss price from the same response
10. WHEN Gemini classifies the action as "set_sl_breakeven", THE Signal_Parser SHALL use the extracted pair from the response (to move SL to entry price)
11. WHEN Gemini classifies the action as "tp_partial", THE Signal_Parser SHALL use the extracted pair and the AI-determined percentage to close from the same response (percentage is decided by Gemini based on message context, not hardcoded)
12. WHEN Gemini classifies the action as "cancel", THE Signal_Parser SHALL use the extracted pair and direction from the response
13. WHEN Gemini classifies the action as "reverse", THE Signal_Parser SHALL use the extracted pair and determine new direction (opposite of current)
14. WHEN Gemini classifies the action as "re_entry", THE Signal_Parser SHALL use the extracted pair, direction, and entry parameters from the response
15. WHEN Gemini classifies the action as "cutloss", THE Signal_Parser SHALL use the extracted pair from the response
16. WHEN Gemini classifies the action as "skip", THE Signal_Parser SHALL take no trading action (the Gemini response is still saved to DB per criterion 5)
17. THE Signal_Parser SHALL NOT extract leverage from messages — leverage is determined by fixed rules in the Trade_Engine based on the trading pair
18. IF the Gemini AI API returns an error, THEN THE Signal_Parser SHALL retry the request up to 3 times with 5-second intervals
19. IF the Signal_Parser cannot parse a valid structured response from Gemini, THEN THE Signal_Parser SHALL log the message as unparseable and skip further processing
20. WHEN Gemini includes image data (current message image and reply image) in the request, THE Signal_Parser SHALL instruct Gemini to read entry, TP, and SL levels from chart lines in the images
21. WHEN an admin sends a message indicating a TP level is hit (e.g., "hit TP1", "TP1 done", profit screenshot), THE Signal_Parser SHALL instruct Gemini to classify the action as "tp_partial" and determine the appropriate close percentage based on message context
22. WHEN an admin sends a message indicating SL should be moved to breakeven (e.g., "SL+", "set SL+", "SL di entry"), THE Signal_Parser SHALL instruct Gemini to classify the action as "set_sl_breakeven"
23. WHEN Gemini classifies a "tp_partial" action, THE Signal_Parser SHALL accept the AI-determined close percentage from Gemini response without applying any hardcoded default percentage

### Requirement 3: Tag-Based Signal Recognition

**User Story:** As a trader, I want the system to correctly interpret Caracrypto tag conventions where [OPEN] and [CLOSED] both mean "open new position" and [CANCEL] means cancel, so that signals are not misinterpreted.

#### Acceptance Criteria

1. WHEN a message contains the tag "[OPEN]", THE Signal_Parser SHALL treat the message as a new signal to open a position during Gemini classification
2. WHEN a message contains the tag "[CLOSED]", THE Signal_Parser SHALL treat the message as a new signal to open a position (same behavior as [OPEN]) during Gemini classification
3. WHEN a message contains the tag "[CANCEL]", THE Signal_Parser SHALL treat the message as a cancel instruction during Gemini classification
4. WHEN a [CANCEL] message refers to a pair with a pending limit order, THE Trade_Engine SHALL delete the pending limit order
5. WHEN a [CANCEL] message refers to a pair with an already-filled position, THE Trade_Engine SHALL close the position at market price

### Requirement 4: Position Management and State Tracking

**User Story:** As a trader, I want the system to track all running positions and their states, so that modification commands (update SL, TP partial, cutloss) can be applied to the correct positions.

#### Acceptance Criteria

1. THE Position_Manager SHALL maintain a real-time list of all running positions with: pair, direction, entry price, current SL, current TP levels, and order status
2. WHEN a new position is opened on Binance, THE Position_Manager SHALL add the position to the running positions list
3. WHEN a position is fully closed (TP hit, SL hit, cutloss, or cancel), THE Position_Manager SHALL remove the position from the running positions list and add the pair to the closed today list
4. THE Position_Manager SHALL maintain an allowed_running list of pairs mentioned by admin that do not yet have a complete trade plan, stored in-memory only (Python set)
5. WHEN a pair is mentioned in a message without complete trade parameters, THE Position_Manager SHALL add the pair to the in-memory allowed_running set
6. WHEN a complete trade plan is received for a pair in the allowed_running set, THE Position_Manager SHALL remove the pair from allowed_running and execute the trade
7. THE Position_Manager SHALL provide the current state (running positions, running pairs, closed today, allowed running) to the Signal_Parser for inclusion in Message_Context during the single Gemini call
8. WHEN the system restarts, THE Position_Manager SHALL recover running_positions from the running_positions database table, and start with an empty allowed_running set (in-memory data is volatile and acceptable to lose on restart)

### Requirement 5: Trade Execution Actions

**User Story:** As a trader, I want the system to execute various trading actions on Binance Futures based on Gemini AI classification, with full Binance filter compliance (tick size, step size, min notional, max leverage), so that all signal types are handled automatically with position TP/SL as safety-net strategy and orders never fail due to filter violations.

#### Acceptance Criteria

1. WHEN a new_signal action with order type "market" is received, THE Trade_Engine SHALL place a market order on Binance Futures for the specified pair and direction
2. WHEN a new_signal action with order type "limit" is received, THE Trade_Engine SHALL place a limit order on Binance Futures at the entry price extracted from the chart image
3. WHEN placing a new order, THE Trade_Engine SHALL set the margin mode to CROSS for the trading pair
4. WHEN placing a new order, THE Trade_Engine SHALL query Binance for the pair's maximum allowed leverage and set leverage to min(configured_leverage, pair_max_leverage), where configured_leverage comes from LEVERAGE_MAP (x125 for BTCUSDT, x100 for ETHUSDT, x50 for all other pairs)
5. THE Trade_Engine SHALL place TP and SL as POSITION TP/SL orders (type=TAKE_PROFIT_MARKET for TP, type=STOP_MARKET for SL, with closePosition=true) attached to the position, NOT as separate conditional open orders
6. WHEN a market order is filled, THE Trade_Engine SHALL immediately place the position TP/SL orders (since market orders fill instantly, TP/SL can be set right after)
7. WHEN a limit order is placed, THE Trade_Engine SHALL NOT place position TP/SL orders immediately (position TP/SL can only be set after the position is filled)
8. WHEN a limit order fill is detected (via Binance User Data Stream ORDER_TRADE_UPDATE event), THE Trade_Engine SHALL place the position TP/SL orders for the newly filled position
9. THE Trade_Engine SHALL place exactly 1 take-profit position order at the LAST TP level (highest price for LONG direction, lowest price for SHORT direction) from the Gemini-extracted take_profit_levels list
10. THE Trade_Engine SHALL place exactly 1 stop-loss position order at the stop_loss price from the Gemini-extracted data
11. THE Trade_Engine SHALL NOT place multiple TP orders — only the single final TP level is placed as a position safety-net order on Binance
12. IF the Gemini-extracted take_profit_levels is null or empty, THEN THE Trade_Engine SHALL skip placing the TP position order (only set SL if stop_loss is available)
13. IF the Gemini-extracted stop_loss is null, THEN THE Trade_Engine SHALL skip placing the SL position order (only set TP if take_profit_levels is available)
14. IF both take_profit_levels and stop_loss are null, THEN THE Trade_Engine SHALL execute the order without any position TP/SL (pure admin-driven management via Telegram messages)
15. WHEN an update_sl action is received, THE Trade_Engine SHALL modify the stop loss of the existing position for the specified pair to the new SL price
16. WHEN a set_sl_breakeven action is received, THE Trade_Engine SHALL move the stop loss of the existing position to the entry price
17. WHEN a tp_partial action is received, THE Trade_Engine SHALL close the AI-determined percentage of the position at market price
18. WHEN a cutloss action is received, THE Trade_Engine SHALL close the entire position for the specified pair at market price
19. WHEN a cancel action is received for a pending limit order, THE Trade_Engine SHALL delete the pending limit order from Binance
20. WHEN a cancel action is received for a filled position, THE Trade_Engine SHALL close the position at market price
21. WHEN a reverse action is received, THE Trade_Engine SHALL close the current position and open a new position in the opposite direction with the same leverage rule applied
22. WHEN a re_entry action is received, THE Trade_Engine SHALL open a new position for the specified pair using the provided entry parameters with the correct leverage for that pair
23. IF the Binance Futures API returns an error during any trade action, THEN THE Trade_Engine SHALL log the error and send a WhatsApp alert via the Alert_Service
24. THE Trade_Engine SHALL NOT contain any manual order type logic, fallback decision code, or leverage extraction logic
25. WHEN a tp_partial action is executed, THE Trade_Engine SHALL update the TP position order on Binance to reflect the reduced position quantity (cancel existing TP order and place new one with reduced quantity)
26. WHEN placing any order, THE Trade_Engine SHALL query Binance exchange info to get the Tick_Size for the trading pair and round all prices (entry_price, take_profit, stop_loss) to the nearest valid tick size
27. WHEN placing any order, THE Trade_Engine SHALL query Binance exchange info to get the Step_Size for the trading pair and round all quantities to the nearest valid step size
28. WHEN the calculated order value (quantity × price) is less than the Min_Notional for the trading pair, THE Trade_Engine SHALL increase the quantity to meet the minimum notional requirement using formula: quantity = min_notional / price (rounded to step size)
29. THE Trade_Engine SHALL cache Binance exchange info (tick size, step size, min notional per pair) and refresh the cache every 24 hours or on application startup
30. THE Trade_Engine SHALL cache the maximum allowed leverage per pair from Binance and refresh the cache every 24 hours or on application startup

### Requirement 6: Position Sizing with Fixed Margin Per Trade

**User Story:** As a trader, I want the system to use a fixed percentage of my account balance as margin per trade (configurable), so that position sizing is consistent and predictable regardless of the pair being traded.

#### Acceptance Criteria

1. WHEN Gemini AI extracts a signal as normal risk, THE Trade_Engine SHALL calculate position size using formula: quantity = (balance * TRADE_MARGIN_PERCENT / 100) * leverage / entry_price
2. WHEN Gemini AI extracts a signal as high risk (indicated by phrases like "high risk" or "buat yg berani2 aja"), THE Trade_Engine SHALL calculate position size using formula: quantity = (balance * TRADE_MARGIN_PERCENT / 100 * high_risk_multiplier) * leverage / entry_price
3. THE Trade_Engine SHALL read the TRADE_MARGIN_PERCENT from environment variable (default 1)
4. THE Trade_Engine SHALL read the high_risk_multiplier from configuration (default 0.5)
5. THE Trade_Engine SHALL use the actual_leverage (after max leverage capping) in the position size calculation
6. WHEN the calculated quantity results in an order value below Min_Notional, THE Trade_Engine SHALL adjust quantity upward to meet the minimum notional requirement

### Requirement 7: Real-Time Price Monitoring, User Data Stream, and Alerting

**User Story:** As a trader, I want the system to monitor prices in real-time, detect limit order fills via Binance User Data Stream, and send alerts when important levels are reached, so that I stay informed about my positions and TP/SL is correctly placed after limit order fills.

#### Acceptance Criteria

1. WHEN a Trade_Plan has an open position, THE Price_Watcher SHALL subscribe to the Binance Futures WebSocket stream for the corresponding pair
2. WHILE subscribed to a WebSocket stream, THE Price_Watcher SHALL process price updates in real-time
3. WHEN the current price reaches the stop loss level, THE Price_Watcher SHALL send an alert via the Alert_Service indicating the SL has been hit
4. WHEN the current price reaches a take profit level (any TP level from the extracted list), THE Price_Watcher SHALL send an alert via the Alert_Service indicating the TP level has been reached
5. THE Price_Watcher SHALL NOT execute any trade actions (no partial close, no SL modification) — all trade execution is driven by admin messages through Telegram
6. THE Price_Watcher SHALL NOT close positions — Binance handles final TP/SL closure via the placed position TP/SL orders
7. IF the WebSocket connection drops, THEN THE Price_Watcher SHALL reconnect within 10 seconds
8. WHEN a Trade_Plan position is fully closed (detected via Binance order fill event or position update), THE Price_Watcher SHALL unsubscribe from the corresponding WebSocket stream and notify the Position_Manager
9. THE Price_Watcher SHALL subscribe to the Binance User Data Stream (WebSocket) to receive ORDER_TRADE_UPDATE events for the trading account
10. WHEN an ORDER_TRADE_UPDATE event indicates a limit order has been filled (order status = FILLED, order type = LIMIT), THE Price_Watcher SHALL trigger the Trade_Engine to place position TP/SL orders for the newly filled position
11. THE Price_Watcher SHALL maintain a mapping of pending limit order IDs to their associated TradeAction data (pair, take_profit_levels, stop_loss) so that TP/SL can be placed when the fill event arrives
12. WHEN a limit order is placed by the Trade_Engine, THE Price_Watcher SHALL register the order ID and associated TP/SL parameters for fill detection

### Requirement 8: WhatsApp Alert Notifications

**User Story:** As a trader, I want to receive WhatsApp notifications for important trading events, so that I stay informed about my positions.

#### Acceptance Criteria

1. WHEN the Alert_Service needs to send a notification, THE Alert_Service SHALL send a POST request to https://wuzapi.paulus-lestyo.my.id/chat/send/text
2. THE Alert_Service SHALL include headers: accept: application/json, token: abc, Content-Type: application/json
3. THE Alert_Service SHALL send the message body in format: {"Phone":"6281239466830","Body":"<message_text>"}
4. WHEN a market order is executed, THE Alert_Service SHALL send a notification containing pair, direction, executed price, and order type
5. WHEN a limit order is placed, THE Alert_Service SHALL send a notification containing pair, direction, limit price, and order type
6. WHEN a position modification is executed (update SL, SL breakeven, TP partial, cutloss, reverse), THE Alert_Service SHALL send a notification containing pair, action type, and relevant details
7. WHEN a take profit level is detected by Price_Watcher, THE Alert_Service SHALL send a notification containing pair and the TP level reached
8. WHEN a stop loss hit is detected by Price_Watcher, THE Alert_Service SHALL send a notification containing pair and the SL level
9. IF the WuzAPI endpoint returns an error, THEN THE Alert_Service SHALL log the error and retry up to 3 times with 10-second intervals

### Requirement 9: Risk Management

**User Story:** As a trader, I want the system to enforce risk management rules, so that I don't lose more than acceptable limits.

#### Acceptance Criteria

1. THE Trade_Engine SHALL enforce a configurable maximum number of concurrent open positions
2. THE Trade_Engine SHALL enforce a configurable daily loss limit as a percentage of account balance
3. WHEN the daily loss limit is reached, THE Trade_Engine SHALL stop opening new positions and send a WhatsApp alert
4. WHEN the maximum concurrent positions limit is reached, THE Trade_Engine SHALL queue new Trade_Plans and send a WhatsApp alert
5. IF the account balance is insufficient for a new order (margin required exceeds available balance), THEN THE Trade_Engine SHALL skip the Trade_Plan and send a WhatsApp alert

### Requirement 10: Database Storage

**User Story:** As a trader, I want all messages, Gemini responses, position states, and modification history stored persistently in a simplified 3-table schema, so that I can track and audit trading activity and chat history is always preserved.

#### Acceptance Criteria

1. THE Database SHALL run as a PostgreSQL instance inside a Docker container managed by docker-compose
2. THE Database SHALL use exactly 3 tables: messages, running_positions, and modification_logs
3. WHEN a message is received from Telegram, THE Database SHALL immediately INSERT a row in the messages table with fields: text, message_id, group_id, topic_id, and received_at timestamp (before Gemini processing begins)
4. WHEN the received message is a reply, THE Database SHALL populate reply_to_message_id (Telegram message ID of the replied message), reply_text (copied from the replied message row), and reply_extracted_data (copied from the replied message row's extracted_data field)
5. WHEN Gemini returns a response, THE Database SHALL UPDATE the same message row with: extracted_data (full Gemini response as JSON), gemini_action (denormalized action string for quick filtering), and processed_at timestamp
6. THE messages table SHALL store the following fields: id, message_id, group_id, topic_id, text, extracted_data (JSON), reply_to_message_id, reply_text, reply_extracted_data (JSON), gemini_action, received_at, and processed_at
7. THE running_positions table SHALL store active positions with fields: id, pair, direction, entry_price, current_sl, tp_levels (JSON), leverage, order_id, quantity, message_id (FK to messages), and opened_at
8. WHEN a position modification occurs (update SL, TP partial, cutloss, reverse), THE Database SHALL INSERT a row in the modification_logs table with: pair, action_type, details (JSON), message_id (FK to messages), and timestamp
9. THE Database SHALL store running positions state in the running_positions table for recovery after system restart
10. THE Database SHALL NOT store allowed_running data — allowed_running is managed in-memory only by Position_Manager

### Requirement 11: Message Edit Handling

**User Story:** As a trader, I want the system to correctly handle Telegram message edit events, so that cancel edits are processed as actionable signals while tag-change edits (OPEN→CLOSED) are safely ignored.

#### Acceptance Criteria

1. WHILE the Signal_Listener is connected, THE Signal_Listener SHALL listen for message edit events in addition to new message events from configured groups and forum topics
2. WHEN a message edit event is received, THE Signal_Listener SHALL look up the existing message row in the messages table by message_id and group_id
3. WHEN an edit changes a message from [OPEN] tag to [CLOSED] tag, THE Signal_Listener SHALL update the text field in the existing message row and skip further processing (the trade was already executed when the original [OPEN] message was received)
4. WHEN an edit changes a message to contain [CANCEL] tag (from [OPEN] or [CLOSED]), THE Signal_Listener SHALL update the text field in the existing message row and forward the edited message for Gemini processing as a cancel signal
5. WHEN an edit does not match the [OPEN]→[CLOSED] or →[CANCEL] patterns, THE Signal_Listener SHALL update the text field in the existing message row without re-processing through Gemini
6. IF a message edit event refers to a message_id that does not exist in the messages table, THEN THE Signal_Listener SHALL ignore the edit event

### Requirement 12: System Configuration and Deployment

**User Story:** As a trader, I want to configure the system through environment variables and deploy it using Docker, so that I can adjust settings and run the system easily.

#### Acceptance Criteria

1. THE application SHALL run inside a Docker container defined in a docker-compose configuration alongside the PostgreSQL container
2. THE Signal_Listener SHALL read Telegram credentials from environment variables: TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE
3. THE Signal_Listener SHALL read the target group IDs from a Python configuration as a list of integers (TELEGRAM_GROUPS)
4. THE Signal_Listener SHALL read forum topic filters from a Python configuration as a dictionary mapping group IDs to lists of topic IDs (TELEGRAM_FORUM_TOPICS)
5. THE Signal_Parser SHALL read the Gemini AI API key from environment variable GEMINI_API_KEY and model name from GEMINI_MODEL
6. THE Trade_Engine SHALL read Binance API credentials from environment variables: BINANCE_API_KEY, BINANCE_API_SECRET
7. THE Trade_Engine SHALL read risk management parameters (max_concurrent_positions, daily_loss_limit_percent, high_risk_multiplier) from configuration
8. THE Trade_Engine SHALL read leverage configuration from a fixed mapping: {"BTCUSDT": 125, "ETHUSDT": 100, "default": 50}, with actual leverage capped by the pair's maximum allowed leverage on Binance
9. THE Trade_Engine SHALL set margin mode to CROSS for all pairs before placing orders
10. THE Database SHALL read PostgreSQL connection parameters from the docker-compose internal network configuration
11. THE Alert_Service SHALL read WuzAPI token and target phone number from configuration
12. THE docker-compose configuration SHALL define two services: app (application container) and postgres (PostgreSQL 16 container)
13. THE Trade_Engine SHALL read TRADE_MARGIN_PERCENT from environment variable (default value: 1), representing the percentage of account balance used as margin per trade
14. THE Trade_Engine SHALL cache exchange info (tick size, step size, min notional, max leverage) from Binance on startup and refresh every 24 hours
