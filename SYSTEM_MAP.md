# Tujuan
# Peta sistem backend Telegram Signal Trader untuk navigasi arsitektur.
# Caller
# Developer dan agent implementasi.
# Dependensi
# requirements/design/tasks di .kiro/specs/telegram-signal-trader.
# Main Functions
# Memetakan entrypoint, service, dan storage.
# Side Effects
# Menjadi kontrak arsitektur sesi ini.

## Entry Point
- `CaraCrypto/__main__.py`: bootstrap aplikasi, loop pemrosesan message, dan helper satu-siklus `_process_one_signal` untuk integration test.

## Flow Utama
1. `signal_listener.py` menerima event Telegram dan membentuk `RawSignalMessage`.
2. `database.py` menyimpan row `messages` sebelum Gemini.
3. `context_builder.py` menyiapkan konteks (history + position state).
4. `signal_parser.py` single-call Gemini (extract + classify), lalu update row yang sama.
5. `trade_engine.py` memvalidasi aksi executable lalu mengeksekusi aksi non-skip ke Binance dengan dispatch action, risk checks, sizing, TP/SL safety order, dan queue sederhana saat limit posisi tercapai.
6. `price_watcher.py` monitor harga + user data stream untuk fill/closure setelah pipeline subscribe hanya pada order yang diterima engine.
7. `alert_service.py` kirim notifikasi WhatsApp.

## Data Access
- `database.py` mengelola 3 tabel:
  - `messages`
  - `running_positions`
  - `modification_logs`

## Konfigurasi
- `config.py`: env + static config (group/topic, leverage map, risk).

## Scripts
- `scripts/e2e_image_to_binance_testnet.py`: skenario operator untuk image signal -> TradeAction -> TradeEngine -> Binance Futures testnet.

## Modul Inti
- `config.py`: loader env + dataclass konfigurasi aplikasi.
- `models.py`: enum domain dan dataclass payload/state/result.
- `signal_listener.py`: listener Telethon untuk new message + edit event + reply/image context.
- `signal_parser.py`: Gemini single-call parser text+image, guard teks aksi current-message, hint klasifikasi, validasi action.
- `trade_engine.py`: action executor Binance-API-like client (client diinjeksikan, adapter `new_order`/`futures_create_order`, dan return status eksekusi untuk gating watcher).
- `price_watcher.py`: alert-only price watcher + handler user data stream events.
- `database.py`: SQLAlchemy async DAL 3 tabel.
- `alert_service.py`: sender notifikasi WhatsApp (WuzAPI).

## Test Map
- `tests/test_config_models.py`: property leverage map + keyword order-type.
- `tests/test_database_properties.py`: property contract storage message dan reply linkage.
- `tests/test_alert_service.py`: property format pesan alert + PnL.
- `tests/test_position_manager.py`: property konsistensi running/closed/allowed state.
- `tests/test_context_builder.py`: property kelengkapan payload context.
- `tests/test_signal_listener.py`: property filter forum-topic + klasifikasi edit.
- `tests/test_signal_parser.py`: property hint tag + skip/invalid response handling.
- `tests/test_trade_engine.py`: property leverage/final TP/reverse dispatch.
- `tests/test_price_watcher.py`: property limit fill trigger + closure handling.
- `tests/test_integration_pipeline.py`: integration flow satu pesan dari store->parse->execute.
- `tests/test_risk_management.py`: property enforcement risk-limit branches.
