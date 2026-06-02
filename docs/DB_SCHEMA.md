# Tujuan
# Kontrak schema database untuk bot trading.
# Caller
# `CaraCrypto/database.py`.
# Dependensi
# PostgreSQL 16, SQLAlchemy async.
# Main Functions
# Mendefinisikan 3 tabel wajib.
# Side Effects
# Menjadi referensi validasi perubahan data-access.

## messages
- `id` BIGSERIAL PK
- `message_id` BIGINT NOT NULL
- `group_id` BIGINT NOT NULL
- `topic_id` BIGINT NULL
- `text` TEXT NOT NULL
- `extracted_data` JSONB NULL
- `reply_to_message_id` BIGINT NULL
- `reply_text` TEXT NULL
- `reply_extracted_data` JSONB NULL
- `gemini_action` VARCHAR(64) NULL
- `received_at` TIMESTAMPTZ NOT NULL
- `processed_at` TIMESTAMPTZ NULL
- Unique key: (`message_id`, `group_id`)

## running_positions
- `id` BIGSERIAL PK
- `pair` VARCHAR(32) NOT NULL UNIQUE
- `direction` VARCHAR(16) NOT NULL
- `entry_price` NUMERIC(30, 10) NOT NULL
- `current_sl` NUMERIC(30, 10) NULL
- `tp_levels` JSONB NULL
- `leverage` INTEGER NOT NULL
- `order_id` VARCHAR(128) NOT NULL
- `quantity` NUMERIC(30, 10) NOT NULL
- `message_id` BIGINT NULL FK -> messages.id
- `opened_at` TIMESTAMPTZ NOT NULL
- `status` VARCHAR(16) NOT NULL DEFAULT `running`
- Status values: `running`, `pending`

## modification_logs
- `id` BIGSERIAL PK
- `pair` VARCHAR(32) NOT NULL
- `action_type` VARCHAR(64) NOT NULL
- `details` JSONB NULL
- `message_id` BIGINT NULL FK -> messages.id
- `timestamp` TIMESTAMPTZ NOT NULL
