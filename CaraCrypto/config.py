# Tujuan
# Mendefinisikan konfigurasi aplikasi dari env dan static map.
# Caller
# `CaraCrypto.__main__` dan service modules.
# Dependensi
# os, dataclasses.
# Main Functions
# `load_config()`.
# Side Effects
# Membaca environment variable proses.

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List

LEVERAGE_MAP: Dict[str, int] = {"BTCUSDT": 125, "ETHUSDT": 100}
DEFAULT_LEVERAGE: int = 50
MARGIN_MODE: str = "CROSS"

TELEGRAM_GROUPS: List[int] = []
TELEGRAM_FORUM_TOPICS: Dict[int, List[int]] = {}


@dataclass
class TelegramConfig:
    api_id: int
    api_hash: str
    phone: str
    groups: List[int] = field(default_factory=list)
    forum_topics: Dict[int, List[int]] = field(default_factory=dict)


@dataclass
class GeminiConfig:
    api_key: str
    model: str = "gemini-2.0-flash"


@dataclass
class BinanceConfig:
    api_key: str
    api_secret: str


@dataclass
class DatabaseConfig:
    url: str


@dataclass
class AlertConfig:
    base_url: str = "https://wuzapi.paulus-lestyo.my.id"
    token: str = "abc"
    phone: str = "120363426398056602@g.us"


@dataclass
class RiskConfig:
    max_concurrent_positions: int = 5
    max_position_size_percent: float = 10.0
    daily_loss_limit_percent: float = 5.0
    high_risk_multiplier: float = 0.5
    trade_margin_percent: float = 1.0


@dataclass
class AppConfig:
    telegram: TelegramConfig
    gemini: GeminiConfig
    binance: BinanceConfig
    database: DatabaseConfig
    alert: AlertConfig
    risk: RiskConfig


def _parse_int_list(value: str) -> List[int]:
    if not value.strip():
        return []
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def load_config() -> AppConfig:
    groups = _parse_int_list(os.getenv("TELEGRAM_GROUPS", "")) or TELEGRAM_GROUPS
    return AppConfig(
        telegram=TelegramConfig(
            api_id=int(os.getenv("TELEGRAM_API_ID", "0")),
            api_hash=os.getenv("TELEGRAM_API_HASH", ""),
            phone=os.getenv("TELEGRAM_PHONE", ""),
            groups=groups,
            forum_topics=TELEGRAM_FORUM_TOPICS,
        ),
        gemini=GeminiConfig(
            api_key=os.getenv("GEMINI_API_KEY", ""),
            model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"),
        ),
        binance=BinanceConfig(
            api_key=os.getenv("BINANCE_API_KEY", ""),
            api_secret=os.getenv("BINANCE_API_SECRET", ""),
        ),
        database=DatabaseConfig(
            url=os.getenv(
                "DATABASE_URL",
                "postgresql+asyncpg://trader:password@localhost:5432/caracrypto",
            )
        ),
        alert=AlertConfig(
            token=os.getenv("WUZAPI_TOKEN", "abc"),
            phone=os.getenv("WUZAPI_PHONE", "120363426398056602@g.us"),
        ),
        risk=RiskConfig(
            max_concurrent_positions=int(os.getenv("MAX_CONCURRENT_POSITIONS", "5")),
            max_position_size_percent=float(os.getenv("MAX_POSITION_SIZE_PERCENT", "10")),
            daily_loss_limit_percent=float(os.getenv("DAILY_LOSS_LIMIT_PERCENT", "5")),
            high_risk_multiplier=float(os.getenv("HIGH_RISK_MULTIPLIER", "0.5")),
            trade_margin_percent=float(os.getenv("TRADE_MARGIN_PERCENT", "1")),
        ),
    )
