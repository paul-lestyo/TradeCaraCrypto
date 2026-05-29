# Tujuan
# Diagnostic ambil saldo Futures USDT-M dari .env, mencoba seluruh endpoint
# yang dipakai TradeEngine._get_account_balance dan menampilkan respons mentah
# supaya error -2015 (key/IP/permission) bisa dipinpoint cepat.
# Caller
# Operator/developer lokal: `python -m scripts.check_binance_balance`.
# Dependensi
# python-binance, python-dotenv (sudah ada di requirements.txt).
# Main Functions
# `main()` mencoba 4 endpoint, print ringkasan + raw response.
# Side Effects
# HTTP GET ke Binance (mainnet/testnet) dan logging timestamp offset.

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import time
import traceback
from decimal import Decimal
from typing import Any, Optional

# Pastikan akar repo ada di sys.path saat script dipanggil langsung
# (`python scripts/check_binance_balance.py`), bukan via `python -m`.
_PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from binance.client import Client
from binance.exceptions import BinanceAPIException
from dotenv import load_dotenv

from CaraCrypto.config import BINANCE_ENV_DEFAULT, BINANCE_FUTURES_HOSTS


def _require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise SystemExit(f"Missing env: {name}")
    return value


def _build_client(env_mode: str) -> Client:
    key = _require_env("BINANCE_API_KEY")
    secret = _require_env("BINANCE_API_SECRET")
    client = Client(key, secret)
    base_url = BINANCE_FUTURES_HOSTS.get(env_mode, BINANCE_FUTURES_HOSTS[BINANCE_ENV_DEFAULT])
    if env_mode != "mainnet":
        client.FUTURES_URL = f"{base_url}/fapi"
        client.FUTURES_DATA_URL = f"{base_url}/futures/data"
    server_time = client.futures_time()["serverTime"]
    client.timestamp_offset = int(server_time - int(time.time() * 1000))
    return client


def _extract_usdt(response: Any) -> Optional[Decimal]:
    if isinstance(response, list):
        for item in response:
            if isinstance(item, dict) and item.get("asset") == "USDT":
                for key in ("availableBalance", "balance", "walletBalance", "crossWalletBalance"):
                    if key in item:
                        try:
                            return Decimal(str(item[key]))
                        except Exception:
                            continue
    if isinstance(response, dict):
        for key in ("availableBalance", "totalWalletBalance", "walletBalance"):
            if key in response:
                try:
                    return Decimal(str(response[key]))
                except Exception:
                    continue
        assets = response.get("assets")
        if isinstance(assets, list):
            return _extract_usdt(assets)
    return None


def _try_method(client: Client, label: str, fn) -> None:
    print(f"\n--- {label} ---")
    try:
        response = fn()
    except BinanceAPIException as exc:
        print(f"BinanceAPIException code={exc.code} message={exc.message}")
        if exc.code == -2015:
            print(
                "hint: -2015 biasanya berarti (a) API key/secret salah,"
                " (b) IP runner belum di-whitelist di pengaturan API key,"
                " atau (c) permission Futures belum diaktifkan untuk key ini."
            )
        return
    except Exception as exc:
        print(f"Unexpected error: {exc}")
        traceback.print_exc(limit=4)
        return
    try:
        preview = json.dumps(response, indent=2, default=str)
    except Exception:
        preview = repr(response)
    if len(preview) > 1500:
        preview = preview[:1500] + "... [truncated]"
    print("raw_response:", preview)
    parsed = _extract_usdt(response)
    print("parsed_usdt_balance:", parsed)


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose Binance Futures USDT balance fetching.")
    parser.add_argument(
        "--env-mode",
        choices=tuple(BINANCE_FUTURES_HOSTS.keys()),
        default=None,
        help="Override host Futures (default: ikut BINANCE_ENV di .env, fallback mainnet).",
    )
    parser.add_argument(
        "--dotenv",
        default=".env",
        help="Path file .env yang dimuat (default .env di cwd).",
    )
    args = parser.parse_args()

    load_dotenv(args.dotenv)
    env_mode = (args.env_mode or os.getenv("BINANCE_ENV") or BINANCE_ENV_DEFAULT).strip().lower()
    if env_mode not in BINANCE_FUTURES_HOSTS:
        print(f"unsupported env={env_mode}, falling back to {BINANCE_ENV_DEFAULT}")
        env_mode = BINANCE_ENV_DEFAULT
    print(f"env_mode={env_mode} base_url={BINANCE_FUTURES_HOSTS[env_mode]}")

    try:
        client = _build_client(env_mode)
    except SystemExit as exc:
        print(str(exc))
        return 2
    except BinanceAPIException as exc:
        print(f"futures_time() failed: code={exc.code} message={exc.message}")
        return 1

    print("timestamp_offset_ms:", getattr(client, "timestamp_offset", None))

    _try_method(client, "futures_account_balance()", client.futures_account_balance)
    _try_method(
        client,
        "futures_account_balance(asset='USDT') (signed)",
        lambda: client.futures_account_balance(asset="USDT"),
    )
    _try_method(client, "futures_account()", client.futures_account)

    return 0


if __name__ == "__main__":
    sys.exit(main())
