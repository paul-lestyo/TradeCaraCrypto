# Tujuan
# Smoke test user-data websocket Binance Futures (ORDER_TRADE_UPDATE).
# Caller
# Operator/developer lokal.
# Dependensi
# python-binance, aiohttp, python-dotenv.

from __future__ import annotations

import argparse
import asyncio
import pathlib
import sys
from urllib.parse import urlparse

import aiohttp
from dotenv import load_dotenv

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from binance.um_futures import UMFutures
except Exception:
    from binance.client import Client as UMFutures

from CaraCrypto.config import load_config


def _create_client(cfg):
    params = UMFutures.__init__.__code__.co_varnames
    if "key" in params and "secret" in params:
        return UMFutures(key=cfg.binance.api_key, secret=cfg.binance.api_secret, base_url=cfg.binance.futures_base_url)
    kwargs = {"api_key": cfg.binance.api_key, "api_secret": cfg.binance.api_secret}
    if "testnet" in params and cfg.binance.env == "testnet":
        kwargs["testnet"] = True
    if "demo" in params and cfg.binance.env == "demo":
        kwargs["demo"] = True
    c = UMFutures(**kwargs)
    if cfg.binance.env != "mainnet":
        c.FUTURES_URL = f"{cfg.binance.futures_base_url}/fapi"
        c.FUTURES_DATA_URL = f"{cfg.binance.futures_base_url}/futures/data"
    return c


def _create_listen_key(client) -> str | None:
    for method_name in ("new_listen_key", "futures_stream_get_listen_key", "futures_get_listen_key"):
        fn = getattr(client, method_name, None)
        if not callable(fn):
            continue
        try:
            resp = fn()
        except Exception:
            continue
        if isinstance(resp, dict) and resp.get("listenKey"):
            return str(resp["listenKey"])
        if isinstance(resp, str) and resp.strip():
            return resp.strip()
    return None


def _keepalive(client, listen_key: str) -> None:
    for method_name in ("renew_listen_key", "futures_stream_keepalive", "futures_keepalive"):
        fn = getattr(client, method_name, None)
        if not callable(fn):
            continue
        try:
            fn(listenKey=listen_key)
            return
        except TypeError:
            try:
                fn()
                return
            except Exception:
                continue
        except Exception:
            continue


def _close(client, listen_key: str) -> None:
    for method_name in ("close_listen_key", "futures_stream_close", "futures_close_listen_key"):
        fn = getattr(client, method_name, None)
        if not callable(fn):
            continue
        try:
            fn(listenKey=listen_key)
            return
        except TypeError:
            try:
                fn()
                return
            except Exception:
                continue
        except Exception:
            continue


def _build_ws_url(client, listen_key: str) -> str:
    base = getattr(client, "base_url", None) or getattr(client, "BASE_URL", None) or getattr(client, "FUTURES_URL", None)
    host = urlparse(base).netloc.lower() if isinstance(base, str) else ""
    if "testnet" in host or "binancefuture.com" in host:
        return f"wss://stream.binancefuture.com/ws/{listen_key}"
    return f"wss://fstream.binance.com/ws/{listen_key}"


async def _run(duration_sec: int) -> None:
    load_dotenv()
    cfg = load_config()
    client = _create_client(cfg)
    listen_key = _create_listen_key(client)
    if not listen_key:
        raise RuntimeError("cannot create futures listen key from client")
    ws_url = _build_ws_url(client, listen_key)
    print(f"[stream-test] env={cfg.binance.env} ws={ws_url}")

    async def keepalive_loop():
        while True:
            await asyncio.sleep(30 * 60)
            _keepalive(client, listen_key)

    keepalive_task = asyncio.create_task(keepalive_loop())
    try:
        timeout = aiohttp.ClientTimeout(total=duration_sec + 10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(ws_url, heartbeat=20) as ws:
                end_at = asyncio.get_event_loop().time() + duration_sec
                while asyncio.get_event_loop().time() < end_at:
                    try:
                        msg = await ws.receive(timeout=1.0)
                    except TimeoutError:
                        continue
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    payload = msg.json()
                    if payload.get("e") == "ORDER_TRADE_UPDATE":
                        o = payload.get("o", {})
                        status = str(o.get("X"))
                        exec_type = str(o.get("x"))
                        side = str(o.get("S"))
                        reduce_only = _to_bool(o.get("R"))
                        close_position = _to_bool(o.get("cp"))
                        phase = _classify_order_phase(status, exec_type, reduce_only, close_position)
                        print(
                            f"[stream-test] {phase} "
                            f"pair={o.get('s')} id={o.get('i')} type={o.get('o')} "
                            f"side={side} status={status} exec={exec_type} "
                            f"reduce_only={reduce_only} close_position={close_position} "
                            f"filled={o.get('z')} last_fill={o.get('l')}"
                        )
    finally:
        keepalive_task.cancel()
        _close(client, listen_key)


def _classify_order_phase(status: str, exec_type: str, reduce_only: bool, close_position: bool) -> str:
    if status == "NEW":
        if reduce_only or close_position:
            return "ORDER_CLOSE_NEW"
        return "ORDER_OPEN_NEW"
    if status == "PARTIALLY_FILLED":
        if reduce_only or close_position:
            return "ORDER_CLOSE_PARTIAL_FILL"
        return "PARTIAL_FILL"
    if status == "FILLED":
        if reduce_only or close_position:
            return "ORDER_CLOSE_FILL"
        return "FILL"
    if status == "CANCELED":
        return "CANCEL"
    if status == "EXPIRED":
        return "EXPIRE"
    if status == "REJECTED" or exec_type == "REJECTED":
        return "REJECT"
    return "UPDATE"


def _to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    if value is None:
        return False
    return bool(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Test Binance Futures user-data websocket stream")
    parser.add_argument("--duration-sec", type=int, default=90)
    args = parser.parse_args()
    asyncio.run(_run(args.duration_sec))


if __name__ == "__main__":
    main()
