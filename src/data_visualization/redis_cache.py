from __future__ import annotations

import hashlib
import os
import pickle
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def redis_enabled() -> bool:
    # I keep Redis optional so local dashboard work still runs if Redis is down.
    return os.getenv("REDIS_ENABLED", "true").lower() in {"1", "true", "yes"}


def get_redis_client():
    if not redis_enabled():
        return None

    try:
        import redis
    except ImportError:
        return None

    password = os.getenv("REDIS_PASSWORD") or None
    client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        password=password,
        socket_connect_timeout=float(os.getenv("REDIS_CONNECT_TIMEOUT_SECONDS", "1")),
        socket_timeout=float(os.getenv("REDIS_SOCKET_TIMEOUT_SECONDS", "2")),
    )

    try:
        client.ping()
    except Exception:
        return None

    return client


def build_cache_key(namespace: str, *parts: object) -> str:
    # I hash the variable pieces so Redis keys stay short and safe.
    prefix = os.getenv("REDIS_KEY_PREFIX", "streamingetl")
    version = os.getenv("DASHBOARD_CACHE_VERSION", "v1")
    raw_key = "|".join(str(part) for part in parts)
    digest = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    return f"{prefix}:{namespace}:{version}:{digest}"


def get_cached_value(cache_key: str) -> Any | None:
    client = get_redis_client()
    if client is None:
        return None

    try:
        cached_bytes = client.get(cache_key)
        if cached_bytes is None:
            return None
        return pickle.loads(cached_bytes)
    except Exception:
        return None


def set_cached_value(cache_key: str, value: Any) -> None:
    client = get_redis_client()
    if client is None:
        return

    ttl_seconds = int(os.getenv("REDIS_CACHE_TTL_SECONDS", "300"))
    try:
        client.setex(cache_key, ttl_seconds, pickle.dumps(value))
    except Exception:
        return
