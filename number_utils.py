"""
number_utils.py — Unified Number Normalization & OTP Extraction
================================================================
Single source of truth for:
  - Mobile number normalization (Indian + international)
  - Garbage number filtering
  - SIM number extraction from device data
  - OTP code extraction from SMS bodies
  - All shared regex patterns

Used by: beast_engine.py, stream_scanner.py, otp_web.py, otp_interceptor.py
"""
from __future__ import annotations
import re
from typing import Any

__all__ = [
    "normalize_mobile", "is_garbage", "clean_mobile",
    "extract_sim_numbers", "extract_numbers_from_messages", "extract_otp",
    "get_country_from_number", "format_phone_number",
    "OTP_WORD_PATTERN", "OTP_PATTERN", "RAW_MOBILE_PATTERN",
    "RAW_US_MOBILE_PATTERN", "RAW_KW_MOBILE_PATTERN", "RAW_INTL_PATTERN",
    "NUMBER_PATTERNS", "ACTIVATION_PATTERN",
]

# ==================== DIGIT NORMALIZATION ====================
_EASTERN_ARABIC_DIGITS = "٠١٢٣٤٥٦٧٨٩"
_PERSIAN_DIGITS = "۰۱۲۳۴۵۶۷۸۹"
_DEVANAGARI_DIGITS = "०१२३४५६७८९"
_ASCII_DIGITS = "0123456789"
_DIGIT_TRANSLATION_TABLE = str.maketrans(
    _EASTERN_ARABIC_DIGITS + _PERSIAN_DIGITS + _DEVANAGARI_DIGITS,
    _ASCII_DIGITS + _ASCII_DIGITS + _ASCII_DIGITS
)


def normalize_digits(text: Any) -> str:
    """Translate Eastern Arabic (٠-٩), Persian (۰-۹), and Devanagari (०-९) digits to standard ASCII digits 0-9."""
    if not text:
        return ""
    return str(text).translate(_DIGIT_TRANSLATION_TABLE)



# ==================== REGEX PATTERNS ====================
# OTP keyword detection (English, Hinglish, Arabic, Devanagari Hindi, Portuguese, Spanish)
OTP_WORD_PATTERN = re.compile(
    r"(?i)\b(?:otp|one[- ]?time|code|pin|passcode|verification|verify|verif|auth|secret|token|confirmation"
    r"|c[oó]digo|senha|seguran[cç]a|clave|autenticaci[oó]n|mot\s*de\s*passe)\b"
    r"|aapka\s+otp|otp\s+hai|رمز\s*التحقق|رمز\s*التأكيد|رمز\s*الدخول|كود\s*التحقق|كود\s*التفعيل"
    r"|کد\s*تأیید|کد\s*ورود|رمز\s*ورود"
    r"|ओटीपी|सत्यापन|कोड|पासवर्ड|सुरक्षा|पिन"
)

# OTP code extraction (4-8 digits)
OTP_PATTERN = re.compile(r"(?<!\d)(\d{4,8})(?!\d)|[A-Z]-(\d{6})|(\d{3}[-\s]\d{3})")

# Raw Indian mobile number (with optional 91 prefix)
RAW_MOBILE_PATTERN = re.compile(r"(?<!\d)(?:\+91|91)?([6-9]\d{9})(?!\d)")

# Raw US mobile number (NANP 10-digit with optional +1/1 prefix)
RAW_US_MOBILE_PATTERN = re.compile(r"(?<!\d)(?:\+?1[\s.-]?)?([2-9]\d{2}[\s.-]?[2-9]\d{2}[\s.-]?\d{4})(?!\d)")

# Raw Kuwait mobile number (8-digit starting with 5, 6, 9 with optional +965 prefix)
RAW_KW_MOBILE_PATTERN = re.compile(r"(?<!\d)(?:\+?965[\s.-]?)?([569]\d{7})(?!\d)")

# Raw International E.164-like candidate pattern
RAW_INTL_PATTERN = re.compile(r"(?<!\d)\+?([1-9]\d{9,14})(?!\d)")

# Jio-specific number patterns in SMS
NUMBER_PATTERNS = (
    re.compile(r"(?i)\bjio\s*(?:number|no[.]?)\s*[:=-]?\s*(?:[+]91)?([6-9]\d{9})"),
    re.compile(r"(?i)\brecharge(?:\s+now)?\s+jio\s+no[.]?\s*[:=-]?\s*(?:[+]91)?([6-9]\d{9})"),
)

# Google AI Pro activation link pattern (supports serviceactivation & partnerdash)
ACTIVATION_PATTERN = re.compile(
    r"https?://(?P<domain>(?:serviceactivation|partnerdash)[.]google(?:apis)?[.]com)/subscription/(?:new/)?"
    r"(?P<token>[A-Za-z0-9_\-=+]{40,})(?P<padding>={0,2})",
    re.IGNORECASE,
)


def build_canonical_activation_url(match: re.Match | str, domain: str | None = None) -> str:
    """Builds canonical Google activation URL preserving or standardizing domain."""
    if isinstance(match, str):
        cleaned = match.strip().rstrip(".,;!?'\"")
        m = ACTIVATION_PATTERN.search(cleaned)
        if not m:
            return cleaned
        match = m
    token = match.group("token")
    padding = match.group("padding") or ""
    dom = domain or (match.groupdict().get("domain") or "serviceactivation.google.com")
    return f"https://{dom}/subscription/new/{token}{padding}"


# Full OTP extraction patterns (ordered by specificity)
_OTP_EXTRACT_PATTERNS = [
    re.compile(r"(?i)\b(?:otp|code|pin|verification)\s*(?:is|:|\s)\s*(\d{4,8})\b"),
    re.compile(r"(?<!\d)(\d{4,8})\s+(?:is\s+(?:your|the)\s+(?:otp|code|pin|verification))", re.IGNORECASE),
    re.compile(r"(?i)(?:code|otp)\s*[:=]\s*(\d{3}[-\s]?\d{3})"),
    re.compile(r"(?i)[A-Z]-(\d{6})\s+is\s+your"),
    re.compile(r"(?i)(?:login|sign.?in|access)\s*code\s*[:=]?\s*(\d{4,8})"),
    re.compile(r"(?i)(?:aapka|apka|tumhara)\s+(?:otp|code)\s+(?:hai|he|h)\s*(\d{4,8})"),
    re.compile(r"(?i)(?:otp|code)\s+(?:bheja|send|sent)\s+(?:gaya|kiya)?\s*(\d{4,8})"),
    re.compile(r"(?i)(?:jio|myjio)\s+.*?(?:otp|code)\s*[:=]?\s*(\d{4,8})"),
    re.compile(r"(?i)(?:do\s+not\s+share|confidential).*?(\d{6})"),
    # Arabic patterns
    re.compile(r"(?:رمز\s*(?:التحقق|التأكيد|الدخول|السري)|كود\s*(?:التحقق|التفعيل))\s*[:=\s]?\s*(\d{4,8})"),
    re.compile(r"(\d{4,8})\s+(?:هو\s*رمز\s*(?:التحقق|التأكيد|الدخول))"),
    # Persian patterns
    re.compile(r"(?:کد\s*(?:تأیید|ورود|فعالسازی)|رمز\s*ورود)\s*[:=\s]?\s*(\d{4,8})"),
    re.compile(r"(\d{4,8})\s+(?:است|می\s*باشد|کد\s*تأیید)"),
    # Portuguese / Spanish patterns
    re.compile(r"(?i)\b(?:c[oó]digo\s+(?:de\s+)?(?:verifica[cç][aã]o|acesso|seguran[cç]a)|senha\s+de\s+acesso)\s*[:=\s]?\s*(\d{4,8})\b"),
    re.compile(r"(?i)\b(?:use|digite)\s+(\d{4,8})\s+(?:para\s+confirmar|para\s+verificar|como\s+c[oó]digo)\b"),
    # Hindi / Devanagari patterns
    re.compile(r"(?:ओटीपी|सत्यापन\s*कोड|लॉगिन\s*कोड|पासवर्ड)\s*[:=]?\s*(\d{4,8})"),
    re.compile(r"(\d{4,8})\s+(?:है\s+आपका\s+ओटीपी|का\s+उपयोग\s+करें)"),
    OTP_PATTERN,
]

# ==================== GARBAGE FILTER ====================
GARBAGE_SET = frozenset({
    "9999999999", "8888888888", "7777777777", "6666666666",
    "6000060000", "6000000000", "7000000000", "8000000000", "9000000000",
    "1234567890", "9876543210", "1111111111", "2222222222",
    "9999900000", "9000090000", "8000080000", "7000070000",
    "9123456789", "6123456789",
})


def is_garbage(num: str) -> bool:
    """Check if a number is garbage/test/placeholder."""
    if not num:
        return True
    s = re.sub(r"[^0-9]", "", str(num))
    if len(s) < 7:
        return True
    if s in GARBAGE_SET:
        return True
    # Strip country codes for pattern testing if present
    core = s
    if s.startswith("91") and len(s) == 12:
        core = s[2:]
    elif s.startswith("1") and len(s) == 11:
        core = s[1:]
    elif s.startswith("965") and len(s) == 11:
        core = s[3:]
    elif s.startswith("234") and len(s) == 13:
        core = s[3:]
    elif s.startswith("55") and len(s) == 13:
        core = s[2:]
    elif s.startswith("880") and len(s) == 13:
        core = s[3:]
    elif s.startswith("92") and len(s) == 12:
        core = s[2:]
    elif s.startswith("977") and len(s) == 13:
        core = s[3:]
    elif s.startswith("94") and len(s) == 11:
        core = s[2:]
    elif s.startswith("62") and 11 <= len(s) <= 14:
        core = s[2:]
    elif s.startswith("254") and len(s) == 12:
        core = s[3:]
    elif s.startswith("60") and 11 <= len(s) <= 12:
        core = s[2:]
    elif s.startswith("66") and len(s) == 11:
        core = s[2:]
    elif s.startswith("84") and len(s) == 11:
        core = s[2:]

    digits = set(core)
    if len(digits) <= 1:
        return True
    if len(core) >= 8 and len(digits) == 2:
        alt = core[0] + core[1]
        if (alt * (len(core) // 2 + 1))[:len(core)] == core:
            return True
    # 6+ consecutive same digit
    for i in range(len(core) - 5):
        if len(set(core[i:i + 6])) == 1:
            return True
    if core.endswith("00000") or core.endswith("000000"):
        return True
    # Sequential run detection (e.g. 012345678, 98765432)
    asc_seq = "01234567890123456789"
    desc_seq = "98765432109876543210"
    if len(core) >= 7 and (core in asc_seq or core in desc_seq):
        return True
    return False


def normalize_mobile(raw: Any) -> str | None:
    """
    Normalize any raw value to a clean mobile number:
      - India: 10 digits starting with 6-9 (e.g. 9876543210)
      - USA: 11 digits with country code 1 (e.g. 12125550183)
      - Kuwait: 11 digits with country code 965 (e.g. 96598765432)
    Returns normalized string or None.
    """
    if raw is None:
        return None
    raw_str = normalize_digits(str(raw)).strip()
    s = re.sub(r"[^0-9]", "", raw_str)
    if not s:
        return None

    # Nigeria format: +234 followed by 10 digits (e.g. 2348036938467)
    if s.startswith("234") and len(s) == 13:
        if not is_garbage(s):
            return s
    elif len(s) == 10 and s[0] in "789" and not s[0] in "6":
        # Could be Nigerian 10-digit — but ambiguous with Indian, skip auto-detect
        pass

    # Brazil format: +55 followed by 11 digits (e.g. 5511999998888)
    if s.startswith("55") and len(s) == 13:
        if not is_garbage(s):
            return s

    # Kuwait format: +965 followed by 8 digits (starting with 5, 6, 9)
    if s.startswith("965") and len(s) == 11 and s[3] in "569":
        if not is_garbage(s):
            return s
    elif len(s) == 8 and s[0] in "569":
        # 8-digit Kuwait national number
        kw_candidate = "965" + s
        if not is_garbage(kw_candidate):
            return kw_candidate

    # USA format: +1 followed by 10 digits (NANP: area code [2-9]XX)
    if s.startswith("1") and len(s) == 11 and s[1] in "23456789":
        if not is_garbage(s):
            return s
    elif raw_str.startswith("+1") and len(s) == 11:
        if not is_garbage(s):
            return s

    # Bangladesh format: +880 followed by 10 digits starting with 1
    if s.startswith("880") and len(s) == 13 and s[3] == "1":
        if not is_garbage(s):
            return s

    # Pakistan format: +92 followed by 10 digits starting with 3
    if s.startswith("92") and len(s) == 12 and s[2] == "3":
        if not is_garbage(s):
            return s

    # Nepal format: +977 followed by 10 digits starting with 9
    if s.startswith("977") and len(s) == 13 and s[3] == "9":
        if not is_garbage(s):
            return s

    # Sri Lanka format: +94 followed by 9 digits starting with 7
    if s.startswith("94") and len(s) == 11 and s[2] == "7":
        if not is_garbage(s):
            return s

    # Indonesia format: +62 followed by 9-12 digits starting with 8
    if s.startswith("62") and 11 <= len(s) <= 14 and s[2] == "8":
        if not is_garbage(s):
            return s

    # Kenya format: +254 followed by 9 digits starting with 1 or 7
    if s.startswith("254") and len(s) == 12 and s[3] in "17":
        if not is_garbage(s):
            return s

    # Malaysia format: +60 followed by 9-10 digits starting with 1
    if s.startswith("60") and 11 <= len(s) <= 12 and s[2] == "1":
        if not is_garbage(s):
            return s

    # Thailand format: +66 followed by 9 digits starting with 6, 8, 9
    if s.startswith("66") and len(s) == 11 and s[2] in "689":
        if not is_garbage(s):
            return s

    # Vietnam format: +84 followed by 9 digits starting with 3, 5, 7, 8, 9
    if s.startswith("84") and len(s) == 11 and s[2] in "35789":
        if not is_garbage(s):
            return s

    # India format: 10 digits starting with 6-9 (or with 91 prefix or 0 trunk prefix)
    if s.startswith("0") and len(s) == 11:
        cand = s[1:]
        if cand[0] in "6789" and not is_garbage(cand):
            return cand
    elif s.startswith("91") and len(s) == 12:
        cand = s[2:]
        if cand[0] in "6789" and not is_garbage(cand):
            return cand
    elif len(s) > 10 and s.startswith("91"):
        cand = s[-10:]
        if cand[0] in "6789" and not is_garbage(cand):
            return cand
    elif len(s) == 10 and s[0] in "6789" and not is_garbage(s):
        return s

    # Fallback USA 10-digit if explicitly requested or formatted as (XXX) XXX-XXXX
    if len(s) == 10 and s[0] in "23456789" and ("(" in raw_str or "-" in raw_str or raw_str.startswith("+1")):
        us_cand = "1" + s
        if not is_garbage(us_cand):
            return us_cand

    return None


# Alias for backward compatibility
clean_mobile = normalize_mobile


def get_country_from_number(num: str) -> tuple[str, str, str, str]:
    """
    Returns (country_code_iso, dial_prefix, flag_emoji, country_name)
    e.g. ("US", "+1", "🇺🇸", "USA"), ("KW", "+965", "🇰🇼", "Kuwait"), ("IN", "+91", "🇮🇳", "India")
    """
    if not num:
        return "IN", "+91", "🇮🇳", "India"
    s = re.sub(r"[^0-9]", "", str(num))
    if s.startswith("234") and len(s) == 13:
        return "NG", "+234", "🇳🇬", "Nigeria"
    if s.startswith("55") and len(s) == 13:
        return "BR", "+55", "🇧🇷", "Brazil"
    if s.startswith("1") and len(s) == 11:
        return "US", "+1", "🇺🇸", "USA"
    if s.startswith("965") and len(s) == 11:
        return "KW", "+965", "🇰🇼", "Kuwait"
    if s.startswith("880") and len(s) == 13:
        return "BD", "+880", "🇧🇩", "Bangladesh"
    if s.startswith("92") and len(s) == 12:
        return "PK", "+92", "🇵🇰", "Pakistan"
    if s.startswith("977") and len(s) == 13:
        return "NP", "+977", "🇳🇵", "Nepal"
    if s.startswith("94") and len(s) == 11:
        return "LK", "+94", "🇱🇰", "Sri Lanka"
    if s.startswith("62") and 11 <= len(s) <= 14:
        return "ID", "+62", "🇮🇩", "Indonesia"
    if s.startswith("254") and len(s) == 12:
        return "KE", "+254", "🇰🇪", "Kenya"
    if s.startswith("60") and 11 <= len(s) <= 12:
        return "MY", "+60", "🇲🇾", "Malaysia"
    if s.startswith("66") and len(s) == 11:
        return "TH", "+66", "🇹🇭", "Thailand"
    if s.startswith("84") and len(s) == 11:
        return "VN", "+84", "🇻🇳", "Vietnam"
    if len(s) == 10 and s[0] in "6789":
        return "IN", "+91", "🇮🇳", "India"
    if s.startswith("91") and len(s) == 12:
        return "IN", "+91", "🇮🇳", "India"
    return "GLOBAL", "+", "🌐", "Global"


def format_phone_number(num: str) -> str:
    """Format phone number with country code and clean spacing."""
    if not num:
        return ""
    s = re.sub(r"[^0-9]", "", str(num))
    if s.startswith("234") and len(s) == 13:
        # +234 803 693 8467
        return f"+234 {s[3:6]} {s[6:9]} {s[9:]}"
    if s.startswith("55") and len(s) == 13:
        # +55 (11) 99999-8888
        return f"+55 ({s[2:4]}) {s[4:9]}-{s[9:]}"
    if s.startswith("1") and len(s) == 11:
        # +1 (NPA) NXX-XXXX
        return f"+1 ({s[1:4]}) {s[4:7]}-{s[7:]}"
    if s.startswith("965") and len(s) == 11:
        # +965 XXXX XXXX
        return f"+965 {s[3:7]} {s[7:]}"
    if s.startswith("880") and len(s) == 13:
        # +880 1XXX XXXXXX
        return f"+880 {s[3:7]} {s[7:]}"
    if s.startswith("92") and len(s) == 12:
        # +92 3XX XXXXXXX
        return f"+92 {s[2:5]} {s[5:]}"
    if s.startswith("977") and len(s) == 13:
        # +977 98XX XXXXXX
        return f"+977 {s[3:7]} {s[7:]}"
    if s.startswith("94") and len(s) == 11:
        # +94 7X XXX XXXX
        return f"+94 {s[2:4]} {s[4:7]} {s[7:]}"
    if s.startswith("62") and 11 <= len(s) <= 14:
        # +62 8XX XXXX XXX
        return f"+62 {s[2:5]} {s[5:]}"
    if s.startswith("254") and len(s) == 12:
        # +254 7XX XXXXXX
        return f"+254 {s[3:6]} {s[6:]}"
    if s.startswith("60") and 11 <= len(s) <= 12:
        # +60 1X XXX XXXX
        return f"+60 {s[2:4]} {s[4:]}"
    if s.startswith("66") and len(s) == 11:
        # +66 8X XXX XXXX
        return f"+66 {s[2:4]} {s[4:7]} {s[7:]}"
    if s.startswith("84") and len(s) == 11:
        # +84 3XX XXX XXX
        return f"+84 {s[2:5]} {s[5:]}"
    if len(s) == 10 and s[0] in "6789":
        # +91 XXXXX XXXXX
        return f"+91 {s[:5]} {s[5:]}"
    if s.startswith("91") and len(s) == 12:
        return f"+91 {s[2:7]} {s[7:]}"
    return num


def extract_sim_numbers(device_data: Any) -> set[str]:
    """Extract all phone numbers from Firebase client/device data using direct fields and deep search."""
    found: set[str] = set()
    if not device_data:
        return found

    def _walk(obj: Any):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (str, int)):
                    mob = normalize_mobile(v)
                    if mob:
                        found.add(mob)
                    for m in RAW_MOBILE_PATTERN.findall(str(v)):
                        nm = normalize_mobile(m)
                        if nm:
                            found.add(nm)
                    for m in RAW_US_MOBILE_PATTERN.findall(str(v)):
                        nm = normalize_mobile(m)
                        if nm:
                            found.add(nm)
                    for m in RAW_KW_MOBILE_PATTERN.findall(str(v)):
                        nm = normalize_mobile(m)
                        if nm:
                            found.add(nm)
                    for m in RAW_INTL_PATTERN.findall(str(v)):
                        nm = normalize_mobile(m)
                        if nm:
                            found.add(nm)
                elif isinstance(v, (dict, list)):
                    _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (str, int)):
                    mob = normalize_mobile(item)
                    if mob:
                        found.add(mob)
                _walk(item)

    _walk(device_data)
    return found


def extract_numbers_from_messages(messages: Any) -> set[str]:
    """Extract mobile numbers from SMS message history (handles dict and list)."""
    found: set[str] = set()
    if not messages:
        return found
    
    items = messages.values() if isinstance(messages, dict) else (messages if isinstance(messages, list) else [])
    
    # Jio-specific patterns first
    for item in items:
        if not isinstance(item, dict):
            continue
        body = str(item.get("message", item.get("body", item.get("msg", item.get("text", "")))))
        for pattern in NUMBER_PATTERNS:
            for match in pattern.findall(body):
                m = normalize_mobile(match)
                if m:
                    found.add(m)

    # General number extraction as fallback
    if not found:
        for item in items:
            if not isinstance(item, dict):
                continue
            for key_name in ("message", "body", "sender", "phoneNumber", "from", "to", "phone", "number", "dest"):
                val = str(item.get(key_name, ""))
                mob = normalize_mobile(val)
                if mob:
                    found.add(mob)
                for m in RAW_MOBILE_PATTERN.findall(val):
                    mob_re = normalize_mobile(m)
                    if mob_re:
                        found.add(mob_re)
                for m in RAW_US_MOBILE_PATTERN.findall(val):
                    mob_re = normalize_mobile(m)
                    if mob_re:
                        found.add(mob_re)
                for m in RAW_KW_MOBILE_PATTERN.findall(val):
                    mob_re = normalize_mobile(m)
                    if mob_re:
                        found.add(mob_re)
    return found


def extract_otp(text: str) -> str | None:
    """Extract OTP code from SMS text using all known patterns. Returns code or None."""
    if not text:
        return None
    text = normalize_digits(text)
    # Only try extraction if it looks like an OTP message
    if not OTP_WORD_PATTERN.search(text):
        return None
    for pattern in _OTP_EXTRACT_PATTERNS:
        match = pattern.search(text)
        if match:
            # Get first non-None group
            code = None
            for g in match.groups():
                if g:
                    code = g
                    break
            if code:
                code = code.replace("-", "").replace(" ", "")
                if 4 <= len(code) <= 8 and code.isdigit():
                    return code
    return None
