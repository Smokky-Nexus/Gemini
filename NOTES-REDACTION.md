# Redaction notes — prepared for responsible disclosure

This directory is a **sanitized copy** of `sitama-jio`, prepared so a vendor or
bug-bounty triage team can review the code without receiving harvested victim data
or live credentials.

The original directory is untouched.

---

## What was REMOVED and why

These files existed in the original. All contained either third-party personal
data or live secrets, so they are **not** included here.

| File | Contents in original | Why removed |
|---|---|---|
| `live_phones.txt` | 80 rows of real 10-digit Indian mobile numbers, each paired with the Firebase endpoint + API key that read that subscriber's OTP | Personal data of uninvolved third parties |
| `working_panels.txt` | 170 unauthenticated Firebase RTDB URLs, some with `AIzaSy…` keys | Live credentials to systems not owned by the reporter |
| `discovered_panels.txt` | 41 further unauthenticated RTDB endpoints | Same |
| `intl_panels_found.txt` | 6 RTDB endpoints with readable key names | Same |
| `panels.txt` | 2.2 KB of panel entries | Same |
| `gemini_results.csv` | 725 KB, one row per completed number (mobile, status, activation URL) | Personal data + downstream fraud artifacts |
| `gemini_activation_links.txt` | Claimed `serviceactivation.google.com` redemption links | Stolen entitlements |
| `wave_logs/*.jsonl` | Timestamped per-request records: OTP sent to batches of numbers, timings, proxy used | Record of harm to identifiable people |
| `panel_scores.json` | 39 KB scoring/profiling of victim databases | Same |
| `proxies.txt` | Working proxy credentials `host:port:user:pass` | Live secrets |
| `dead_panels.txt`, `fresh_panels.txt`, `used_links.txt` | Empty / trivial state | Operational state |
| `.venv/` (41 MB) | Windows venv from the original author's machine (`C:\Users\Sitama\…`) | Build artifact, non-portable |
| `venv/` (88 MB) | Linux venv, third-party site-packages | Build artifact, regenerable from `requirements.txt` |
| `__pycache__/` | Compiled bytecode | Build artifact |

Each removed text/JSON file is replaced by a **header-only placeholder** so the
code still runs and a reviewer can see which files the program expects.

### Note on the panel lists specifically

These endpoints belong to identifiable third-party services — including what
appear to be Indian government and banking systems (`pmkisan-*` farmer-welfare,
`rto-e-chall-*` / `echallan-*` traffic-challan, `yono-*` banking, `tillu-*`).

**If you are a reporter:** do not include these in a submission. Disclosing a
live third-party endpoint to one vendor hands it to a party with no authority
over the affected system, and it exposes the vendor's other users. Report
Firebase RTDB misconfiguration generically, or route each finding to the service
that actually owns the database.

---

## Pass 2 — database references removed from source

On request, the enumeration engine was also removed from the source itself.

### `firebase_scanner.py`

| Removed | Detail |
|---|---|
| `DOMAINS` list | Five Firebase RTDB hostname suffixes → replaced with `DOMAINS: list[str] = []` |
| `generate_candidates()` | The enumeration primitive. Now raises `_RedactionNotice` and returns nothing. Signature preserved. |
| `_generate_candidates_original()` | Original body replaced with a docstring. It resolved a hostname suffix, combined it with keyword/numeric/hex dictionaries, and emitted a probe-ready URL. |

**Verification:** a full-tree grep for `firebaseio`, `firebasedatabase`,
`default-rtdb`, `rtdb`, and `AIzaSy` returns zero matches outside this file
and the README. Both entry points were executed and confirmed to raise
`_RedactionNotice` rather than enumerate.

### Deliberately retained

`DORK_KEYWORDS` and `SUFFIXES` are **kept**. They are the most useful part of
the file for triage: they enumerate exactly which organisations and services
were targeted — `pmkisan`, `echallan`, `yono`, `sbi`, `ayushman`, `digilocker`,
`irctc`, `fastag`, `upi`, and so on, across India, Gulf, Nigeria, Kenya and
South-East Asia. That targeting profile is material to a triager assessing
severity and blast radius. The lists are inert without the removed function.

### Effect

The disclosure copy can no longer discover any database. Combined with the
zeroed `config.json`, there is no path by which this package can enumerate,
probe, or target a live Firebase instance. It demonstrates the vulnerability
class; it cannot exploit it.

---

## What was CHANGED

`config.json` only. The abuse primitives are neutralised so the copy cannot
function as a weapon even if executed:

- `threads.otp_workers`: 150 → 0
- `threads.wave_otp_workers`: 80 → 0
- `threads.wave_runners`: 3 → 0
- `wave.wave_batch_size`: 80 → 0
- `international.enabled`: true → false
- `international.use_for_otp_intercept`: true → false
- `daemon.run_beast_engine`: true → false
- `proxy.paid_proxies_file` repointed to a non-existent placeholder

All `.py` sources are **byte-identical** to the original, with the single
documented exception of `firebase_scanner.py` (see Pass 2 above). None were
stubbed out, renamed, or refactored beyond that removal — a reviewer is
assessing the real code.

---

## What this code does

`beast_engine.py` orchestrates a pipeline:

1. **Discover** open Firebase Realtime Database instances (`firebase_scanner.py`)
2. **Harvest** phone numbers from the exposed data
3. **Trigger** OTP requests against a third-party's SMS/voice login endpoint at
   high concurrency (`OTP_DISPATCH_WORKERS = 150`, `beast_engine.py:85`)
4. **Intercept** the resulting OTP from the exposed database
5. **Redeem** a promotional subscription with the intercepted OTP

Step 1 is a legitimate vulnerability class — misconfigured Firebase RTDB
databases exposing PII are a real and common bug-bounty finding.

Steps 3–5 are not. They automate OTP interception and subscription fraud
against uninvolved victims. No bug-bounty scope authorises that: the victim
subscriber is not the vendor, and the subscriber never consented.

The scale in `config.json` is included as evidence of intended throughput, not
as validated behaviour.