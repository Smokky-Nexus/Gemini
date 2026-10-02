# sitama-jio — security research sample

Sanitized copy prepared for responsible disclosure to an affected vendor or
bug-bounty triage team.

**Read `NOTES-REDACTION.md` first.** It explains exactly what was removed,
what was changed, and what the code does.

## What was excluded

Third-party personal data, live credentials, and build artifacts were removed.
This directory contains **no** harvested phone numbers, **no** live Firebase
endpoints or `AIzaSy…` keys, and **no** claimed redemption links.

## What is included

- All `.py` sources, **byte-identical** to the original except
  `firebase_scanner.py`, whose database-enumeration routine was removed and
  replaced with a shim that raises `_RedactionNotice`
- `config.json`, with the abuse primitives neutralised (workers set to 0)
- Header-only placeholders for every data file the code expects

## Verified clean

Full-tree grep returns **zero** matches for `firebaseio`, `firebasedatabase`,
`default-rtdb`, `rtdb`, and `AIzaSy` across all code and data files. There are
no live database references of any kind in this package.

## Safety

The neutralised `config.json` disables OTP dispatch. Do not restore the
original worker counts. If you need to demonstrate impact, request test
credentials and a written scope from the vendor first.

## Reproducing

```bash
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
./venv/bin/python -c "import beast_engine"   # import-only smoke test
```

The program will load and start, but performs no network operations without
operator input and no data files to seed from.

## Contact

TODO — replace with your researcher name, email, and disclosure reference
(program name, bounty platform, or ticket ID) before submitting.