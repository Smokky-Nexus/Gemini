"""
panel_scores.py — Panel Intelligence Scoring Engine
====================================================
Tracks per-panel performance: hits (active devices found),
claims (links successfully claimed), and avg latency.
Panels are sorted by composite score so the best ones
get scanned first every run.

Score formula:
  score = (hits * hit_weight) + (claims * claim_weight) + (1000/max(avg_latency_ms,1) * latency_weight)

Auto-loads/saves panel_scores.json in the project directory.
"""
from __future__ import annotations
import json
import threading
import time
from pathlib import Path
from typing import Any

SCORES_FILE = Path("panel_scores.json")

# Weights — tweak via config.json if needed
HIT_WEIGHT: float = 0.4
CLAIM_WEIGHT: float = 10.0
LATENCY_WEIGHT: float = 0.2

_lock = threading.Lock()
_data: dict[str, dict[str, Any]] = {}
_dirty = False


def _default_entry() -> dict[str, Any]:
    return {
        "hits": 0,
        "claims": 0,
        "total_latency_ms": 0.0,
        "request_count": 0,
        "last_seen": 0,
        "score": 0.0,
    }


def load(path: Path = SCORES_FILE) -> None:
    """Load scores from disk. Called once at startup."""
    global _data
    if path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            with _lock:
                _data = {k: {**_default_entry(), **v} for k, v in raw.items() if isinstance(v, dict)}
        except Exception:
            pass


def save(path: Path = SCORES_FILE) -> None:
    """Flush scores to disk."""
    global _dirty
    with _lock:
        if not _dirty:
            return
        snapshot = dict(_data)
        _dirty = False
    try:
        path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    except Exception:
        pass


def _recalculate(entry: dict[str, Any]) -> None:
    avg_lat = (entry["total_latency_ms"] / max(1, entry["request_count"]))
    entry["score"] = (
        entry["hits"] * HIT_WEIGHT
        + entry["claims"] * CLAIM_WEIGHT
        + (1000.0 / max(avg_lat, 1.0)) * LATENCY_WEIGHT
    )


def record_hit(url: str, latency_ms: float = 0.0) -> None:
    """Call when a panel yields at least one active device."""
    global _dirty
    with _lock:
        e = _data.setdefault(url, _default_entry())
        
        now = time.time()
        is_stale = False
        if e.get("last_seen", 0) > 0 and (now - e.get("last_seen", 0)) > 86400:
            is_stale = True
            
        e["hits"] += 1
        if latency_ms > 0:
            e["total_latency_ms"] += latency_ms
            e["request_count"] += 1
            
        _recalculate(e)
        
        if is_stale:
            e["score"] *= 0.9
            
        e["last_seen"] = time.time()
        _dirty = True


def record_claim(url: str) -> None:
    """Call when a Google AI Pro link is successfully claimed via this panel."""
    global _dirty
    with _lock:
        e = _data.setdefault(url, _default_entry())
        e["claims"] += 1
        e["last_seen"] = time.time()
        _recalculate(e)
        _dirty = True


def decay_stale_scores(max_age_hours: int = 24):
    """Decay scores for panels not seen recently. Call periodically."""
    global _dirty
    now = time.time()
    cutoff = now - (max_age_hours * 3600)
    with _lock:
        decayed = False
        for url, data in _data.items():
            if data.get("last_seen", now) < cutoff:
                data["score"] = max(0, data.get("score", 0) * 0.9)
                decayed = True
        if decayed:
            _dirty = True


def record_latency(url: str, latency_ms: float) -> None:
    """Call after every successful panel request with the measured latency."""
    global _dirty
    with _lock:
        e = _data.setdefault(url, _default_entry())
        e["total_latency_ms"] += latency_ms
        e["request_count"] += 1
        _recalculate(e)
        _dirty = True


def get_score(url: str) -> float:
    with _lock:
        return _data.get(url, _default_entry())["score"]


def sorted_panels(panels: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """
    Sort panels by score descending.
    Unknown panels (no score yet) go to the middle, not the end —
    so new panels still get tried, but proven ones come first.
    """
    with _lock:
        snapshot = dict(_data)

    def key(p: tuple[str, str]) -> float:
        url = p[0]
        if url in snapshot:
            return snapshot[url]["score"]
        return 0.5  # neutral score for unknowns

    return sorted(panels, key=key, reverse=True)


def stats_summary() -> dict[str, Any]:
    with _lock:
        if not _data:
            return {"panels_tracked": 0, "total_hits": 0, "total_claims": 0}
        return {
            "panels_tracked": len(_data),
            "total_hits": sum(e["hits"] for e in _data.values()),
            "total_claims": sum(e["claims"] for e in _data.values()),
            "top_panels": sorted(
                [(url, e["score"], e["claims"]) for url, e in _data.items()],
                key=lambda x: x[1],
                reverse=True
            )[:5],
        }


# Auto-save background thread
def _auto_save_loop(interval: int = 60) -> None:
    while True:
        time.sleep(interval)
        save()


def start_auto_save(interval: int = 60) -> None:
    import threading as _t
    t = _t.Thread(target=_auto_save_loop, args=(interval,), daemon=True)
    t.start()


# Load on import
load()
