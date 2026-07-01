#!/usr/bin/env python3
"""check_gaps.py: scan data/ and report, per data type, the last write time
and intra-day gaps for a given UTC date (default: today).

Gap rule: for polled types, any spacing between consecutive records larger
than 1.6x the expected interval counts as a gap. The liquidation stream is
event-driven (and exchange-side sampled), so only its last event time is
reported, never a gap.

Usage:
  python check_gaps.py                  # today (UTC)
  python check_gaps.py --date 2026-06-11
"""

import argparse
import gzip
import json
import time
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"

# expected seconds between records; None = event-driven, no gap check
# ts key: which timestamp defines spacing (bars are spaced by exchange
# bar time, snapshots by local receive time)
TYPES = {
    "oi":                 {"interval": 300,  "ts": "ts_event"},
    "oi_1m":              {"interval": 60,   "ts": "ts_event"},
    "lsr_global_account": {"interval": 300,  "ts": "ts_event"},
    "lsr_top_account":    {"interval": 300,  "ts": "ts_event"},
    "lsr_top_position":   {"interval": 300,  "ts": "ts_event"},
    "lsr_taker":          {"interval": 300,  "ts": "ts_event"},
    "mark":               {"interval": 60,   "ts": "ts_local"},
    "depth":              {"interval": 60,   "ts": "ts_local"},
    "depth_imbalance":    {"interval": 5,    "ts": "ts_local"},
    "depth20":            {"interval": 30,   "ts": "ts_local"},
    "options_deribit":    {"interval": 3600, "ts": "ts_local", "group": "snapshot_ts"},
    "liquidation":        {"interval": None, "ts": "ts_local"},
}
GAP_FACTOR = 1.6


def load_records(dtype: str, date_str: str):
    dir_ = DATA_DIR / dtype
    for path in (dir_ / f"{date_str}.jsonl", dir_ / f"{date_str}.jsonl.gz"):
        if not path.exists():
            continue
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
        return


def fmt_ts(ms):
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M:%S")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    help="UTC date to inspect, YYYY-MM-DD (default: today)")
    args = ap.parse_args()

    now_ms = int(time.time() * 1000)
    print(f"data dir : {DATA_DIR}")
    print(f"date     : {args.date} (UTC)")
    print()
    header = f"{'type':<20}{'rows':>7}{'first':>10}{'last':>10}{'age_s':>8}{'gaps':>6}{'max_gap_s':>11}"
    print(header)
    print("-" * len(header))

    for dtype, spec in TYPES.items():
        records = list(load_records(dtype, args.date))
        if not records:
            print(f"{dtype:<20}{'0':>7}{'-':>10}{'-':>10}{'-':>8}{'-':>6}{'-':>11}")
            continue

        # per-symbol/group series so multi-symbol files do not fake gaps
        series: dict[str, list[int]] = {}
        for rec in records:
            ts = rec.get(spec["ts"]) or rec.get("ts_local")
            if spec.get("group"):
                key = str(rec.get("currency", ""))
                ts = rec.get(spec["group"], ts)
            else:
                key = str(rec.get("symbol", ""))
            series.setdefault(key, []).append(ts)

        all_ts = sorted(t for v in series.values() for t in v)
        first, last = all_ts[0], all_ts[-1]
        age_s = (now_ms - last) // 1000

        gaps = 0
        max_gap = 0.0
        if spec["interval"]:
            threshold = spec["interval"] * GAP_FACTOR * 1000
            for ts_list in series.values():
                uniq = sorted(set(ts_list))
                for a, b in zip(uniq, uniq[1:]):
                    d = b - a
                    max_gap = max(max_gap, d / 1000)
                    if d > threshold:
                        gaps += 1
            gap_str, max_str = str(gaps), f"{max_gap:.0f}"
        else:
            gap_str, max_str = "n/a", "n/a"

        print(f"{dtype:<20}{len(records):>7}{fmt_ts(first):>10}{fmt_ts(last):>10}"
              f"{age_s:>8}{gap_str:>6}{max_str:>11}")

    print()
    print("note: liquidation is event-driven (exchange samples max 1/s/symbol);")
    print("      no gap metric applies, only the last event time.")


if __name__ == "__main__":
    main()
