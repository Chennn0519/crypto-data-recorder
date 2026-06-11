#!/usr/bin/env python3
"""crypto-data-recorder: 24/7 recorder for exchange data that has no purchasable history.

Records (all free public endpoints, no API keys):
  oi                  Binance futures open interest history (5m bars, REST poll)
  lsr_global_account  Binance global long/short account ratio (5m bars, REST poll)
  lsr_top_account     Binance top trader long/short account ratio (5m bars, REST poll)
  lsr_top_position    Binance top trader long/short position ratio (5m bars, REST poll)
  lsr_taker           Binance taker buy/sell volume ratio (5m bars, REST poll)
  liquidation         Binance futures forced liquidation orders (websocket;
                      exchange pushes at most 1 order per second per symbol,
                      i.e. this stream is a sample, not the full flow)
  depth               Binance futures order book snapshot, top 100 levels (REST, 1m)
  options_deribit     Deribit option chain book summary (REST, 1h)

Every record is one JSON line with two timestamps:
  ts_local  local receive time (UTC, ms since epoch)
  ts_event  exchange-reported event time (UTC, ms; null if the source has none)
The raw exchange payload is kept unmodified under "data".

Files: data/<type>/<YYYY-MM-DD>.jsonl (UTC date). On daily rotation,
previous days are gzipped in the background.

The recorder records faithfully and reconnects forever; it never computes,
interpolates, or backfills. Gaps stay visible through the timestamps.

Usage:
  python recorder.py                 # run forever (production)
  python recorder.py --minutes 30    # stop after 30 minutes (local testing)
"""

import argparse
import asyncio
import glob
import gzip
import json
import logging
import logging.handlers
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import websockets

# ---------------------------------------------------------------- config

CONFIG = {
    "symbols": ["BTCUSDT", "ETHUSDT"],   # Binance USDT-M futures
    "deribit_currencies": ["BTC"],       # Deribit option chains
    "intervals": {                       # seconds
        "bars": 300,        # oi + the four long/short ratios (5m bars)
        "depth": 60,
        "options": 3600,
        "heartbeat": 3600,
    },
    "bar_poll_offset": 30,  # poll 5m endpoints 30s after the bar boundary
    "depth_limit": 100,
    "http_timeout": 10,
    "ws_url_base": "wss://fstream.binance.com",
    "binance_base": "https://fapi.binance.com",
    "deribit_base": "https://www.deribit.com",
}

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
LOG_DIR = ROOT / "logs"

# Binance 5m-bar REST endpoints -> data type name
BAR_ENDPOINTS = {
    "oi": "/futures/data/openInterestHist",
    "lsr_global_account": "/futures/data/globalLongShortAccountRatio",
    "lsr_top_account": "/futures/data/topLongShortAccountRatio",
    "lsr_top_position": "/futures/data/topLongShortPositionRatio",
    "lsr_taker": "/futures/data/takerlongshortRatio",
}

log = logging.getLogger("recorder")
SESSION = requests.Session()
SESSION.headers["User-Agent"] = "crypto-data-recorder/0.1"


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_date_str(ts: float | None = None) -> str:
    dt = datetime.fromtimestamp(ts if ts is not None else time.time(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


# ---------------------------------------------------------------- writer

class JsonlWriter:
    """Append-only JSONL writer, one file per data type per UTC day.

    Thread-safe. On date rollover the previous day's files are gzipped
    in a background thread (the .jsonl is removed after compression).
    """

    def __init__(self, root: Path):
        self.root = root
        self.lock = threading.Lock()
        self._files = {}   # dtype -> (date_str, file handle)
        self.counts = {}   # dtype -> records written since start

    def write(self, dtype: str, record: dict) -> None:
        line = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
        today = utc_date_str()
        with self.lock:
            cur = self._files.get(dtype)
            if cur is None or cur[0] != today:
                if cur is not None:
                    cur[1].close()
                dir_ = self.root / dtype
                dir_.mkdir(parents=True, exist_ok=True)
                fh = open(dir_ / f"{today}.jsonl", "a", encoding="utf-8")
                self._files[dtype] = (today, fh)
                threading.Thread(
                    target=compress_old_files, args=(dir_, today), daemon=True
                ).start()
            fh = self._files[dtype][1]
            fh.write(line + "\n")
            fh.flush()
            self.counts[dtype] = self.counts.get(dtype, 0) + 1

    def close(self) -> None:
        with self.lock:
            for _, fh in self._files.values():
                fh.close()
            self._files.clear()


def compress_old_files(dir_: Path, keep_date: str) -> None:
    """Gzip every .jsonl in dir_ whose date is older than keep_date."""
    for path_str in glob.glob(str(dir_ / "*.jsonl")):
        path = Path(path_str)
        if path.stem >= keep_date:
            continue
        gz_path = path.with_suffix(".jsonl.gz")
        try:
            with open(path, "rb") as src, gzip.open(gz_path, "wb") as dst:
                while chunk := src.read(1 << 20):
                    dst.write(chunk)
            path.unlink()
            log.info("compressed %s", gz_path.name)
        except OSError as e:
            log.error("failed to compress %s: %s", path, e)


WRITER = JsonlWriter(DATA_DIR)


def last_event_ts(dtype: str, symbol: str) -> int:
    """Max ts_event already on disk today for (dtype, symbol); 0 if none.

    Used to deduplicate bar endpoints across polls and restarts.
    """
    path = DATA_DIR / dtype / f"{utc_date_str()}.jsonl"
    if not path.exists():
        return 0
    best = 0
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("symbol") == symbol and rec.get("ts_event"):
                    best = max(best, rec["ts_event"])
    except OSError:
        pass
    return best


# ---------------------------------------------------------------- REST pollers

LAST_SEEN: dict[tuple[str, str], int] = {}


def fetch_bars() -> None:
    """Poll the five Binance 5m-bar endpoints, write only unseen bars."""
    for dtype, endpoint in BAR_ENDPOINTS.items():
        for symbol in CONFIG["symbols"]:
            key = (dtype, symbol)
            if key not in LAST_SEEN:
                LAST_SEEN[key] = last_event_ts(dtype, symbol)
            r = SESSION.get(
                CONFIG["binance_base"] + endpoint,
                params={"symbol": symbol, "period": "5m", "limit": 3},
                timeout=CONFIG["http_timeout"],
            )
            r.raise_for_status()
            ts_local = now_ms()
            rows = sorted(r.json(), key=lambda x: x.get("timestamp", 0))
            for row in rows:
                ts_event = row.get("timestamp")
                if not ts_event or ts_event <= LAST_SEEN[key]:
                    continue
                WRITER.write(dtype, {
                    "ts_local": ts_local,
                    "ts_event": ts_event,
                    "symbol": symbol,
                    "data": row,
                })
                LAST_SEEN[key] = ts_event


def fetch_depth() -> None:
    """Order book snapshot, top N levels, per symbol."""
    for symbol in CONFIG["symbols"]:
        r = SESSION.get(
            CONFIG["binance_base"] + "/fapi/v1/depth",
            params={"symbol": symbol, "limit": CONFIG["depth_limit"]},
            timeout=CONFIG["http_timeout"],
        )
        r.raise_for_status()
        resp = r.json()
        WRITER.write("depth", {
            "ts_local": now_ms(),
            "ts_event": resp.get("E"),  # Binance message output time
            "symbol": symbol,
            "data": resp,
        })


def fetch_options() -> None:
    """Deribit option chain book summary; one line per instrument,
    grouped by snapshot_ts (local request time of the snapshot)."""
    for currency in CONFIG["deribit_currencies"]:
        snapshot_ts = now_ms()
        r = SESSION.get(
            CONFIG["deribit_base"] + "/api/v2/public/get_book_summary_by_currency",
            params={"currency": currency, "kind": "option"},
            timeout=30,  # large response
        )
        r.raise_for_status()
        resp = r.json()
        if "result" not in resp:
            raise RuntimeError(f"deribit error response: {resp.get('error')}")
        ts_local = now_ms()
        for item in resp["result"]:
            WRITER.write("options_deribit", {
                "ts_local": ts_local,
                "ts_event": item.get("creation_timestamp"),
                "snapshot_ts": snapshot_ts,
                "currency": currency,
                "data": item,
            })
        log.info("options snapshot %s: %d instruments", currency, len(resp["result"]))


def poll_loop(stop: threading.Event, name: str, interval: int, offset: int, fn) -> None:
    """Call fn immediately, then on every aligned interval tick until stop.

    A failed call is retried up to 3 times within the tick; after that the
    tick is skipped (the gap stays detectable via timestamps).
    """
    _call_with_retry(stop, name, fn)
    while not stop.is_set():
        wait = interval - (time.time() - offset) % interval
        if stop.wait(wait):
            break
        _call_with_retry(stop, name, fn)


def _call_with_retry(stop: threading.Event, name: str, fn) -> None:
    for attempt in range(1, 4):
        if stop.is_set():
            return
        try:
            fn()
            return
        except Exception as e:
            log.warning("%s poll failed (attempt %d/3): %s", name, attempt, e)
            stop.wait(5)
    log.error("%s: giving up this tick", name)


# ---------------------------------------------------------------- websocket

async def _ws_main(stop: threading.Event) -> None:
    streams = "/".join(f"{s.lower()}@forceOrder" for s in CONFIG["symbols"])
    url = f"{CONFIG['ws_url_base']}/stream?streams={streams}"
    backoff = 1
    while not stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log.info("liquidation websocket connected (%s)", streams)
                connected_at = time.time()
                while not stop.is_set():
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        continue
                    ts_local = now_ms()
                    payload = json.loads(msg)
                    data = payload.get("data", payload)
                    if data.get("e") != "forceOrder":
                        continue
                    order = data.get("o", {})
                    WRITER.write("liquidation", {
                        "ts_local": ts_local,
                        "ts_event": data.get("E"),
                        "symbol": order.get("s"),
                        "data": order,
                    })
                    if time.time() - connected_at > 60:
                        backoff = 1  # connection proved stable, reset backoff
        except Exception as e:
            if stop.is_set():
                break
            log.warning("websocket error: %s; reconnecting in %ds", e, backoff)
            deadline = time.time() + backoff
            while time.time() < deadline and not stop.is_set():
                await asyncio.sleep(0.5)
            backoff = min(backoff * 2, 60)
    log.info("liquidation websocket stopped")


def ws_thread(stop: threading.Event) -> None:
    asyncio.run(_ws_main(stop))


# ---------------------------------------------------------------- heartbeat

def heartbeat_loop(stop: threading.Event, started: float) -> None:
    interval = CONFIG["intervals"]["heartbeat"]
    while not stop.wait(interval):
        uptime_h = (time.time() - started) / 3600
        counts = dict(sorted(WRITER.counts.items()))
        log.info("heartbeat: uptime %.1fh, records since start: %s", uptime_h, counts)


# ---------------------------------------------------------------- main

def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter(
        "%(asctime)sZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
    )
    fmt.converter = time.gmtime
    file_h = logging.handlers.RotatingFileHandler(
        LOG_DIR / "recorder.log", maxBytes=10_000_000, backupCount=5, encoding="utf-8"
    )
    console_h = logging.StreamHandler()
    for h in (file_h, console_h):
        h.setFormatter(fmt)
        log.addHandler(h)
    log.setLevel(logging.INFO)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--minutes", type=float, default=0,
                    help="stop after N minutes (0 = run forever)")
    args = ap.parse_args()

    setup_logging()
    started = time.time()
    log.info("recorder starting: symbols=%s deribit=%s pid=%d",
             CONFIG["symbols"], CONFIG["deribit_currencies"], os.getpid())

    stop = threading.Event()
    iv = CONFIG["intervals"]
    threads = [
        threading.Thread(target=poll_loop, name="bars",
                         args=(stop, "bars", iv["bars"], CONFIG["bar_poll_offset"], fetch_bars)),
        threading.Thread(target=poll_loop, name="depth",
                         args=(stop, "depth", iv["depth"], 0, fetch_depth)),
        threading.Thread(target=poll_loop, name="options",
                         args=(stop, "options", iv["options"], 0, fetch_options)),
        threading.Thread(target=ws_thread, name="ws", args=(stop,)),
        threading.Thread(target=heartbeat_loop, name="heartbeat", args=(stop, started)),
    ]
    for t in threads:
        t.daemon = True
        t.start()

    deadline = started + args.minutes * 60 if args.minutes > 0 else None
    try:
        while not stop.is_set():
            if deadline and time.time() >= deadline:
                log.info("test duration reached (%.0f min), stopping", args.minutes)
                stop.set()
                break
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("interrupted, stopping")
        stop.set()

    for t in threads:
        t.join(timeout=10)
    WRITER.close()
    log.info("recorder stopped: records written: %s", dict(sorted(WRITER.counts.items())))


if __name__ == "__main__":
    main()
