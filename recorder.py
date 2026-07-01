#!/usr/bin/env python3
"""crypto-data-recorder: 24/7 recorder for exchange data that has no purchasable history.

Records (all free public endpoints, no API keys):
  oi                  Binance futures open interest history (5m bars, REST poll)
  oi_1m               Binance futures open interest, instantaneous value (REST, 1m)
  lsr_global_account  Binance global long/short account ratio (5m bars, REST poll)
  lsr_top_account     Binance top trader long/short account ratio (5m bars, REST poll)
  lsr_top_position    Binance top trader long/short position ratio (5m bars, REST poll)
  lsr_taker           Binance taker buy/sell volume ratio (5m bars, REST poll)
  liquidation         Binance futures forced liquidation orders (websocket;
                      exchange pushes at most 1 order per second per symbol,
                      i.e. this stream is a sample, not the full flow)
  mark                Binance mark/index price + funding rate (websocket 1s
                      stream, sampled to 1 record per minute)
  depth               Binance futures order book snapshot, top 100 levels (REST, 1m)
  depth20             raw top-20 order book snapshot (websocket, every 30s)
  depth_imbalance     derived order-book imbalance features computed from the
                      top-20 websocket book (every 5s) -- the only derived
                      stream; the raw book it is computed from is kept in depth20
  options_deribit     Deribit option chain book summary (REST, 1h)

Every record is one JSON line with two timestamps:
  ts_local  local receive time (UTC, ms since epoch)
  ts_event  exchange-reported event time (UTC, ms; null if the source has none)
The raw exchange payload is kept unmodified under "data" (except depth_imbalance,
whose "data" holds the computed features).

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
    "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],   # Binance USDT-M futures
    "deribit_currencies": ["BTC"],       # Deribit option chains
    "intervals": {                       # seconds
        "bars": 300,        # oi + the four long/short ratios (5m bars)
        "oi_1m": 60,        # instantaneous open interest, finer than the 5m bars
        "depth": 60,        # REST top-100 snapshot
        "depth_imbalance": 5,   # derived order-book imbalance from the ws book
        "depth_raw": 30,        # raw top-20 book snapshot from the ws book
        "mark": 60,             # mark/index/funding, aggregated from the 1s stream
        "options": 3600,
        "heartbeat": 3600,
    },
    "bar_poll_offset": 30,  # poll 5m endpoints 30s after the bar boundary
    "depth_limit": 100,
    "depth_ws_levels": 20,  # partial-book-depth stream depth (5/10/20 allowed)
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


def fetch_oi_1m() -> None:
    """Instantaneous open interest per symbol, polled every minute.

    The 5m `oi` stream uses openInterestHist, whose bars only update every
    5 minutes. This uses the current-value endpoint so open interest can be
    sampled at 1-minute grid resolution. Kept as a separate stream so the
    coarser historical `oi` series stays continuous.
    """
    for symbol in CONFIG["symbols"]:
        r = SESSION.get(
            CONFIG["binance_base"] + "/fapi/v1/openInterest",
            params={"symbol": symbol},
            timeout=CONFIG["http_timeout"],
        )
        r.raise_for_status()
        resp = r.json()
        WRITER.write("oi_1m", {
            "ts_local": now_ms(),
            "ts_event": resp.get("time"),  # exchange server time of the value
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
    # forceOrder lives on the /market route; legacy unrouted /stream URLs were
    # decommissioned 2026-04-23 (they connect fine but push no data)
    url = f"{CONFIG['ws_url_base']}/market/stream?streams={streams}"
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


# ---------------------------------------------------------------- mark price ws

def _flush_mark(latest: dict) -> None:
    """Write one mark-price record per symbol (the last tick of the minute)."""
    for symbol, (ts_local, data) in latest.items():
        WRITER.write("mark", {
            "ts_local": ts_local,
            "ts_event": data.get("E"),
            "symbol": symbol,
            "data": data,   # raw payload: p=mark, i=index, r=funding, T=next funding
        })


async def _mark_ws_main(stop: threading.Event) -> None:
    streams = "/".join(f"{s.lower()}@markPrice@1s" for s in CONFIG["symbols"])
    # markPrice lives on the /market route (see the 2026-04-23 route change)
    url = f"{CONFIG['ws_url_base']}/market/stream?streams={streams}"
    backoff = 1
    while not stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log.info("markPrice websocket connected (%s)", streams)
                connected_at = time.time()
                latest = {}                        # symbol -> (ts_local, data)
                last_minute = int(time.time() // 60)
                while not stop.is_set():
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=2)
                    except asyncio.TimeoutError:
                        msg = None
                    if msg is not None:
                        data = json.loads(msg).get("data", {})
                        if data.get("e") == "markPriceUpdate":
                            latest[data.get("s")] = (now_ms(), data)
                        if time.time() - connected_at > 60:
                            backoff = 1
                    cur_minute = int(time.time() // 60)
                    if cur_minute != last_minute:
                        _flush_mark(latest)        # sample the last tick per minute
                        last_minute = cur_minute
        except Exception as e:
            if stop.is_set():
                break
            log.warning("markPrice websocket error: %s; reconnecting in %ds", e, backoff)
            deadline = time.time() + backoff
            while time.time() < deadline and not stop.is_set():
                await asyncio.sleep(0.5)
            backoff = min(backoff * 2, 60)
    log.info("markPrice websocket stopped")


def mark_ws_thread(stop: threading.Event) -> None:
    asyncio.run(_mark_ws_main(stop))


# ---------------------------------------------------------------- depth book ws

def compute_imbalance(bids: list, asks: list) -> dict | None:
    """Order-book imbalance features from raw top-N bid/ask level lists.

    bids are [price, qty] descending, asks ascending. Returns None if either
    side is empty. All quantities are in base asset (contracts), prices quote.
    imbalance_N is in [-1, 1]: +1 all bids, -1 all asks.
    """
    b = [(float(p), float(q)) for p, q in bids]
    a = [(float(p), float(q)) for p, q in asks]
    if not b or not a:
        return None
    best_bid, best_ask = b[0][0], a[0][0]
    mid = (best_bid + best_ask) / 2
    out = {"mid": mid, "spread": best_ask - best_bid,
           "best_bid": best_bid, "best_ask": best_ask}
    for n in (5, 10, 20):
        bq = sum(q for _, q in b[:n])
        aq = sum(q for _, q in a[:n])
        tot = bq + aq
        out[f"bid_qty_{n}"] = bq
        out[f"ask_qty_{n}"] = aq
        out[f"imbalance_{n}"] = (bq - aq) / tot if tot else 0.0
    for label, pct in (("0p5pct", 0.005), ("1pct", 0.01)):
        lo, hi = mid * (1 - pct), mid * (1 + pct)
        out[f"bid_within_{label}"] = sum(q for p, q in b if p >= lo)
        out[f"ask_within_{label}"] = sum(q for p, q in a if p <= hi)
    return out


def _flush_imbalance(book: dict) -> None:
    ts_local = now_ms()
    for symbol, (_, ts_event, bids, asks) in book.items():
        feat = compute_imbalance(bids, asks)
        if feat is None:
            continue
        WRITER.write("depth_imbalance", {
            "ts_local": ts_local,
            "ts_event": ts_event,
            "symbol": symbol,
            "data": feat,
        })


def _flush_depth_raw(book: dict) -> None:
    ts_local = now_ms()
    for symbol, (_, ts_event, bids, asks) in book.items():
        WRITER.write("depth20", {
            "ts_local": ts_local,
            "ts_event": ts_event,
            "symbol": symbol,
            "data": {"bids": bids, "asks": asks},
        })


async def _depth_ws_main(stop: threading.Event) -> None:
    lvl = CONFIG["depth_ws_levels"]
    streams = "/".join(f"{s.lower()}@depth{lvl}@100ms" for s in CONFIG["symbols"])
    # partial-book-depth streams are on the /public route (not /market)
    url = f"{CONFIG['ws_url_base']}/public/stream?streams={streams}"
    imb_iv = CONFIG["intervals"]["depth_imbalance"]
    raw_iv = CONFIG["intervals"]["depth_raw"]
    backoff = 1
    while not stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log.info("depth websocket connected (%s)", streams)
                connected_at = time.time()
                book = {}   # symbol -> (ts_recv, ts_event, bids, asks)
                now = time.time()
                next_imb = now - now % imb_iv + imb_iv
                next_raw = now - now % raw_iv + raw_iv
                while not stop.is_set():
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=2)
                    except asyncio.TimeoutError:
                        msg = None
                    if msg is not None:
                        data = json.loads(msg).get("data", {})
                        if data.get("e") == "depthUpdate" and data.get("b") is not None:
                            book[data.get("s")] = (now_ms(), data.get("T") or data.get("E"),
                                                   data.get("b"), data.get("a"))
                        if time.time() - connected_at > 60:
                            backoff = 1
                    now = time.time()
                    if now >= next_imb:
                        _flush_imbalance(book)
                        next_imb += imb_iv
                        if now >= next_imb:            # fell behind: realign
                            next_imb = now - now % imb_iv + imb_iv
                    if now >= next_raw:
                        _flush_depth_raw(book)
                        next_raw += raw_iv
                        if now >= next_raw:
                            next_raw = now - now % raw_iv + raw_iv
        except Exception as e:
            if stop.is_set():
                break
            log.warning("depth websocket error: %s; reconnecting in %ds", e, backoff)
            deadline = time.time() + backoff
            while time.time() < deadline and not stop.is_set():
                await asyncio.sleep(0.5)
            backoff = min(backoff * 2, 60)
    log.info("depth websocket stopped")


def depth_ws_thread(stop: threading.Event) -> None:
    asyncio.run(_depth_ws_main(stop))


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
        threading.Thread(target=poll_loop, name="oi_1m",
                         args=(stop, "oi_1m", iv["oi_1m"], 0, fetch_oi_1m)),
        threading.Thread(target=poll_loop, name="depth",
                         args=(stop, "depth", iv["depth"], 0, fetch_depth)),
        threading.Thread(target=poll_loop, name="options",
                         args=(stop, "options", iv["options"], 0, fetch_options)),
        threading.Thread(target=ws_thread, name="ws", args=(stop,)),
        threading.Thread(target=mark_ws_thread, name="mark_ws", args=(stop,)),
        threading.Thread(target=depth_ws_thread, name="depth_ws", args=(stop,)),
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
