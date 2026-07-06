# crypto-data-recorder

A 24/7 recorder for crypto market data that exchanges do not keep history for.
It records faithfully and reconnects forever; it never computes, interpolates,
or backfills. All sources are free public endpoints, no API keys required.

一支 24 小時常駐的加密貨幣資料錄製器,專門記錄「交易所不保留歷史、
事後買不到」的資料。只負責忠實記錄與斷線自癒,不做任何計算。
所有來源皆為免費公開端點,不需要任何 API key。

## What it records / 錄什麼

| type | source | cadence |
|---|---|---|
| `oi` | Binance `/futures/data/openInterestHist` (free API only keeps 30 days) | 5m |
| `oi_1m` | Binance `/fapi/v1/openInterest` instantaneous value | 1m |
| `lsr_global_account` | Binance `/futures/data/globalLongShortAccountRatio` | 5m |
| `lsr_top_account` | Binance `/futures/data/topLongShortAccountRatio` | 5m |
| `lsr_top_position` | Binance `/futures/data/topLongShortPositionRatio` | 5m |
| `lsr_taker` | Binance `/futures/data/takerlongshortRatio` | 5m |
| `liquidation` | Binance websocket `<symbol>@forceOrder` (exchange pushes at most 1 order/s/symbol — a sample, not the full flow) | realtime |
| `mark` | Binance websocket `<symbol>@markPrice@1s` (mark/index/funding), sampled | 1m |
| `depth` | Binance `/fapi/v1/depth?limit=100` order book snapshot | 1m |
| `depth20` | raw top-20 book snapshot from the websocket stream | 10s |
| `depth20_stream` | raw top-20 book, every exchange push (`<symbol>@depth20@100ms`); records flag update-id chain breaks with `"gap": true` | ~100ms |
| `depth_imbalance` | derived imbalance features from the top-20 websocket book | 5s |
| `aggtrade` | Binance websocket `<symbol>@aggTrade`, every aggregated trade | realtime |
| `options_deribit` | Deribit `public/get_book_summary_by_currency` option chain | 1h |

Symbols: BTCUSDT + ETHUSDT + SOLUSDT (Binance), BTC (Deribit). Edit `CONFIG`
in `recorder.py` to change.

## Data format / 資料格式

One JSON line per record, files split by type and UTC date:
`data/<type>/<YYYY-MM-DD>.jsonl`. Previous days are gzipped automatically.

Every record carries two timestamps so point-in-time correctness can be
verified later (每筆都帶兩個時間戳,供未來驗證 point-in-time 正確性):

```json
{
  "ts_local": 1760000000123,   // local receive time, UTC ms
  "ts_event": 1760000000000,   // exchange-reported event time, UTC ms (null if absent)
  "symbol": "BTCUSDT",
  "data": { ... }              // raw exchange payload, unmodified
}
```

Recorded data never enters version control (`data/`, `logs/` are gitignored).
錄到的資料一律不進版控。

## Run / 執行

Dependencies: Python 3.10+, `requests`, `websockets`.

```
python recorder.py                 # run forever (production)
python recorder.py --minutes 30    # stop after 30 minutes (testing)
python check_gaps.py               # report last write time and gaps per type
```

On Windows use UTF-8 mode: `$env:PYTHONUTF8=1` before running.

## Design principles / 設計原則

1. A recorder, not an analyzer. 錄影機,不是分析器。
2. Two timestamps on every record. 每筆雙時間戳。
3. Boring formats: JSON Lines, daily files, gzip. 格式選無聊的。
4. Self-healing: websocket reconnect with exponential backoff, REST retry,
   process restart by systemd. Gaps are acceptable, fake data is not —
   gaps stay detectable via timestamps, never silently filled.
   寧可有缺口,不可有假資料。
5. Minimal dependencies: stdlib + requests + websockets. 依賴最少。

## License

MIT
