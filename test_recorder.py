"""Minimal unit tests for the pure logic in recorder.py.

Run with:  python -m unittest test_recorder -v
No network, no filesystem writes: WRITER is swapped for a fake.
"""

import unittest

import recorder


class FakeWriter:
    def __init__(self):
        self.records = []

    def write(self, dtype, record):
        self.records.append((dtype, record))


class ComputeImbalanceTest(unittest.TestCase):
    def test_hand_computed_book(self):
        bids = [["100", "2"], ["99", "3"]]     # descending
        asks = [["101", "1"], ["102", "4"]]    # ascending
        out = recorder.compute_imbalance(bids, asks)
        self.assertAlmostEqual(out["mid"], 100.5)
        self.assertAlmostEqual(out["spread"], 1.0)
        self.assertAlmostEqual(out["bid_qty_5"], 5.0)
        self.assertAlmostEqual(out["ask_qty_5"], 5.0)
        self.assertAlmostEqual(out["imbalance_5"], 0.0)
        # within 0.5% of mid 100.5 -> [99.9975, 101.0025]: bid 100 only, ask 101 only
        self.assertAlmostEqual(out["bid_within_0p5pct"], 2.0)
        self.assertAlmostEqual(out["ask_within_0p5pct"], 1.0)
        # within 1% -> [99.495, 101.505]: bid 100 only (99 is 1.49% below mid)
        self.assertAlmostEqual(out["bid_within_1pct"], 2.0)
        self.assertAlmostEqual(out["ask_within_1pct"], 1.0)

    def test_empty_side_returns_none(self):
        self.assertIsNone(recorder.compute_imbalance([], [["101", "1"]]))
        self.assertIsNone(recorder.compute_imbalance([["100", "1"]], []))


class StaleFlushTest(unittest.TestCase):
    """A frozen order book must NOT keep producing records (the gap must
    stay visible in the data, per the recorder's design)."""

    def setUp(self):
        self._real_writer = recorder.WRITER
        recorder.WRITER = FakeWriter()

    def tearDown(self):
        recorder.WRITER = self._real_writer

    def _book(self, age_ms):
        ts_recv = recorder.now_ms() - age_ms
        return {"BTCUSDT": (ts_recv, ts_recv, [["100", "2"]], [["101", "1"]])}

    def test_fresh_book_is_written(self):
        recorder._flush_imbalance(self._book(age_ms=1_000), max_age_ms=10_000)
        recorder._flush_depth_raw(self._book(age_ms=1_000), max_age_ms=60_000)
        dtypes = [d for d, _ in recorder.WRITER.records]
        self.assertEqual(dtypes, ["depth_imbalance", "depth20"])

    def test_stale_book_is_skipped(self):
        recorder._flush_imbalance(self._book(age_ms=30_000), max_age_ms=10_000)
        recorder._flush_depth_raw(self._book(age_ms=120_000), max_age_ms=60_000)
        self.assertEqual(recorder.WRITER.records, [])


if __name__ == "__main__":
    unittest.main()
