"""
PART 1.2 - HBase loader (wide-column model) via the Thrift gateway (happybase).

Loads
  * user_sessions   <- data/clean_jsonl/sessions          (row key user_id#reverse_ts#session_id)
  * product_metrics <- data/clean_jsonl/product_daily_metrics (row key product_id#yyyymmdd)
                       (produced by src/spark/02_product_daily_metrics.py)

Tables must exist first (src/hbase/schema.hbase defines pre-splits, compression, bloom filters).
If they don't, a fallback creates them through Thrift without pre-splits.

Usage: python load_hbase.py [--limit N] [--batch 2000]
"""
import argparse
import calendar
import glob
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import DATA_DIR, HBASE_HOST, HBASE_PORT, Timer, reverse_ts  # noqa: E402

import happybase  # noqa: E402

JSONL = os.path.join(DATA_DIR, "clean_jsonl")


def read_jsonl(table):
    for path in sorted(glob.glob(os.path.join(JSONL, table, "*.json"))):
        with open(path, encoding="utf-8") as f:
            for line in f:
                yield json.loads(line)


def b(v):
    return b"" if v is None else str(v).encode()


def epoch(s):
    return calendar.timegm(datetime.fromisoformat(s).timetuple())


def session_row(s):
    key = f"{s['user_id']}#{reverse_ts(epoch(s['start_time']))}#{s['session_id']}"
    cart = s.get("cart_contents") or {}
    cols = {
        b"s:session_id": b(s["session_id"]),
        b"s:start_time": b(s["start_time"]),
        b"s:end_time": b(s["end_time"]),
        b"s:duration_seconds": b(s["duration_seconds"]),
        b"s:device_type": b(s["device_type"]),
        b"s:device_os": b(s["device_os"]),
        b"s:browser": b(s["browser"]),
        b"s:city": b((s.get("geo_data") or {}).get("city")),
        b"s:state": b((s.get("geo_data") or {}).get("state")),
        b"s:referrer": b(s["referrer"]),
        b"s:conversion_status": b(s["conversion_status"]),
        b"s:num_page_views": b(s["num_page_views"]),
        b"s:num_products_viewed": b(s["num_products_viewed"]),
        b"s:cart_value": b(round(sum(c["quantity"] * c["price"] for c in cart.values()), 2)),
    }
    for i, pv in enumerate(s.get("page_views") or []):
        cols[f"pv:{i:03d}".encode()] = "|".join(
            "" if pv.get(k) is None else str(pv[k])
            for k in ("timestamp", "page_type", "product_id", "category_id", "view_duration")).encode()
    for pid, c in cart.items():
        cols[f"c:{pid}".encode()] = f"{c['quantity']}|{c['price']}".encode()
    return key.encode(), cols


def ensure_tables(conn):
    existing = {t.decode() for t in conn.tables()}
    if "user_sessions" not in existing:
        print("   (schema.hbase not applied - creating user_sessions via Thrift, no pre-split)")
        conn.create_table("user_sessions", {"s": dict(max_versions=1, in_memory=True),
                                            "pv": dict(max_versions=1), "c": dict(max_versions=1)})
    if "product_metrics" not in existing:
        conn.create_table("product_metrics", {"m": dict(max_versions=1)})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="max sessions to load (0 = all)")
    ap.add_argument("--batch", type=int, default=2000)
    a = ap.parse_args()

    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=120000)
    ensure_tables(conn)

    with Timer("user_sessions"):
        tbl = conn.table("user_sessions")
        n = 0
        with tbl.batch(batch_size=a.batch) as bt:
            for s in read_jsonl("sessions"):
                if a.limit and n >= a.limit:
                    break
                k, cols = session_row(s)
                bt.put(k, cols)
                n += 1
                if n % 50000 == 0:
                    print(f"     {n:,} sessions")
        print(f"     loaded {n:,} sessions")

    with Timer("product_metrics"):
        tbl = conn.table("product_metrics")
        n = 0
        with tbl.batch(batch_size=a.batch) as bt:
            for m in read_jsonl("product_daily_metrics"):
                key = f"{m['product_id']}#{m['date'].replace('-', '')[:8]}".encode()
                bt.put(key, {f"m:{c}".encode(): b(m.get(c, 0))
                             for c in ("views", "unique_sessions", "cart_adds", "purchases",
                                       "units", "revenue")})
                n += 1
        print(f"     loaded {n:,} product-day rows")
    conn.close()


if __name__ == "__main__":
    main()
