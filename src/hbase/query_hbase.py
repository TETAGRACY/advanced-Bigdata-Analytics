"""
PART 1.2 - HBase query examples (happybase client).

  Q1  All sessions of a user, newest first           -> prefix scan  user_id#
  Q2  Last N sessions of a user                       -> prefix scan + limit (reverse ts = newest first)
  Q3  User sessions in a time window                  -> bounded range scan on reverse-ts row keys
  Q4  Replay one session's click journey              -> single-row GET of CF 'pv' only
  Q5  Product daily performance & view->purchase rate -> range scan product_id#yyyymmdd

Usage: python query_hbase.py [--user user_000042] [--product prod_00010]
Output is printed and also saved to output/hbase_query_results.txt
"""
import argparse
import calendar
import io
import os
import sys
import time
from contextlib import redirect_stdout
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import HBASE_HOST, HBASE_PORT, OUTPUT_DIR, reverse_ts  # noqa: E402

import happybase  # noqa: E402


def d(v):
    return v.decode() if isinstance(v, bytes) else v


def ep(y, m, dd):
    return calendar.timegm(datetime(y, m, dd).timetuple())


def timed(label, fn):
    t0 = time.perf_counter()
    out = fn()
    print(f"\n=== {label}  [{(time.perf_counter() - t0) * 1000:.1f} ms, {len(out)} rows]")
    return out


def run(conn, user, product):
    us = conn.table("user_sessions")
    pm = conn.table("product_metrics")
    summary_cols = [b"s:start_time", b"s:duration_seconds", b"s:device_type", b"s:referrer",
                    b"s:conversion_status", b"s:num_page_views", b"s:cart_value"]

    rows = timed(f"Q1 all sessions of {user} (prefix scan, CF 's' only)",
                 lambda: list(us.scan(row_prefix=f"{user}#".encode(), columns=summary_cols)))
    for k, v in rows[:10]:
        print(f"  {d(k):<45} {d(v[b's:start_time'])}  {d(v[b's:device_type']):<8}"
              f" {d(v[b's:referrer']):<14} {d(v[b's:conversion_status']):<10} pv={d(v[b's:num_page_views'])}"
              f" cart=${d(v[b's:cart_value'])}")
    if len(rows) > 10:
        print(f"  ... {len(rows) - 10} more")
    conv = sum(1 for _, v in rows if v[b"s:conversion_status"] == b"converted")
    if rows:
        print(f"  -> {len(rows)} sessions, {conv} converted ({100 * conv / len(rows):.1f}%)")

    last3 = timed(f"Q2 last 3 sessions of {user} (prefix scan + limit=3; reverse ts => newest first)",
                  lambda: list(us.scan(row_prefix=f"{user}#".encode(), columns=[b"s:start_time"], limit=3)))
    for k, v in last3:
        print(" ", d(k), d(v[b"s:start_time"]))

    start = f"{user}#{reverse_ts(ep(2025, 4, 1))}".encode()   # newer bound comes first
    stop = f"{user}#{reverse_ts(ep(2025, 3, 1))}".encode()
    march = timed(f"Q3 {user} sessions during March 2025 (range scan {d(start)} .. {d(stop)})",
                  lambda: list(us.scan(row_start=start, row_stop=stop,
                                       columns=[b"s:start_time", b"s:conversion_status"])))
    for k, v in march:
        print(" ", d(v[b"s:start_time"]), d(v[b"s:conversion_status"]))

    if rows:
        key = rows[0][0]
        journey = timed(f"Q4 replay journey of {d(key)} (GET, CF 'pv' + 'c')",
                        lambda: sorted(us.row(key, columns=[b"pv", b"c"]).items()))
        for q, v in journey:
            print(f"  {d(q):<18} {d(v)}")

    days = timed(f"Q5 {product} daily metrics (range scan {product}#20250101..{product}#20250501)",
                 lambda: list(pm.scan(row_start=f"{product}#20250101".encode(),
                                      row_stop=f"{product}#20250501".encode())))
    tv = sum(int(v[b"m:views"]) for _, v in days)
    tc = sum(int(v[b"m:cart_adds"]) for _, v in days)
    tp = sum(int(v[b"m:purchases"]) for _, v in days)
    tr = sum(float(v[b"m:revenue"]) for _, v in days)
    for k, v in days[:7]:
        print(f"  {d(k)}  views={d(v[b'm:views'])} carts={d(v[b'm:cart_adds'])} "
              f"purchases={d(v[b'm:purchases'])} revenue={d(v[b'm:revenue'])}")
    if tv:
        print(f"  -> {len(days)} active days, views={tv}, cart adds={tc}, purchases={tp}, "
              f"revenue=${tr:,.2f}; view->cart {100 * tc / tv:.1f}%, view->purchase {100 * tp / tv:.1f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--user", default=None)
    ap.add_argument("--product", default=None)
    a = ap.parse_args()
    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=120000)

    # default: pick an active user / product from the data itself
    user = a.user or d(next(conn.table("user_sessions").scan(row_prefix=b"user_0000", limit=1))[0]).split("#")[0]
    product = a.product
    if not product:   # top seller found by the MongoDB pipeline A3, if it has been run
        try:
            import csv
            with open(os.path.join(OUTPUT_DIR, "mongo_A3_top_products.csv")) as f:
                product = next(csv.DictReader(f))["product_id"]
        except (OSError, StopIteration, KeyError):
            product = "prod_00010"

    buf = io.StringIO()
    with redirect_stdout(buf):
        run(conn, user, product)
    print(buf.getvalue())
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, "hbase_query_results.txt"), "w", encoding="utf-8") as f:
        f.write(buf.getvalue())


if __name__ == "__main__":
    main()
