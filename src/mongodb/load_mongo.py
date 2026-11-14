"""
PART 1.1 - MongoDB loader (document model).

Collections & modelling decisions
---------------------------------
categories    : hierarchy kept as ONE document per category with an embedded `subcategories`
                array (small, bounded, always read together).
products      : catalog document; embeds a *denormalised* copy of {category, subcategory, margin}
                (extended-reference pattern) so catalogue pages / revenue queries need no $lookup;
                embeds `price_history` (bounded array, always shown with the product).
users         : profile + embedded `purchase_summary` (computed pattern) - order count, spend,
                AOV, first/last purchase, top categories, preferred payment. This is the data a
                profile page / CRM tool reads every time, so we pre-compute it at load time instead
                of aggregating all transactions on each read.
transactions  : one document per order with embedded line `items` (items never live without
                their order; atomic single-document writes). Each item carries category_id
                (denormalised) so revenue-by-category is a single-collection pipeline.

Sessions are intentionally NOT stored here - high-volume, append-only, time-ordered clickstream
goes to HBase (see ../hbase). See the report for the full justification.

Usage:  python load_mongo.py [--limit-txn N]
"""
import argparse
import glob
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import DATA_DIR, MONGO_DB, MONGO_URI, Timer  # noqa: E402

from pymongo import ASCENDING, DESCENDING, InsertOne, MongoClient  # noqa: E402

JSONL = os.path.join(DATA_DIR, "clean_jsonl")


def read_jsonl(table):
    for path in sorted(glob.glob(os.path.join(JSONL, table, "*.json"))):
        with open(path, encoding="utf-8") as f:
            for line in f:
                yield json.loads(line)


def to_dt(s):
    return datetime.fromisoformat(s) if s else None


def bulk_insert(coll, docs, batch=5000):
    ops, n = [], 0
    for d in docs:
        ops.append(InsertOne(d))
        if len(ops) >= batch:
            n += coll.bulk_write(ops, ordered=False).inserted_count
            ops = []
    if ops:
        n += coll.bulk_write(ops, ordered=False).inserted_count
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-txn", type=int, default=0, help="0 = load all transactions")
    a = ap.parse_args()

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    client.admin.command("ping")
    db = client[MONGO_DB]
    for c in ["categories", "products", "users", "transactions"]:
        db[c].drop()

    # ---------------- categories ----------------
    with Timer("categories"):
        cats = defaultdict(lambda: {"subcategories": []})
        sub_lookup = {}
        for s in read_jsonl("subcategories"):
            c = cats[s["category_id"]]
            c["_id"] = s["category_id"]
            c["name"] = s["category_name"]
            c["subcategories"].append({"subcategory_id": s["subcategory_id"],
                                       "name": s["subcategory_name"],
                                       "profit_margin": s["profit_margin"]})
            sub_lookup[s["subcategory_id"]] = s
        db.categories.insert_many(list(cats.values()))

    # ---------------- products ----------------
    with Timer("products"):
        prod_cat = {}

        def products():
            for p in read_jsonl("products"):
                prod_cat[p["product_id"]] = p["category_id"]
                yield {
                    "_id": p["product_id"],
                    "name": p["name"],
                    "category": {"id": p["category_id"], "name": p.get("category_name")},
                    "subcategory": {"id": p["subcategory_id"], "name": p.get("subcategory_name"),
                                    "profit_margin": p.get("profit_margin")},
                    "base_price": p["base_price"],
                    "inventory": {"current_stock": p["current_stock"], "is_active": p["is_active"]},
                    "price_history": [{"price": h["price"], "date": to_dt(h["date"])}
                                      for h in (p.get("price_history") or [])],
                    "creation_date": to_dt(p["creation_date"]),
                }
        n = bulk_insert(db.products, products())
        print(f"     {n:,} products")

    # ---------------- transactions (+ build user summaries in one pass) ----------------
    with Timer("transactions"):
        summary = defaultdict(lambda: {"orders": 0, "spent": 0.0, "first": None, "last": None,
                                       "cats": Counter(), "pay": Counter()})

        def txns():
            for i, t in enumerate(read_jsonl("transactions")):
                if a.limit_txn and i >= a.limit_txn:
                    break
                ts = to_dt(t["timestamp"])
                items = [{"product_id": it["product_id"], "category_id": prod_cat.get(it["product_id"]),
                          "quantity": it["quantity"], "unit_price": it["unit_price"],
                          "subtotal": it["subtotal"]} for it in t["items"]]
                if t["is_revenue"]:
                    s = summary[t["user_id"]]
                    s["orders"] += 1
                    s["spent"] += t["total"]
                    s["first"] = ts if s["first"] is None or ts < s["first"] else s["first"]
                    s["last"] = ts if s["last"] is None or ts > s["last"] else s["last"]
                    s["pay"][t["payment_method"]] += 1
                    for it in items:
                        s["cats"][it["category_id"]] += it["subtotal"]
                yield {"_id": t["transaction_id"], "session_id": t.get("session_id"),
                       "user_id": t["user_id"], "timestamp": ts, "items": items,
                       "subtotal": t["subtotal"], "discount": t["discount"], "total": t["total"],
                       "payment_method": t["payment_method"], "status": t["status"]}
        n = bulk_insert(db.transactions, txns())
        print(f"     {n:,} transactions")

    # ---------------- users with embedded purchase summary ----------------
    with Timer("users"):
        def users():
            for u in read_jsonl("users"):
                s = summary.get(u["user_id"])
                doc = {"_id": u["user_id"],
                       "geo_data": {"city": u["city"], "state": u["state"], "country": u["country"]},
                       "registration_date": to_dt(u["registration_date"]),
                       "last_active": to_dt(u["last_active"])}
                if s:
                    doc["purchase_summary"] = {
                        "total_orders": s["orders"], "total_spent": round(s["spent"], 2),
                        "avg_order_value": round(s["spent"] / s["orders"], 2),
                        "first_purchase": s["first"], "last_purchase": s["last"],
                        "top_categories": [c for c, _ in s["cats"].most_common(3)],
                        "preferred_payment": s["pay"].most_common(1)[0][0]}
                else:
                    doc["purchase_summary"] = {"total_orders": 0, "total_spent": 0.0}
                yield doc
        n = bulk_insert(db.users, users())
        print(f"     {n:,} users")

    # ---------------- indexes that back the query patterns ----------------
    with Timer("indexes"):
        db.transactions.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])  # order history
        db.transactions.create_index([("timestamp", ASCENDING)])                         # period revenue
        db.transactions.create_index([("items.product_id", ASCENDING)])                  # multikey: product sales
        db.transactions.create_index([("session_id", ASCENDING)], sparse=True)           # session -> order
        db.products.create_index([("category.id", ASCENDING), ("subcategory.id", ASCENDING)])
        db.users.create_index([("purchase_summary.total_spent", DESCENDING)])
        db.users.create_index([("geo_data.state", ASCENDING)])

    for c in ["categories", "products", "users", "transactions"]:
        print(f"   {c:<13} {db[c].estimated_document_count():>9,} docs")
    print("   sample user:", db.users.find_one({"purchase_summary.total_orders": {"$gt": 2}}))


if __name__ == "__main__":
    main()
