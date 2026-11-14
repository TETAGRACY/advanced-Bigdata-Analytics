"""
dataset_generator.py
--------------------
Synthetic e-commerce dataset generator for the AUCA Big Data Analytics Final Project.

Produces the entities described in the project brief (same field names / nesting):
    users.json, categories.json, products.json,
    sessions_0.json, sessions_1.json, ...   (chunked because of volume)
    transactions.json

Design notes
  * 90 days of activity ending on END_DATE.
  * Realistic skew so that analytics produce meaningful patterns:
      - Zipf-like category & product popularity
      - per-user "propensity" (a minority of heavy buyers)
      - device type and referrer influence conversion
      - products from the same subcategory tend to be bought together (affinity)
      - a mid-period promotion week with higher traffic and discounts
  * ~1% deliberate "dirty" records (missing geo fields, inconsistent casing,
    string-typed numbers, duplicate transactions) so the Spark cleaning stage
    has real work to do.

Usage:
    python dataset_generator.py --out ../data --users 10000 --products 5000 \
        --sessions 200000 --chunk 50000 --seed 42
Requirements: pip install faker pandas
"""
import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

END_DATE = datetime(2025, 4, 15, 23, 59, 59)
TIMESPAN_DAYS = 90
START_DATE = END_DATE - timedelta(days=TIMESPAN_DAYS)
PROMO_START = datetime(2025, 3, 2)                  # one "spring sale" week (2-8 March 2025)
PROMO_END = PROMO_START + timedelta(days=7)

DEVICE_PROFILES = [
    ({"type": "mobile", "os": "iOS", "browser": "Safari"}, 0.25),
    ({"type": "mobile", "os": "Android", "browser": "Chrome"}, 0.25),
    ({"type": "desktop", "os": "Windows", "browser": "Chrome"}, 0.20),
    ({"type": "desktop", "os": "macOS", "browser": "Safari"}, 0.10),
    ({"type": "desktop", "os": "Windows", "browser": "Edge"}, 0.08),
    ({"type": "tablet", "os": "iPadOS", "browser": "Safari"}, 0.07),
    ({"type": "tablet", "os": "Android", "browser": "Chrome"}, 0.05),
]
# conversion multipliers by device type and referrer (drives funnel insights)
DEVICE_CONV = {"desktop": 1.35, "mobile": 0.80, "tablet": 1.0}
REFERRERS = [("direct", 0.25), ("search_engine", 0.30), ("social_media", 0.20),
             ("email", 0.10), ("affiliate", 0.08), ("paid_ads", 0.07)]
REFERRER_CONV = {"direct": 1.2, "search_engine": 1.0, "social_media": 0.6,
                 "email": 1.6, "affiliate": 0.9, "paid_ads": 0.8}
PAYMENT_METHODS = [("credit_card", 0.45), ("paypal", 0.22), ("debit_card", 0.15),
                   ("apple_pay", 0.08), ("google_pay", 0.06), ("bank_transfer", 0.04)]
TXN_STATUS = [("completed", 0.62), ("shipped", 0.18), ("delivered", 0.12),
              ("refunded", 0.05), ("cancelled", 0.03)]


def wchoice(rng, pairs):
    items, weights = zip(*pairs)
    return rng.choices(items, weights=weights, k=1)[0]


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def rand_dt(rng, start, end):
    return start + timedelta(seconds=rng.randint(0, int((end - start).total_seconds())))


# --------------------------------------------------------------------------- #
def gen_categories(fake, rng, n):
    cats = []
    for i in range(n):
        subs = []
        for j in range(rng.randint(3, 6)):
            subs.append({
                "subcategory_id": f"sub_{i:03d}_{j:02d}",
                "name": fake.catch_phrase().title(),
                "profit_margin": round(rng.uniform(0.10, 0.40), 2),
            })
        cats.append({"category_id": f"cat_{i:03d}", "name": fake.company(),
                     "subcategories": subs})
    return cats


def gen_products(fake, rng, n, categories):
    # Zipf-like weight per category -> some categories dominate the catalogue/traffic
    cat_weights = [1.0 / (k + 1) ** 0.8 for k in range(len(categories))]
    products = []
    for i in range(n):
        cat = rng.choices(categories, weights=cat_weights, k=1)[0]
        sub = rng.choice(cat["subcategories"])
        base = round(rng.lognormvariate(3.6, 0.8), 2)          # median ~ $37
        base = min(max(base, 2.99), 1999.99)
        created = rand_dt(rng, START_DATE - timedelta(days=200), END_DATE - timedelta(days=10))
        history, price, d = [], round(base * rng.uniform(1.0, 1.3), 2), created
        for _ in range(rng.randint(1, 4)):
            history.append({"price": price, "date": iso(d.replace(hour=0, minute=0, second=0))})
            d = d + timedelta(days=rng.randint(10, 60))
            if d > END_DATE:
                break
            price = round(base * rng.uniform(0.85, 1.15), 2)
        current_price = history[-1]["price"]
        stock = rng.choice([0] * 1 + [rng.randint(1, 500) for _ in range(19)])
        products.append({
            "product_id": f"prod_{i:05d}",
            "name": fake.catch_phrase().title(),
            "category_id": cat["category_id"],
            "subcategory_id": sub["subcategory_id"],
            "base_price": current_price,
            "current_stock": stock,
            "is_active": stock > 0 and rng.random() > 0.03,
            "price_history": history,
            "creation_date": iso(created.replace(hour=0, minute=0, second=0)),
        })
    return products


def gen_users(fake, rng, n):
    users = []
    for i in range(n):
        reg = rand_dt(rng, START_DATE - timedelta(days=300), END_DATE - timedelta(days=5))
        geo = {"city": fake.city(), "state": fake.state_abbr(), "country": "US"}
        if rng.random() < 0.01:                     # dirty: missing state
            geo["state"] = None
        users.append({
            "user_id": f"user_{i:06d}",
            "geo_data": geo,
            "registration_date": iso(reg),
            "last_active": iso(reg),                # updated after sessions are generated
        })
    return users


# --------------------------------------------------------------------------- #
def build_session(rng, fake, user, uprops, products, by_sub, prod_weights, active_idx):
    """Create one browsing session (page_views journey + cart + conversion)."""
    reg = datetime.fromisoformat(user["registration_date"])
    lo = max(START_DATE, reg)
    # promo week gets ~2x traffic: sample start date with rejection
    while True:
        start = rand_dt(rng, lo, END_DATE - timedelta(hours=2))
        in_promo = PROMO_START <= start < PROMO_END
        if in_promo or rng.random() < 0.55:
            break
    # evening peak
    if rng.random() < 0.4:
        start = start.replace(hour=rng.choice([18, 19, 20, 21, 22]))

    device = wchoice(rng, DEVICE_PROFILES)
    referrer = wchoice(rng, REFERRERS)
    page_views, viewed, cart = [], [], {}
    t = start
    pv = lambda ptype, pid=None, cid=None: {
        "timestamp": iso(t), "page_type": ptype, "product_id": pid,
        "category_id": cid, "view_duration": rng.randint(5, 240)}

    page_views.append(pv("home"))
    n_steps = max(1, int(rng.expovariate(1 / 6)))
    # session "interest": a subcategory anchor, drives co-viewing/co-buying
    anchor = products[rng.choices(active_idx, weights=prod_weights, k=1)[0]]
    for _ in range(n_steps):
        t = t + timedelta(seconds=page_views[-1]["view_duration"] + rng.randint(1, 20))
        r = rng.random()
        if r < 0.20:
            page_views.append(pv("category_listing", None, anchor["category_id"]))
        elif r < 0.30:
            page_views.append(pv("search"))
        else:
            if rng.random() < 0.6:   # stay within the anchor subcategory
                p = rng.choice(by_sub[anchor["subcategory_id"]])
            else:
                p = products[rng.choices(active_idx, weights=prod_weights, k=1)[0]]
            page_views.append(pv("product_detail", p["product_id"], p["category_id"]))
            if p["product_id"] not in viewed:
                viewed.append(p["product_id"])
            add_p = 0.18 * uprops["propensity"] * DEVICE_CONV[device["type"]]
            if p["is_active"] and rng.random() < add_p:
                q = rng.choices([1, 2, 3, 4], weights=[70, 20, 7, 3])[0]
                item = cart.setdefault(p["product_id"], {"quantity": 0, "price": p["base_price"]})
                item["quantity"] += q

    status = "browsed"
    if cart:
        t = t + timedelta(seconds=page_views[-1]["view_duration"] + 3)
        page_views.append(pv("cart"))
        conv_p = (0.38 * uprops["propensity"] * DEVICE_CONV[device["type"]]
                  * REFERRER_CONV[referrer] * (1.3 if in_promo else 1.0))
        if rng.random() < min(conv_p, 0.92):
            t = t + timedelta(seconds=page_views[-1]["view_duration"] + 3)
            page_views.append(pv("checkout"))
            status = "converted"
        else:
            status = "abandoned"

    end = t + timedelta(seconds=page_views[-1]["view_duration"])
    geo = dict(user["geo_data"])
    geo["ip_address"] = fake.ipv4_private()
    sess = {
        "session_id": "sess_" + uuid.UUID(int=rng.getrandbits(128)).hex[:10],
        "user_id": user["user_id"],
        "start_time": iso(start),
        "end_time": iso(end),
        "duration_seconds": int((end - start).total_seconds()),
        "geo_data": geo,
        "device_profile": dict(device),
        "viewed_products": viewed,
        "page_views": page_views,
        "cart_contents": cart,
        "conversion_status": status,
        "referrer": referrer,
    }
    # ---- dirty data injection (~1%) ----
    r = rng.random()
    if r < 0.004:
        sess["device_profile"]["type"] = sess["device_profile"]["type"].upper()
    elif r < 0.007:
        sess["duration_seconds"] = str(sess["duration_seconds"])
    elif r < 0.010:
        sess["referrer"] = None
    return sess, in_promo


def make_transaction(rng, session, in_promo, user_id, ts, items_src):
    items, subtotal = [], 0.0
    for pid, (qty, price) in items_src.items():
        st = round(qty * price, 2)
        items.append({"product_id": pid, "quantity": qty, "unit_price": price, "subtotal": st})
        subtotal += st
    subtotal = round(subtotal, 2)
    disc_rate = rng.choice([0, 0, 0, 0.05, 0.10]) if not in_promo else rng.choice([0.10, 0.15, 0.20])
    discount = round(subtotal * disc_rate, 2)
    return {
        "transaction_id": "txn_" + uuid.UUID(int=rng.getrandbits(128)).hex[:12],
        "session_id": session,
        "user_id": user_id,
        "timestamp": iso(ts),
        "items": items,
        "subtotal": subtotal,
        "discount": discount,
        "total": round(subtotal - discount, 2),
        "payment_method": wchoice(rng, PAYMENT_METHODS),
        "status": wchoice(rng, TXN_STATUS),
    }


# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "..", "data", "raw"))
    ap.add_argument("--users", type=int, default=10000)
    ap.add_argument("--products", type=int, default=5000)
    ap.add_argument("--categories", type=int, default=25)
    ap.add_argument("--sessions", type=int, default=200000)
    ap.add_argument("--chunk", type=int, default=50000)
    ap.add_argument("--direct-txn-ratio", type=float, default=0.15,
                    help="extra transactions without a session (e.g. app/phone orders)")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()

    rng = random.Random(a.seed)
    fake = Faker("en_US")
    Faker.seed(a.seed)
    os.makedirs(a.out, exist_ok=True)

    def dump(name, obj):
        with open(os.path.join(a.out, name), "w", encoding="utf-8") as f:
            json.dump(obj, f)
        print(f"  wrote {name} ({len(obj):,} records)")

    print("Generating categories / products / users ...")
    categories = gen_categories(fake, rng, a.categories)
    products = gen_products(fake, rng, a.products, categories)
    users = gen_users(fake, rng, a.users)

    by_sub = {}
    for p in products:
        by_sub.setdefault(p["subcategory_id"], []).append(p)
    active_idx = [i for i, p in enumerate(products)]
    # product popularity: Zipf over a random permutation
    perm = list(range(len(products)))
    rng.shuffle(perm)
    prod_weights = [0.0] * len(products)
    for rank, idx in enumerate(perm):
        prod_weights[idx] = 1.0 / (rank + 1) ** 0.7

    # user propensity: most users casual, ~10% heavy buyers; activity skewed (Pareto) but capped
    # at 15x so no single "user" generates thousands of sessions (that would be a bot, not a customer)
    uprops = {u["user_id"]: {"propensity": rng.choice([0.5] * 5 + [1.0] * 4 + [2.2]),
                             "activity": min(rng.paretovariate(1.6), 15.0)} for u in users}
    user_w = [uprops[u["user_id"]]["activity"] for u in users]

    print(f"Generating {a.sessions:,} sessions ...")
    transactions, chunk, chunk_no = [], [], 0
    last_active = {}
    for _ in range(a.sessions):
        user = rng.choices(users, weights=user_w, k=1)[0]
        s, in_promo = build_session(rng, fake, user, uprops[user["user_id"]],
                                    products, by_sub, prod_weights, active_idx)
        chunk.append(s)
        last_active[user["user_id"]] = max(last_active.get(user["user_id"], ""), s["end_time"])
        if s["conversion_status"] == "converted":
            items = {pid: (c["quantity"], c["price"]) for pid, c in s["cart_contents"].items()}
            transactions.append(make_transaction(
                rng, s["session_id"], in_promo, s["user_id"],
                datetime.fromisoformat(s["end_time"]), items))
        if len(chunk) >= a.chunk:
            dump(f"sessions_{chunk_no}.json", chunk)
            chunk, chunk_no = [], chunk_no + 1
    if chunk:
        dump(f"sessions_{chunk_no}.json", chunk)

    # transactions with no session (session_id = null), e.g. phone / app orders
    n_direct = int(len(transactions) * a.direct_txn_ratio)
    for _ in range(n_direct):
        user = rng.choices(users, weights=user_w, k=1)[0]
        reg = datetime.fromisoformat(user["registration_date"])
        ts = rand_dt(rng, max(START_DATE, reg), END_DATE)
        anchor = products[rng.choices(active_idx, weights=prod_weights, k=1)[0]]
        pool = by_sub[anchor["subcategory_id"]]
        picks = {anchor["product_id"]: (1, anchor["base_price"])}
        for _ in range(rng.choices([0, 1, 2], weights=[55, 30, 15])[0]):
            p = rng.choice(pool)
            picks[p["product_id"]] = (rng.randint(1, 3), p["base_price"])
        transactions.append(make_transaction(rng, None, PROMO_START <= ts < PROMO_END,
                                             user["user_id"], ts, picks))
        last_active[user["user_id"]] = max(last_active.get(user["user_id"], ""), iso(ts))

    # dirty: inconsistent casing + a few exact duplicate transactions
    for t in rng.sample(transactions, k=max(1, len(transactions) // 200)):
        t["payment_method"] = t["payment_method"].upper()
    transactions.extend(rng.sample(transactions, k=max(1, len(transactions) // 300)))
    rng.shuffle(transactions)

    for u in users:
        if u["user_id"] in last_active:
            u["last_active"] = last_active[u["user_id"]]

    dump("users.json", users)
    dump("categories.json", categories)
    dump("products.json", products)
    dump("transactions.json", transactions)
    print("Done.")


if __name__ == "__main__":
    main()
