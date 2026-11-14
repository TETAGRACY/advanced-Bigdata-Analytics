"""
PART 1.1 - MongoDB aggregation pipelines (non-trivial analytics).

  A1  Revenue & gross-profit analytics by category and month
      ($match on indexed timestamp -> $unwind items -> $lookup product for subcategory margin
       -> $group by category/month -> $lookup category name -> $sort)
  A2  Customer segmentation by purchase frequency and spend (RFM-lite)
      ($addFields recency -> $bucket by order count -> $facet: segment stats + spend tiers by state)
  A3  Product popularity: top sellers by units and revenue, plus promo-week lift
      ($unwind -> $group -> $facet with two rankings -> $lookup product names)

Results are printed and written to output/mongo_A*.csv. Pipelines are also exported to
output/mongo_pipelines.json so they can be pasted into mongosh / Compass.
"""
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import MONGO_DB, MONGO_URI, OUTPUT_DIR, Timer  # noqa: E402

import pandas as pd  # noqa: E402
from pymongo import MongoClient  # noqa: E402

REVENUE_STATUSES = ["completed", "shipped", "delivered"]
AS_OF = datetime(2025, 4, 16)

# ----------------------------------------------------------------------------------------------
A1_REVENUE_BY_CATEGORY_MONTH = [
    {"$match": {"status": {"$in": REVENUE_STATUSES},
                "timestamp": {"$gte": datetime(2025, 1, 1), "$lt": datetime(2025, 5, 1)}}},
    {"$unwind": "$items"},
    # discount is order-level -> allocate it to items proportionally to their subtotal
    {"$addFields": {"net_item": {"$multiply": ["$items.subtotal",
                                               {"$cond": [{"$gt": ["$subtotal", 0]},
                                                          {"$divide": ["$total", "$subtotal"]}, 1]}]}}},
    {"$lookup": {"from": "products", "localField": "items.product_id", "foreignField": "_id",
                 "pipeline": [{"$project": {"subcategory.profit_margin": 1}}], "as": "p"}},
    {"$unwind": "$p"},
    {"$group": {"_id": {"category": "$items.category_id",
                        "month": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}}},
                "revenue": {"$sum": "$net_item"},
                "gross_profit": {"$sum": {"$multiply": ["$net_item", "$p.subcategory.profit_margin"]}},
                "units": {"$sum": "$items.quantity"},
                "orders": {"$addToSet": "$_id"}}},
    {"$lookup": {"from": "categories", "localField": "_id.category", "foreignField": "_id",
                 "pipeline": [{"$project": {"name": 1}}], "as": "c"}},
    {"$project": {"_id": 0, "category_id": "$_id.category", "month": "$_id.month",
                  "category_name": {"$first": "$c.name"},
                  "revenue": {"$round": ["$revenue", 2]},
                  "gross_profit": {"$round": ["$gross_profit", 2]},
                  "margin_pct": {"$round": [{"$multiply": [{"$divide": ["$gross_profit", "$revenue"]}, 100]}, 1]},
                  "units": 1, "orders": {"$size": "$orders"}}},
    {"$sort": {"month": 1, "revenue": -1}},
]

A2_CUSTOMER_SEGMENTS = [
    {"$addFields": {
        "orders": {"$ifNull": ["$purchase_summary.total_orders", 0]},
        "spent": {"$ifNull": ["$purchase_summary.total_spent", 0]},
        "recency_days": {"$cond": [
            {"$ifNull": ["$purchase_summary.last_purchase", False]},
            {"$dateDiff": {"startDate": "$purchase_summary.last_purchase", "endDate": AS_OF, "unit": "day"}},
            None]}}},
    {"$facet": {
        "by_frequency": [
            {"$bucket": {"groupBy": "$orders", "boundaries": [0, 1, 2, 4, 7, 1000], "default": "other",
                         "output": {"users": {"$sum": 1}, "total_revenue": {"$sum": "$spent"},
                                    "avg_spent": {"$avg": "$spent"},
                                    "avg_recency_days": {"$avg": "$recency_days"}}}},
            {"$addFields": {"segment": {"$switch": {"branches": [
                {"case": {"$eq": ["$_id", 0]}, "then": "0 - Never purchased"},
                {"case": {"$eq": ["$_id", 1]}, "then": "1 - One-time"},
                {"case": {"$eq": ["$_id", 2]}, "then": "2 - Occasional (2-3)"},
                {"case": {"$eq": ["$_id", 4]}, "then": "3 - Regular (4-6)"}],
                "default": "4 - Loyal (7+)"}}}},
        ],
        "spend_tier_by_state": [
            {"$match": {"orders": {"$gt": 0}}},
            {"$group": {"_id": "$geo_data.state", "buyers": {"$sum": 1}, "revenue": {"$sum": "$spent"}}},
            {"$addFields": {"revenue_per_buyer": {"$divide": ["$revenue", "$buyers"]}}},
            {"$sort": {"revenue": -1}}, {"$limit": 10},
        ],
    }},
]

PROMO = (datetime(2025, 3, 2), datetime(2025, 3, 9))   # promo week (see dataset_generator)
A3_PRODUCT_POPULARITY = [
    {"$match": {"status": {"$in": REVENUE_STATUSES}}},
    {"$unwind": "$items"},
    {"$group": {"_id": "$items.product_id",
                "units": {"$sum": "$items.quantity"},
                "revenue": {"$sum": "$items.subtotal"},
                "orders": {"$sum": 1},
                "promo_units": {"$sum": {"$cond": [{"$and": [{"$gte": ["$timestamp", PROMO[0]]},
                                                             {"$lt": ["$timestamp", PROMO[1]]}]},
                                                   "$items.quantity", 0]}}}},
    {"$facet": {
        "top_by_units": [{"$sort": {"units": -1}}, {"$limit": 15}],
        "top_by_revenue": [{"$sort": {"revenue": -1}}, {"$limit": 15}],
        "catalog_coverage": [{"$count": "products_sold"}],
    }},
    {"$project": {"top": {"$setUnion": ["$top_by_units", "$top_by_revenue"]},
                  "top_by_units": "$top_by_units._id", "top_by_revenue": "$top_by_revenue._id",
                  "catalog_coverage": 1}},
    {"$unwind": "$top"},
    {"$lookup": {"from": "products", "localField": "top._id", "foreignField": "_id",
                 "pipeline": [{"$project": {"name": 1, "category.name": 1, "base_price": 1}}], "as": "p"}},
    {"$project": {"_id": 0, "product_id": "$top._id", "name": {"$first": "$p.name"},
                  "category": {"$first": "$p.category.name"}, "units": "$top.units",
                  "revenue": {"$round": ["$top.revenue", 2]}, "orders": "$top.orders",
                  "promo_week_share_pct": {"$round": [{"$multiply": [
                      {"$divide": ["$top.promo_units", "$top.units"]}, 100]}, 1]},
                  # 1-based rank in each top-15 list; null when the product is not in that list
                  "rank_units": {"$let": {"vars": {"i": {"$indexOfArray": ["$top_by_units", "$top._id"]}},
                                          "in": {"$cond": [{"$gte": ["$$i", 0]}, {"$add": ["$$i", 1]}, None]}}},
                  "rank_revenue": {"$let": {"vars": {"i": {"$indexOfArray": ["$top_by_revenue", "$top._id"]}},
                                            "in": {"$cond": [{"$gte": ["$$i", 0]}, {"$add": ["$$i", 1]}, None]}}},
                  "products_sold_total": {"$first": "$catalog_coverage.products_sold"}}},
    {"$sort": {"revenue": -1}},
]


def main():
    db = MongoClient(MONGO_URI)[MONGO_DB]
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pd.set_option("display.width", 200, "display.max_columns", 20)

    with Timer("A1 revenue & gross profit by category x month"):
        a1 = pd.DataFrame(db.transactions.aggregate(A1_REVENUE_BY_CATEGORY_MONTH, allowDiskUse=True))
        a1.to_csv(os.path.join(OUTPUT_DIR, "mongo_A1_revenue_by_category_month.csv"), index=False)
        tot = a1.groupby("category_name")[["revenue", "gross_profit"]].sum().sort_values("revenue", ascending=False)
        print(tot.head(10).round(2))

    with Timer("A2 customer segmentation"):
        res = list(db.users.aggregate(A2_CUSTOMER_SEGMENTS))[0]
        seg = pd.DataFrame(res["by_frequency"]).drop(columns="_id")
        seg["revenue_share_pct"] = (100 * seg.total_revenue / seg.total_revenue.sum()).round(1)
        seg.to_csv(os.path.join(OUTPUT_DIR, "mongo_A2_customer_segments.csv"), index=False)
        st = pd.DataFrame(res["spend_tier_by_state"]).rename(columns={"_id": "state"})
        st.to_csv(os.path.join(OUTPUT_DIR, "mongo_A2_top_states.csv"), index=False)
        print(seg.round(2).to_string(index=False))
        print(st.round(2).to_string(index=False))

    with Timer("A3 product popularity"):
        a3 = pd.DataFrame(db.transactions.aggregate(A3_PRODUCT_POPULARITY, allowDiskUse=True))
        a3[["rank_units", "rank_revenue"]] = a3[["rank_units", "rank_revenue"]].astype("Int64")
        a3.to_csv(os.path.join(OUTPUT_DIR, "mongo_A3_top_products.csv"), index=False)
        print(a3.head(15).to_string(index=False))

    # Show that the period filter uses the timestamp index (IXSCAN, not COLLSCAN)
    plan = db.command("explain", {"aggregate": "transactions",
                                  "pipeline": A1_REVENUE_BY_CATEGORY_MONTH[:1], "cursor": {}},
                      verbosity="queryPlanner")
    print("A1 $match winning plan:", json.dumps(plan.get("queryPlanner", plan.get("stages", [{}])[0]
                                                         .get("$cursor", {}).get("queryPlanner", {}))
                                                .get("winningPlan", {}), default=str)[:400])

    with open(os.path.join(OUTPUT_DIR, "mongo_pipelines.json"), "w") as f:
        json.dump({"A1": A1_REVENUE_BY_CATEGORY_MONTH, "A2": A2_CUSTOMER_SEGMENTS,
                   "A3": A3_PRODUCT_POPULARITY}, f, indent=2, default=str)


if __name__ == "__main__":
    main()
