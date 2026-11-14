"""
Spark batch job: build the daily product-performance fact (feeds the HBase `product_metrics` table).

   page_views (product_detail)  -> views, unique_sessions   per product per day
   sessions.cart_contents       -> cart_adds                per product per day
   transaction_items            -> purchases, units, revenue per product per day

The three aggregates are FULL OUTER joined on (product_id, date): a product can be purchased on
a day it was not viewed (session-less orders) and vice-versa -> sparse rows, a natural fit for HBase.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import CLEAN_DIR, DATA_DIR, get_spark, Timer  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def main():
    spark = get_spark("02-product-daily-metrics")
    pv = spark.read.parquet(os.path.join(CLEAN_DIR, "page_views"))
    sess = spark.read.parquet(os.path.join(CLEAN_DIR, "sessions"))
    items = spark.read.parquet(os.path.join(CLEAN_DIR, "transaction_items"))

    with Timer("aggregate"):
        views = (pv.filter("page_type = 'product_detail' AND product_id IS NOT NULL")
                 .groupBy("product_id", F.to_date("ts").alias("date"))
                 .agg(F.count("*").alias("views"), F.countDistinct("session_id").alias("unique_sessions")))
        carts = (sess.select("session_date", F.explode("cart_contents").alias("product_id", "c"))
                 .groupBy("product_id", F.col("session_date").alias("date"))
                 .agg(F.count("*").alias("cart_adds")))
        buys = (items.filter("is_revenue")
                .groupBy("product_id", F.col("txn_date").alias("date"))
                .agg(F.countDistinct("transaction_id").alias("purchases"),
                     F.sum("quantity").alias("units"), F.round(F.sum("subtotal"), 2).alias("revenue")))
        daily = (views.join(carts, ["product_id", "date"], "full")
                 .join(buys, ["product_id", "date"], "full")
                 .na.fill(0, ["views", "unique_sessions", "cart_adds", "purchases", "units"])
                 .na.fill(0.0, ["revenue"])
                 .withColumn("date", F.date_format("date", "yyyy-MM-dd"))
                 .repartition(4, "product_id").sortWithinPartitions("product_id", "date"))
        daily.cache()
        print(f"   {daily.count():,} product-day rows")
        daily.write.mode("overwrite").parquet(os.path.join(CLEAN_DIR, "product_daily_metrics"))
        daily.write.mode("overwrite").json(os.path.join(DATA_DIR, "clean_jsonl", "product_daily_metrics"))
    spark.stop()


if __name__ == "__main__":
    main()
