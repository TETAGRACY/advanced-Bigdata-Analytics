"""
PART 2.1 - Spark batch job: clean & normalise the raw JSON dataset.

raw JSON (data/raw)  ──►  Spark  ──►  data/clean/<table>/        (Parquet, for Spark analytics)
                                  └─► data/clean_jsonl/<table>/  (JSON-lines, for DB loaders)

Cleaning rules (each counted in output/data_quality.csv):
  * Explicit schemas (no inference pass over ~GBs; `cart_contents` is a MAP, not a 5000-field struct)
  * Type coercion: numeric strings -> numbers, ISO strings -> timestamps
  * Standardise categorical text: lower-case device type / payment method, trim strings
  * Missing values: state -> 'UNKNOWN', referrer -> 'unknown', session_id null kept but flagged
  * De-duplicate on business keys (transaction_id, session_id, user_id, product_id)
  * Integrity: drop transactions whose user does not exist; recompute totals if inconsistent
  * Denormalise: categories hierarchy flattened into a subcategory dimension; products enriched
  * Derived fact tables: page_views (exploded), transaction_items (exploded)
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import RAW_DIR, CLEAN_DIR, DATA_DIR, get_spark, save_csv, Timer  # noqa: E402

from pyspark.sql import functions as F, Window  # noqa: E402
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, LongType, MapType,  # noqa: E402
                               StringType, StructField, StructType)

S, D, L, B = StringType(), DoubleType(), LongType(), BooleanType()
GEO = StructType([StructField("city", S), StructField("state", S), StructField("country", S),
                  StructField("ip_address", S)])

USERS_SCHEMA = StructType([StructField("user_id", S), StructField("geo_data", GEO),
                           StructField("registration_date", S), StructField("last_active", S)])
CATEGORIES_SCHEMA = StructType([
    StructField("category_id", S), StructField("name", S),
    StructField("subcategories", ArrayType(StructType([
        StructField("subcategory_id", S), StructField("name", S), StructField("profit_margin", D)])))])
PRODUCTS_SCHEMA = StructType([
    StructField("product_id", S), StructField("name", S), StructField("category_id", S),
    StructField("subcategory_id", S), StructField("base_price", D), StructField("current_stock", L),
    StructField("is_active", B),
    StructField("price_history", ArrayType(StructType([StructField("price", D), StructField("date", S)]))),
    StructField("creation_date", S)])
SESSIONS_SCHEMA = StructType([
    StructField("session_id", S), StructField("user_id", S), StructField("start_time", S),
    StructField("end_time", S),
    StructField("duration_seconds", S),                 # read as string: some records are dirty
    StructField("geo_data", GEO),
    StructField("device_profile", StructType([StructField("type", S), StructField("os", S),
                                              StructField("browser", S)])),
    StructField("viewed_products", ArrayType(S)),
    StructField("page_views", ArrayType(StructType([
        StructField("timestamp", S), StructField("page_type", S), StructField("product_id", S),
        StructField("category_id", S), StructField("view_duration", L)]))),
    StructField("cart_contents", MapType(S, StructType([StructField("quantity", L),
                                                        StructField("price", D)]))),
    StructField("conversion_status", S), StructField("referrer", S)])
TXN_SCHEMA = StructType([
    StructField("transaction_id", S), StructField("session_id", S), StructField("user_id", S),
    StructField("timestamp", S),
    StructField("items", ArrayType(StructType([
        StructField("product_id", S), StructField("quantity", L), StructField("unit_price", D),
        StructField("subtotal", D)]))),
    StructField("subtotal", D), StructField("discount", D), StructField("total", D),
    StructField("payment_method", S), StructField("status", S)])


def read(spark, pattern, schema):
    # generator writes JSON arrays -> multiLine mode
    return spark.read.schema(schema).option("multiLine", True).json(os.path.join(RAW_DIR, pattern))


def main():
    spark = get_spark("01-clean-normalize")
    dq = []   # (table, metric, value)

    with Timer("users"):
        raw = read(spark, "users.json", USERS_SCHEMA)
        dq.append(("users", "raw_rows", raw.count()))
        users = (raw.dropna(subset=["user_id"]).dropDuplicates(["user_id"])
                 .select("user_id",
                         F.trim("geo_data.city").alias("city"),
                         F.coalesce(F.upper(F.trim("geo_data.state")), F.lit("UNKNOWN")).alias("state"),
                         F.coalesce(F.upper("geo_data.country"), F.lit("US")).alias("country"),
                         F.to_timestamp("registration_date").alias("registration_date"),
                         F.to_timestamp("last_active").alias("last_active")))
        dq.append(("users", "missing_state_filled", raw.filter(F.col("geo_data.state").isNull()).count()))

    with Timer("categories -> subcategory dimension"):
        cats = read(spark, "categories.json", CATEGORIES_SCHEMA)
        subcats = (cats.select("category_id", F.col("name").alias("category_name"),
                               F.explode("subcategories").alias("s"))
                   .select("category_id", "category_name", "s.subcategory_id",
                           F.col("s.name").alias("subcategory_name"), "s.profit_margin"))

    with Timer("products"):
        raw = read(spark, "products.json", PRODUCTS_SCHEMA)
        dq.append(("products", "raw_rows", raw.count()))
        products = (raw.dropDuplicates(["product_id"])
                    .withColumn("creation_date", F.to_timestamp("creation_date"))
                    .withColumn("current_stock", F.greatest(F.coalesce("current_stock", F.lit(0)), F.lit(0)))
                    .withColumn("is_active", F.coalesce("is_active", F.lit(False)))
                    .join(F.broadcast(subcats.drop("category_id")), "subcategory_id", "left"))

    with Timer("sessions"):
        raw = read(spark, "sessions_*.json", SESSIONS_SCHEMA)
        n_raw = raw.count()
        dq.append(("sessions", "raw_rows", n_raw))
        dq.append(("sessions", "dirty_duration_string",
                   raw.filter(~F.col("duration_seconds").rlike(r"^\d+$")).count()))
        dq.append(("sessions", "missing_referrer", raw.filter(F.col("referrer").isNull()).count()))
        dq.append(("sessions", "uppercase_device_type",
                   raw.filter(F.col("device_profile.type") != F.lower("device_profile.type")).count()))
        sessions = (raw.dropna(subset=["session_id", "user_id"]).dropDuplicates(["session_id"])
                    .withColumn("start_time", F.to_timestamp("start_time"))
                    .withColumn("end_time", F.to_timestamp("end_time"))
                    .withColumn("duration_seconds",
                                F.coalesce(F.regexp_replace("duration_seconds", r"[^0-9]", "").cast("long"),
                                           F.col("end_time").cast("long") - F.col("start_time").cast("long")))
                    .withColumn("device_type", F.lower(F.trim("device_profile.type")))
                    .withColumn("device_os", F.col("device_profile.os"))
                    .withColumn("browser", F.col("device_profile.browser"))
                    .withColumn("referrer", F.coalesce(F.lower("referrer"), F.lit("unknown")))
                    .withColumn("conversion_status", F.lower("conversion_status"))
                    .withColumn("num_page_views", F.size("page_views"))
                    .withColumn("num_products_viewed", F.size("viewed_products"))
                    .withColumn("cart_items", F.size(F.map_keys("cart_contents")))
                    .withColumn("session_date", F.to_date("start_time"))
                    .drop("device_profile"))

        page_views = (sessions.select("session_id", "user_id",
                                      F.posexplode("page_views").alias("seq", "pv"))
                      .select("session_id", "user_id", "seq",
                              F.to_timestamp("pv.timestamp").alias("ts"),
                              F.lower("pv.page_type").alias("page_type"),
                              F.col("pv.product_id").alias("product_id"),
                              F.col("pv.category_id").alias("category_id"),
                              F.col("pv.view_duration").alias("view_duration")))

    with Timer("transactions"):
        raw = read(spark, "transactions.json", TXN_SCHEMA)
        n_raw = raw.count()
        dq.append(("transactions", "raw_rows", n_raw))
        dq.append(("transactions", "uppercase_payment_method",
                   raw.filter(F.col("payment_method") != F.lower("payment_method")).count()))
        dedup = raw.dropDuplicates(["transaction_id"])
        dq.append(("transactions", "duplicates_removed", n_raw - dedup.count()))
        # referential integrity: user must exist
        txns = dedup.join(users.select("user_id"), "user_id", "left_semi")
        dq.append(("transactions", "orphan_user_removed", dedup.count() - txns.count()))
        recomputed = F.round(F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["subtotal"]), 2)
        txns = (txns
                .withColumn("timestamp", F.to_timestamp("timestamp"))
                .withColumn("payment_method", F.lower(F.trim("payment_method")))
                .withColumn("status", F.lower("status"))
                .withColumn("has_session", F.col("session_id").isNotNull())
                .withColumn("subtotal", F.when(F.abs(F.col("subtotal") - recomputed) > 0.01, recomputed)
                            .otherwise(F.col("subtotal")))
                .withColumn("discount", F.coalesce("discount", F.lit(0.0)))
                .withColumn("total", F.round(F.col("subtotal") - F.col("discount"), 2))
                .withColumn("is_revenue", ~F.col("status").isin("refunded", "cancelled"))
                .withColumn("txn_date", F.to_date("timestamp"))
                .withColumn("num_items", F.size("items")))
        items = (txns.select("transaction_id", "user_id", "timestamp", "txn_date", "status", "is_revenue",
                             F.explode("items").alias("it"))
                 .select("*", "it.product_id", "it.quantity", "it.unit_price", "it.subtotal").drop("it")
                 .join(F.broadcast(products.select("product_id", "category_id", "subcategory_id",
                                                   "category_name", "profit_margin")), "product_id", "left"))

    tables = {"users": users, "subcategories": subcats, "products": products, "sessions": sessions,
              "page_views": page_views, "transactions": txns, "transaction_items": items}
    for name, df in tables.items():
        with Timer(f"write {name}"):
            df.write.mode("overwrite").parquet(os.path.join(CLEAN_DIR, name))
            dq.append((name, "clean_rows", spark.read.parquet(os.path.join(CLEAN_DIR, name)).count()))

    # JSON-lines copies for the MongoDB / HBase loaders (timestamps as ISO strings)
    for name in ["users", "subcategories", "products", "sessions", "transactions"]:
        with Timer(f"write jsonl {name}"):
            (spark.read.parquet(os.path.join(CLEAN_DIR, name)).coalesce(4).write.mode("overwrite")
             .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
             .json(os.path.join(DATA_DIR, "clean_jsonl", name)))

    save_csv(spark.createDataFrame(dq, "table string, metric string, value long"), "data_quality", show=40)
    spark.stop()


if __name__ == "__main__":
    main()
