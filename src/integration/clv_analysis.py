"""
PART 3 - Integrated analytics: Customer Lifetime Value (CLV) estimation & value segmentation.

Business question
  "Which customers are worth the most over the next 3 years, how does on-site engagement relate
   to value, and which segments should marketing target (retain / grow / re-activate)?"

Data sources (one query, three systems)
  MongoDB  users         profile + registration date                      (document store)
  MongoDB  transactions  order history with embedded items                 (document store)
  MongoDB  products      subcategory profit margin (denormalised)          (document store)
  HBase    user_sessions engagement: #sessions, duration, conversion       (wide-column, parallel scans)
  Spark    joins, feature engineering, CLV model, K-Means segmentation, write-back

Processing steps
  1. MongoDB -> Spark. Preferred path: MongoDB Spark Connector (aggregation pipeline pushed down to
     MongoDB, partitions read in parallel). Fallback (no Maven access): the SAME pipeline is executed
     by pymongo and streamed into a DataFrame with an explicit schema.
  2. HBase -> Spark in PARALLEL: the user key-space is cut into N row-key ranges, each Spark task
     opens its own Thrift connection and scans only its range (row_start/row_stop), reading only
     the small 's' column family.
  3. Per-user features: RFM (recency, frequency, monetary), gross margin (item x margin),
     observation window, engagement (sessions, avg minutes, session conversion rate, abandoned carts).
  4. CLV(3y) = annual gross margin x sum_{t=0..2} r^t / (1+d)^t,   d = 10 %
       annual gross margin = observed gross margin x 365 / observed_days
       r (retention) = P(buyer in 1st half of the window buys again in the 2nd half)
  5. K-Means (k=4) on standardised RFM + engagement features -> named value segments.
  6. CLV + segment written BACK into MongoDB users.analytics (serving layer for CRM / apps).
"""
import csv
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import (HBASE_HOST, HBASE_PORT, MONGO_DB, MONGO_URI, OUTPUT_DIR, get_spark,  # noqa: E402
                    save_csv, Timer)

from pyspark.ml.clustering import KMeans  # noqa: E402
from pyspark.ml.feature import StandardScaler, VectorAssembler  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (ArrayType, DoubleType, LongType, StringType, StructField,  # noqa: E402
                               StructType, TimestampType)

AS_OF = "2025-04-16"
WINDOW_START = "2025-01-16"
MID_POINT = "2025-03-02"
DISCOUNT_RATE = 0.10
HORIZON_YEARS = 3
N_SCAN_RANGES = 16
N_USERS_KEYSPACE = 10000
USE_CONNECTOR = os.environ.get("USE_MONGO_CONNECTOR", "1") == "1"

TXN_PIPELINE = [{"$match": {"status": {"$in": ["completed", "shipped", "delivered"]}}},
                {"$project": {"user_id": 1, "timestamp": 1, "total": 1, "subtotal": 1,
                              "items.product_id": 1, "items.subtotal": 1}}]
USERS_SCHEMA = StructType([StructField("user_id", StringType()), StructField("state", StringType()),
                           StructField("registration_date", TimestampType())])
TXN_SCHEMA = StructType([StructField("_id", StringType()), StructField("user_id", StringType()),
                         StructField("timestamp", TimestampType()), StructField("total", DoubleType()),
                         StructField("subtotal", DoubleType()),
                         StructField("items", ArrayType(StructType([StructField("product_id", StringType()),
                                                                     StructField("subtotal", DoubleType())])))])
PROD_SCHEMA = StructType([StructField("product_id", StringType()), StructField("margin", DoubleType())])


# ------------------------------------------------------------------------------ MongoDB
def read_mongo_connector(spark):
    import json

    def rd(coll, pipeline=None):
        r = spark.read.format("mongodb").option("database", MONGO_DB).option("collection", coll)
        return (r.option("aggregation.pipeline", json.dumps(pipeline)) if pipeline else r).load()
    users = rd("users").select(F.col("_id").alias("user_id"), F.col("geo_data.state").alias("state"),
                               "registration_date")
    txns = rd("transactions", TXN_PIPELINE).select("_id", "user_id", "timestamp", "total", "subtotal", "items")
    prods = rd("products").select(F.col("_id").alias("product_id"),
                                  F.col("subcategory.profit_margin").alias("margin"))
    return users, txns, prods


def read_mongo_pymongo(spark):
    from pymongo import MongoClient
    db = MongoClient(MONGO_URI)[MONGO_DB]
    users = [(u["_id"], u["geo_data"].get("state"), u["registration_date"])
             for u in db.users.find({}, {"geo_data.state": 1, "registration_date": 1})]
    txns = [(t["_id"], t["user_id"], t["timestamp"], float(t["total"]), float(t["subtotal"]),
             [(i["product_id"], float(i["subtotal"])) for i in t["items"]])
            for t in db.transactions.aggregate(TXN_PIPELINE)]
    prods = [(p["_id"], float(p["subcategory"]["profit_margin"]))
             for p in db.products.find({}, {"subcategory.profit_margin": 1})]
    return (spark.createDataFrame(users, USERS_SCHEMA), spark.createDataFrame(txns, TXN_SCHEMA),
            spark.createDataFrame(prods, PROD_SCHEMA))


def write_back(spark, seg, source):
    out = seg.select(F.col("user_id").alias("_id"),
                     F.struct(F.col("clv_3y"), F.col("segment"), F.lit(AS_OF).alias("as_of")).alias("analytics"))
    if source.startswith("mongo-spark-connector"):
        (out.write.format("mongodb").mode("append").option("database", MONGO_DB).option("collection", "users")
         .option("operationType", "update").option("idFieldList", "_id").option("upsertDocument", "false").save())
        return
    from pymongo import MongoClient, UpdateOne

    def push(rows):   # each Spark partition writes its own bulk batch -> parallel write-back
        coll = MongoClient(MONGO_URI)[MONGO_DB].users
        ops = [UpdateOne({"_id": r["_id"]}, {"$set": {"analytics": r["analytics"].asDict()}}) for r in rows]
        if ops:
            coll.bulk_write(ops, ordered=False)
    out.foreachPartition(push)


# ------------------------------------------------------------------------------ HBase
HB_SCHEMA = StructType([StructField("user_id", StringType()), StructField("start_time", StringType()),
                        StructField("duration_seconds", LongType()), StructField("num_page_views", LongType()),
                        StructField("conversion_status", StringType()), StructField("cart_value", DoubleType())])


def scan_hbase_range(bounds):
    """Runs inside a Spark task: scan one user-id key range of user_sessions over Thrift."""
    import happybase
    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=300000)
    tbl = conn.table("user_sessions")
    cols = [b"s:start_time", b"s:duration_seconds", b"s:num_page_views", b"s:conversion_status", b"s:cart_value"]
    for start, stop in bounds:
        for key, v in tbl.scan(row_start=start.encode(), row_stop=stop.encode(), columns=cols, batch_size=2000):
            yield (key.decode().split("#")[0], v[b"s:start_time"].decode(), int(v[b"s:duration_seconds"]),
                   int(v[b"s:num_page_views"]), v[b"s:conversion_status"].decode(),
                   float(v.get(b"s:cart_value") or 0))
    conn.close()


def read_hbase_engagement(spark):
    step = N_USERS_KEYSPACE // N_SCAN_RANGES
    ranges = [(f"user_{i:06d}", f"user_{i + step:06d}") for i in range(0, N_USERS_KEYSPACE, step)]
    ranges[-1] = (ranges[-1][0], "user_~")          # open-ended last range ('~' sorts after digits)
    rdd = spark.sparkContext.parallelize(ranges, len(ranges)).mapPartitions(lambda it: scan_hbase_range(list(it)))
    return spark.createDataFrame(rdd, HB_SCHEMA)


# ------------------------------------------------------------------------------ main
def main():
    spark = get_spark("06-integrated-clv", mongo=USE_CONNECTOR)

    with Timer("read MongoDB"):
        source = "pymongo (pipeline executed in MongoDB)"
        if USE_CONNECTOR:
            try:
                users, txns, prods = read_mongo_connector(spark)
                users.limit(1).collect()
                source = "mongo-spark-connector 10.4"
            except Exception as e:  # noqa: BLE001  (connector JAR not downloadable / incompatible)
                print(f"   connector unavailable ({type(e).__name__}: {str(e)[:120]}) -> pymongo fallback")
                users, txns, prods = read_mongo_pymongo(spark)
        else:
            users, txns, prods = read_mongo_pymongo(spark)
        users, txns = users.cache(), txns.cache()
        print(f"   users={users.count():,} revenue txns={txns.count():,} products={prods.count():,} via {source}")

    with Timer(f"read HBase user_sessions ({N_SCAN_RANGES} parallel range scans)"):
        sess = read_hbase_engagement(spark).cache()
        print(f"   sessions={sess.count():,}")

    with Timer("feature engineering"):
        items = (txns.select("_id", "user_id", "total", "subtotal", F.explode("items").alias("it"))
                 .select("_id", "user_id", "total", "subtotal", "it.product_id", F.col("it.subtotal").alias("isub"))
                 .join(F.broadcast(prods), "product_id", "left")
                 .withColumn("gm", F.col("isub") * F.coalesce("margin", F.lit(0.25))
                             * F.when(F.col("subtotal") > 0, F.col("total") / F.col("subtotal")).otherwise(1)))
        gm = items.groupBy("user_id").agg(F.sum("gm").alias("gross_margin"))

        rfm = (txns.groupBy("user_id").agg(
            F.count("*").alias("frequency"), F.sum("total").alias("monetary"),
            F.max("timestamp").alias("last_purchase"),
            F.max(F.when(F.col("timestamp") < F.lit(MID_POINT), 1).otherwise(0)).alias("bought_h1"),
            F.max(F.when(F.col("timestamp") >= F.lit(MID_POINT), 1).otherwise(0)).alias("bought_h2"))
            .withColumn("recency_days", F.datediff(F.lit(AS_OF), "last_purchase")))

        eng = sess.groupBy("user_id").agg(
            F.count("*").alias("sessions"),
            F.round(F.avg("duration_seconds") / 60, 2).alias("avg_session_min"),
            F.avg("num_page_views").alias("avg_page_views"),
            F.avg(F.when(F.col("conversion_status") == "converted", 1).otherwise(0)).alias("session_conv_rate"),
            F.sum(F.when(F.col("conversion_status") == "abandoned", 1).otherwise(0)).alias("abandoned_carts"))

        r = float(rfm.filter("bought_h1 = 1").agg(F.avg("bought_h2")).first()[0])
        multiplier = sum((r ** t) / ((1 + DISCOUNT_RATE) ** t) for t in range(HORIZON_YEARS))
        print(f"   retention r={r:.3f} -> 3-year discounted multiplier={multiplier:.3f}")

        feat = (users.join(rfm, "user_id", "left").join(gm, "user_id", "left").join(eng, "user_id", "left")
                .na.fill(0, ["frequency", "monetary", "gross_margin", "sessions", "avg_session_min",
                             "avg_page_views", "session_conv_rate", "abandoned_carts"])
                .withColumn("recency_days", F.coalesce("recency_days", F.lit(120)))
                .withColumn("observed_days", F.greatest(F.lit(14), F.least(F.lit(90), F.datediff(
                    F.lit(AS_OF), F.greatest(F.col("registration_date"), F.lit(WINDOW_START).cast("timestamp"))))))
                .withColumn("annual_gross_margin", F.col("gross_margin") * 365 / F.col("observed_days"))
                .withColumn("clv_3y", F.round(F.col("annual_gross_margin") * F.lit(multiplier), 2))
                .withColumn("monetary", F.round("monetary", 2))
                .withColumn("gross_margin", F.round("gross_margin", 2)).cache())
        print(f"   feature rows={feat.count():,}")

    with Timer("K-Means segmentation"):
        cols = ["recency_days", "frequency", "monetary", "sessions", "avg_session_min", "session_conv_rate"]
        assembled = VectorAssembler(inputCols=cols, outputCol="raw").transform(feat)
        scaled = StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True) \
            .fit(assembled).transform(assembled)
        model = KMeans(k=4, seed=7, featuresCol="features", predictionCol="cluster").fit(scaled)
        seg = model.transform(scaled).drop("raw", "features").cache()
        prof = [r.asDict() for r in seg.groupBy("cluster").agg(
            F.count("*").alias("users"), F.round(F.avg("clv_3y"), 2).alias("avg_clv_3y"),
            F.round(F.sum("clv_3y"), 2).alias("total_clv_3y"),
            F.round(F.avg("recency_days"), 1).alias("avg_recency_days"),
            F.round(F.avg("frequency"), 2).alias("avg_orders"),
            F.round(F.avg("monetary"), 2).alias("avg_spent"),
            F.round(F.avg("sessions"), 1).alias("avg_sessions"),
            F.round(F.avg("avg_session_min"), 1).alias("avg_session_min"),
            F.round(100 * F.avg("session_conv_rate"), 1).alias("session_conv_pct"),
            F.round(F.avg("abandoned_carts"), 2).alias("avg_abandoned_carts")).collect()]
        # name clusters from centroid profiles: highest CLV, most dormant, then the rest by value
        by_clv = sorted(prof, key=lambda p: -p["avg_clv_3y"])
        names = {by_clv[0]["cluster"]: "Champions"}
        rest = by_clv[1:]
        dormant = max(rest, key=lambda p: p["avg_recency_days"])
        names[dormant["cluster"]] = "At-risk / dormant"
        others = [p for p in rest if p is not dormant]
        for p, nm in zip(others, ["Growing regulars", "Browsers / low value"]):
            names[p["cluster"]] = nm
        total = sum(p["total_clv_3y"] for p in prof)
        for p in prof:
            p["segment"] = names[p["cluster"]]
            p["clv_share_pct"] = round(100 * p["total_clv_3y"] / total, 1)
        name_map = F.create_map(*[x for k, v in names.items() for x in (F.lit(int(k)), F.lit(v))])
        seg = seg.withColumn("segment", name_map[F.col("cluster")])

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    keys = ["segment"] + [k for k in prof[0] if k != "segment"]
    with open(os.path.join(OUTPUT_DIR, "integration_clv_segments.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(sorted(prof, key=lambda p: -p["avg_clv_3y"]))
    for p in sorted(prof, key=lambda p: -p["avg_clv_3y"]):
        print("  ", {k: p[k] for k in keys})

    print("\nTop customers by 3-year CLV:")
    save_csv(seg.select("user_id", "state", "segment", "clv_3y", "frequency", "monetary", "gross_margin",
                        "recency_days", "sessions", "avg_session_min",
                        F.round("session_conv_rate", 3).alias("session_conv_rate"), "abandoned_carts")
             .orderBy(F.desc("clv_3y")), "integration_clv_users", show=10)

    corr = {c: seg.stat.corr(c, "clv_3y") for c in ["sessions", "avg_session_min", "session_conv_rate",
                                                     "abandoned_carts", "frequency"]}
    print("\ncorrelation with CLV:", {k: round(v, 3) for k, v in corr.items()})
    with open(os.path.join(OUTPUT_DIR, "integration_clv_summary.txt"), "w") as f:
        f.write(f"run_at={datetime.now().isoformat(timespec='seconds')}\nsource={source}\n")
        f.write(f"retention_r={r:.4f}\nmultiplier={multiplier:.4f}\ntotal_clv_3y={total:.2f}\n")
        for k, v in corr.items():
            f.write(f"corr_clv_{k}={v:.4f}\n")

    with Timer("write CLV back to MongoDB users.analytics"):
        write_back(spark, seg, source)
    spark.stop()


if __name__ == "__main__":
    main()
