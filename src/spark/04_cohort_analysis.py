"""
PART 2.1 (option 2) - Cohort analysis of user purchasing patterns.

Cohort       = month in which the user registered
Activity     = revenue transactions (completed/shipped/delivered)
Period index = months between registration month and transaction month (0 = same month)

Outputs
  output/spark_cohort_long.csv                 (cohort, txn_month, period, buyers, retention %, revenue, ...)
  output/spark_cohort_retention.csv            pivot: cohort x purchase month -> % of cohort buying
  output/spark_cohort_revenue_per_member.csv   pivot: cohort x purchase month -> revenue per cohort member
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import CLEAN_DIR, get_spark, save_csv, Timer  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def main():
    spark = get_spark("04-cohort-analysis")
    users = spark.read.parquet(os.path.join(CLEAN_DIR, "users"))
    txns = spark.read.parquet(os.path.join(CLEAN_DIR, "transactions")).filter("is_revenue")

    with Timer("cohorts"):
        u = users.select("user_id", F.date_format(F.trunc("registration_date", "month"), "yyyy-MM").alias("cohort"))
        cohort_size = u.groupBy("cohort").agg(F.count("*").alias("cohort_users"))
        t = (txns.select("user_id", "total", F.date_format(F.trunc("timestamp", "month"), "yyyy-MM").alias("month"))
             .join(u, "user_id")
             .withColumn("period", F.months_between(F.to_date("month", "yyyy-MM"),
                                                    F.to_date("cohort", "yyyy-MM")).cast("int")))
        cohort = (t.groupBy("cohort", "month", "period")
                  .agg(F.countDistinct("user_id").alias("active_buyers"),
                       F.round(F.sum("total"), 2).alias("revenue"))
                  .join(cohort_size, "cohort")
                  .withColumn("retention_pct", F.round(100 * F.col("active_buyers") / F.col("cohort_users"), 2))
                  .withColumn("revenue_per_member", F.round(F.col("revenue") / F.col("cohort_users"), 2))
                  .orderBy("cohort", "month")).cache()
        save_csv(cohort, "spark_cohort_long", show=0)

        # pivot in Spark: one row per cohort, one column per purchase month (calendar view)
        for metric, name in [("retention_pct", "spark_cohort_retention"),
                             ("revenue_per_member", "spark_cohort_revenue_per_member")]:
            piv = cohort.groupBy("cohort").pivot("month").agg(F.first(metric)).na.fill(0).orderBy("cohort")
            print(f"\n{metric}:")
            save_csv(piv.join(cohort_size, "cohort").orderBy("cohort"), name, show=20)
    spark.stop()


if __name__ == "__main__":
    main()
