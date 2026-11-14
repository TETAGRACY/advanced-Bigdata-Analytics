"""
PART 2.1 - Product recommendation indicators (Spark DataFrames).

(a) "Customers who bought X also bought Y"  - co-purchase pairs from transaction baskets
(b) "Products frequently viewed together"    - co-view pairs from session viewed_products

For each ordered pair (X -> Y) we compute the classic association-rule metrics:
    support(X,Y)    = baskets containing X and Y / all baskets
    confidence(X→Y) = baskets with X and Y / baskets with X
    lift(X→Y)       = confidence(X→Y) / P(Y)          (> 1 means a genuine affinity)

Optimisations
  * baskets are de-duplicated product SETS (array_distinct) -> one pair per basket
  * pairs generated with a self-join on basket_id restricted to a<b (halves the shuffle),
    then mirrored to give both rule directions
  * single-item baskets filtered out before the join (they can never produce a pair)
  * minimum pair support threshold prunes the long tail before computing metrics
  * product-frequency table is small -> broadcast join
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import CLEAN_DIR, get_spark, save_csv, Timer  # noqa: E402
from pyspark.sql import DataFrame, functions as F  # noqa: E402

MIN_PAIR_COUNT_BUY = 3
MIN_PAIR_COUNT_VIEW = 5
MIN_ANTECEDENT_COUNT = 20


def association_rules(baskets: DataFrame, min_pair_count: int) -> DataFrame:
    """baskets: (basket_id, items: array<string>) -> rules X->Y with support/confidence/lift."""
    baskets = baskets.withColumn("items", F.array_distinct("items")).cache()
    n_baskets = baskets.count()

    item_freq = (baskets.select(F.explode("items").alias("item"))
                 .groupBy("item").agg(F.count("*").alias("item_count")))

    exploded = (baskets.filter(F.size("items") > 1)
                .select("basket_id", F.explode("items").alias("item")))
    pairs = (exploded.alias("a").join(exploded.alias("b"), "basket_id")
             .filter(F.col("a.item") < F.col("b.item"))
             .groupBy(F.col("a.item").alias("x"), F.col("b.item").alias("y"))
             .agg(F.count("*").alias("pair_count"))
             .filter(F.col("pair_count") >= min_pair_count))
    both = pairs.unionByName(pairs.select(F.col("y").alias("x"), F.col("x").alias("y"), "pair_count"))

    fx = F.broadcast(item_freq.withColumnRenamed("item", "x").withColumnRenamed("item_count", "x_count"))
    fy = F.broadcast(item_freq.withColumnRenamed("item", "y").withColumnRenamed("item_count", "y_count"))
    return (both.join(fx, "x").join(fy, "y")
            .withColumn("support", F.col("pair_count") / F.lit(n_baskets))
            .withColumn("confidence", F.col("pair_count") / F.col("x_count"))
            .withColumn("lift", F.col("confidence") / (F.col("y_count") / F.lit(n_baskets)))
            .select("x", "y", "pair_count", "x_count", "y_count",
                    F.round("support", 6).alias("support"), F.round("confidence", 4).alias("confidence"),
                    F.round("lift", 2).alias("lift")))


def main():
    spark = get_spark("03-recommendations")
    txns = spark.read.parquet(os.path.join(CLEAN_DIR, "transactions"))
    sessions = spark.read.parquet(os.path.join(CLEAN_DIR, "sessions"))
    products = spark.read.parquet(os.path.join(CLEAN_DIR, "products")) \
        .select("product_id", "name", "category_name", "subcategory_id")

    with Timer("(a) bought-together rules"):
        # basket = one ORDER (classic market-basket analysis). User-level baskets were tested too,
        # but mixing 90 days of unrelated purchases produced mostly noise (5% same-subcategory rules).
        order_baskets = (txns.filter("is_revenue")
                         .select(F.col("transaction_id").alias("basket_id"), F.col("items.product_id").alias("items")))
        bought = association_rules(order_baskets, MIN_PAIR_COUNT_BUY).cache()

    with Timer("(b) viewed-together rules"):
        view_baskets = (sessions.filter(F.size("viewed_products") > 1)
                        .select(F.col("session_id").alias("basket_id"), F.col("viewed_products").alias("items")))
        viewed = association_rules(view_baskets, MIN_PAIR_COUNT_VIEW).cache()

    def label(df):
        px = products.select(F.col("product_id").alias("x"), F.col("name").alias("x_name"),
                             F.col("subcategory_id").alias("x_sub"))
        py = products.select(F.col("product_id").alias("y"), F.col("name").alias("y_name"),
                             F.col("subcategory_id").alias("y_sub"))
        return df.join(px, "x").join(py, "y").withColumn("same_subcategory", F.col("x_sub") == F.col("y_sub"))

    summary = []
    for name, df in [("bought_together", bought), ("viewed_together", viewed)]:
        # rank only rules whose antecedent has enough evidence (avoids 3/3 = 100% confidence noise)
        top = (label(df).filter(F.col("x_count") >= MIN_ANTECEDENT_COUNT)
               .orderBy(F.desc("confidence"), F.desc("lift"), F.desc("pair_count")))
        top.write.mode("overwrite").parquet(os.path.join(CLEAN_DIR, f"rules_{name}"))
        stats = label(df).agg(F.count("*").alias("rules"), F.avg("lift").alias("avg_lift"),
                              F.avg("confidence").alias("avg_confidence"),
                              F.avg(F.col("same_subcategory").cast("int")).alias("share_same_subcat")).first()
        summary.append((name, stats["rules"], float(stats["avg_lift"] or 0), float(stats["avg_confidence"] or 0),
                        float(stats["share_same_subcat"] or 0)))
        print(f"\n{name}: {stats['rules']:,} rules")
        save_csv(top.select("x", "x_name", "y", "y_name", "pair_count", "x_count", "support", "confidence",
                            "lift", "same_subcategory"), f"spark_rules_{name}", limit=200)
    save_csv(spark.createDataFrame(summary, "rule_set string, rules long, avg_lift double, "
                                            "avg_confidence double, share_same_subcategory double"),
             "spark_rules_summary")

    # Top-3 recommendations per product (window function) = what a "you may also like" widget serves
    from pyspark.sql import Window
    w = Window.partitionBy("x").orderBy(F.desc("confidence"), F.desc("lift"))
    recs = (bought.filter(f"lift > 1 AND x_count >= {MIN_ANTECEDENT_COUNT}").withColumn("rank", F.row_number().over(w)).filter("rank <= 3")
            .groupBy("x").agg(F.collect_list(F.struct("rank", "y", "confidence", "lift")).alias("recommendations")))
    recs.write.mode("overwrite").parquet(os.path.join(CLEAN_DIR, "product_recommendations"))
    print(f"\nproducts with >=1 recommendation: {recs.count():,}")
    spark.stop()


if __name__ == "__main__":
    main()
