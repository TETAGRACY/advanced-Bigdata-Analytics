#!/usr/bin/env bash
# End-to-end pipeline (Linux/macOS/Git-Bash). Run from the project root: ./run_pipeline.sh
set -euo pipefail
step() { echo; echo "==== $1 ===="; shift; "$@"; }
X="docker exec ecom-spark"

step "0. Start containers"             docker compose up -d --build
step "1. Generate dataset"             $X python3 src/dataset_generator.py --out data/raw
step "2. Spark: clean & normalise"     $X spark-submit src/spark/01_clean_normalize.py
step "3. Spark: product daily metrics" $X spark-submit src/spark/02_product_daily_metrics.py
step "4. MongoDB: load"                $X python3 src/mongodb/load_mongo.py
step "5. MongoDB: aggregations"        $X python3 src/mongodb/aggregations.py
step "6. HBase: create schema"         sh -c "docker exec -i ecom-hbase hbase shell -n < src/hbase/schema.hbase"
step "7. HBase: load"                  $X python3 src/hbase/load_hbase.py
step "8. HBase: queries"               $X python3 src/hbase/query_hbase.py --user user_000089
step "9. Spark: recommendations"       $X spark-submit src/spark/03_recommendations.py
step "10. Spark: cohort analysis"      $X spark-submit src/spark/04_cohort_analysis.py
step "11. Spark SQL analytics"         $X spark-submit src/spark/05_spark_sql_analytics.py
step "12. Integrated CLV"              $X spark-submit src/integration/clv_analysis.py
step "13. Visualisations"              $X python3 src/viz/make_charts.py
echo "All steps finished. Results in ./output, charts in ./figures"
