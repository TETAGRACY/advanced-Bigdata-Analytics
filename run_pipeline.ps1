# End-to-end pipeline (Windows PowerShell). Run from the project root:  .\run_pipeline.ps1
# Each step can also be run on its own - see README.md.
$ErrorActionPreference = "Stop"
function Step($name, [scriptblock]$cmd) {
    Write-Host "`n==== $name ====" -ForegroundColor Cyan
    & $cmd
    if ($LASTEXITCODE -ne 0) { throw "Step failed: $name" }
}

Step "0. Start containers"            { docker compose up -d --build }
Step "1. Generate dataset"            { docker exec ecom-spark python3 src/dataset_generator.py --out data/raw }
Step "2. Spark: clean & normalise"    { docker exec ecom-spark spark-submit src/spark/01_clean_normalize.py }
Step "3. Spark: product daily metrics"{ docker exec ecom-spark spark-submit src/spark/02_product_daily_metrics.py }
Step "4. MongoDB: load"               { docker exec ecom-spark python3 src/mongodb/load_mongo.py }
Step "5. MongoDB: aggregations"       { docker exec ecom-spark python3 src/mongodb/aggregations.py }
Step "6. HBase: create schema"        { Get-Content src/hbase/schema.hbase | docker exec -i ecom-hbase hbase shell -n }
Step "7. HBase: load"                 { docker exec ecom-spark python3 src/hbase/load_hbase.py }
Step "8. HBase: queries"              { docker exec ecom-spark python3 src/hbase/query_hbase.py --user user_000089 }
Step "9. Spark: recommendations"      { docker exec ecom-spark spark-submit src/spark/03_recommendations.py }
Step "10. Spark: cohort analysis"     { docker exec ecom-spark spark-submit src/spark/04_cohort_analysis.py }
Step "11. Spark SQL analytics"        { docker exec ecom-spark spark-submit src/spark/05_spark_sql_analytics.py }
Step "12. Integrated CLV (Mongo+HBase+Spark)" { docker exec ecom-spark spark-submit src/integration/clv_analysis.py }
Step "13. Visualisations"             { docker exec ecom-spark python3 src/viz/make_charts.py }
Write-Host "`nAll steps finished. Results: .\output   Charts: .\figures" -ForegroundColor Green
