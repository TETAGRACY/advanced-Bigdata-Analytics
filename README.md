# Distributed Multi-Model Analytics for E-commerce Data
AUCA Big Data Analytics – Final Project (Oct 2026)

MongoDB (document) + HBase (wide-column) + Apache Spark (distributed processing) working together
on a synthetic 90-day e-commerce dataset (users, categories, products, sessions, transactions).

```
                 ┌──────────────────────── Docker network ─────────────────────────┐
 dataset_        │                                                                 │
 generator.py ──►│  data/raw/*.json ──► Spark 01_clean ──► data/clean (Parquet lake)│
                 │                           │  └──► data/clean_jsonl               │
                 │          ┌────────────────┴───────────────┐                      │
                 │          ▼                                ▼                      │
                 │  MongoDB  (host :27017)            HBase (ecom-hbase)            │
                 │  products · users(+summary)        user_sessions (user#revTs)    │
                 │  transactions(+items)              product_metrics (prod#date)   │
                 │  categories                                                      │
                 │          │  aggregation pipelines        │  prefix / range scans │
                 │          └────────────┬───────────────────┘                      │
                 │                       ▼                                          │
                 │   Spark (ecom-spark): recommendations · cohorts · Spark SQL      │
                 │   Integrated CLV: Mongo connector + parallel HBase scans + MLlib │
                 │                       │ write-back CLV ──► MongoDB users          │
                 └───────────────────────┼──────────────────────────────────────────┘
                                         ▼
                           output/*.csv  ·  figures/*.png  ·  report/
```

## Repository layout
| Path | Purpose |
|---|---|
| `src/dataset_generator.py` | Synthetic dataset (schema of the project brief, chunked sessions, ~1% dirty records) |
| `src/common.py` | Config (env vars), SparkSession factory, reverse-timestamp helper |
| `src/spark/01_clean_normalize.py` | **Part 2** cleaning & normalisation → Parquet + JSONL |
| `src/spark/02_product_daily_metrics.py` | Daily product views / cart adds / purchases (feeds HBase) |
| `src/mongodb/load_mongo.py` | **Part 1** MongoDB document model + indexes |
| `src/mongodb/aggregations.py` | **Part 1** three aggregation pipelines (revenue, segmentation, popularity) |
| `src/hbase/schema.hbase` | **Part 1** HBase table / column-family DDL (pre-split, bloom, compression) |
| `src/hbase/load_hbase.py`, `query_hbase.py`, `sample_queries.hbase` | HBase load + user-session queries |
| `src/spark/03_recommendations.py` | **Part 2** bought-together & viewed-together rules (support / confidence / lift) |
| `src/spark/04_cohort_analysis.py` | **Part 2** registration-cohort purchasing analysis |
| `src/spark/05_spark_sql_analytics.py` | **Part 2** Spark SQL (funnel, ranking windows, moving averages, multi-store join) |
| `src/integration/clv_analysis.py` | **Part 3** Customer Lifetime Value across MongoDB + HBase + Spark |
| `src/viz/make_charts.py` | **Part 4** figures |
| `output/` | Query results (CSV/TXT) produced by every step |
| `figures/` | PNG charts used in the report |
| `report/` | Technical report (HTML source + PDF) |

## Prerequisites
* Docker Desktop (≥ 8 GB RAM assigned). Spark, all Python libraries and HBase run in containers.
* **MongoDB**: by default the stack uses a MongoDB server running on the host (`localhost:27017`,
  tested with MongoDB 9.0.4), reached from the Spark container as `host.docker.internal`.
  No local MongoDB? Run it in Docker instead:
  ```powershell
  $env:MONGO_URI="mongodb://mongo:27017"; docker compose --profile docker-mongo up -d
  ```
* The image builds **offline**: Python wheels (`wheels/`) and the MongoDB Spark Connector JARs (`jars/`)
  are committed, so no PyPI/Maven access is needed during `docker compose build` (useful behind
  TLS-intercepting corporate proxies). Only the base images `spark:3.5.3-python3` and
  `harisekhon/hbase` are pulled from Docker Hub.

## Reproduce everything
```powershell
cd Final_Project
.\run_pipeline.ps1          # Windows      (or ./run_pipeline.sh on Linux/macOS)
```
> If `docker compose ...` fails with `unknown shorthand flag: 'd'`, your shell cannot see the Compose
> plugin (typical in an elevated/other-user PowerShell) – use the standalone `docker-compose up -d` instead.

Total run time ≈ 15–25 min on a laptop. Steps (each can be run on its own):
```powershell
docker compose up -d --build
docker exec ecom-spark python3 src/dataset_generator.py --out data/raw      # --sessions 2000000 for full scale
docker exec ecom-spark spark-submit src/spark/01_clean_normalize.py
docker exec ecom-spark spark-submit src/spark/02_product_daily_metrics.py
docker exec ecom-spark python3 src/mongodb/load_mongo.py
docker exec ecom-spark python3 src/mongodb/aggregations.py
Get-Content src/hbase/schema.hbase | docker exec -i ecom-hbase hbase shell -n
docker exec ecom-spark python3 src/hbase/load_hbase.py
docker exec ecom-spark python3 src/hbase/query_hbase.py --user user_000089
docker exec ecom-spark spark-submit src/spark/03_recommendations.py
docker exec ecom-spark spark-submit src/spark/04_cohort_analysis.py
docker exec ecom-spark spark-submit src/spark/05_spark_sql_analytics.py
docker exec ecom-spark spark-submit src/integration/clv_analysis.py
docker exec ecom-spark python3 src/viz/make_charts.py
```

## Sample queries
**MongoDB** (`mongosh ecommerce`, or MongoDB Compass on `mongodb://localhost:27017`)
```js
db.users.findOne({_id: "user_000089"})                       // profile + purchase_summary + analytics.clv_3y
db.transactions.find({user_id: "user_000089"}).sort({timestamp: -1}).limit(5)   // uses {user_id, timestamp} index
db.users.find({"analytics.segment": "At-risk / dormant"}).sort({"analytics.clv_3y": -1}).limit(10)
```
Full pipelines: `output/mongo_pipelines.json`.

**HBase** (`docker exec -it ecom-hbase hbase shell`) – see `src/hbase/sample_queries.hbase`
```ruby
scan 'user_sessions', {ROWPREFIXFILTER => 'user_000089#', COLUMNS => ['s'], LIMIT => 5}   # newest sessions first
scan 'product_metrics', {STARTROW => 'prod_00010#20250301', STOPROW => 'prod_00010#20250401'}
```

**Spark SQL** – all queries are in `src/spark/05_spark_sql_analytics.py` and exported to `output/spark_sql_queries.sql`.

## Web UIs
* HBase Master: http://localhost:16011  (Thrift for host-side clients: `localhost:9091`)
* Spark UI (while a job runs): http://localhost:4040

## Configuration
Environment variables (set in `docker-compose.yml`): `MONGO_URI`, `MONGO_DB`, `HBASE_HOST`, `HBASE_PORT`,
`DATA_DIR`, `SPARK_MASTER` (default `local[*]`; point to `spark://host:7077` for a cluster), `SPARK_DRIVER_MEMORY`.
