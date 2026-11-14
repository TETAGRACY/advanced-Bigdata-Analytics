"""Shared configuration and helpers used by every stage of the pipeline."""
import os
import time

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
RAW_DIR = os.path.join(DATA_DIR, "raw")
CLEAN_DIR = os.path.join(DATA_DIR, "clean")
PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")
FIG_DIR = os.path.join(PROJECT_DIR, "figures")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "ecommerce")
HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))

# Long.MAX_VALUE-style constant for reverse timestamps (seconds resolution, 10 digits)
REVERSE_TS_BASE = 9_999_999_999


def reverse_ts(epoch_seconds: int) -> str:
    """Reverse timestamp so the NEWEST session sorts FIRST inside a user's key range."""
    return f"{REVERSE_TS_BASE - int(epoch_seconds):010d}"


def get_spark(app_name: str, mongo: bool = False):
    """Create a SparkSession tuned for a single-node container run."""
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.appName(app_name)
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "5g"))
         .config("spark.sql.shuffle.partitions", "16")      # small cluster -> fewer shuffle tasks
         .config("spark.sql.adaptive.enabled", "true")       # AQE: coalesce partitions, skew joins
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.jars.ivy", "/opt/spark/ivy"))
    if mongo:
        # In the container the connector JARs are baked into /opt/spark/jars (see Dockerfile).
        # Elsewhere, let Spark resolve the connector from Maven Central.
        import glob
        if not glob.glob(os.path.join(os.environ.get("SPARK_HOME", "/opt/spark"), "jars", "mongo-spark-connector*.jar")):
            b = b.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")
        b = (b.config("spark.mongodb.read.connection.uri", MONGO_URI)
             .config("spark.mongodb.write.connection.uri", MONGO_URI))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def save_csv(df, name, limit=None, show=10):
    """Collect a (small, aggregated) Spark DataFrame and write it to output/<name>.csv.
    Uses only the standard library so Spark jobs do not depend on pandas. Returns the rows."""
    import csv
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    rows = (df.limit(limit) if limit else df).collect()
    cols = df.columns
    with open(os.path.join(OUTPUT_DIR, f"{name}.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r[c] for c in cols])
    if show:
        widths = [max(len(str(c)), *(len(_fmt(r[c])) for r in rows[:show])) if rows else len(c) for c in cols]
        print("   " + "  ".join(str(c).ljust(wd) for c, wd in zip(cols, widths)))
        for r in rows[:show]:
            print("   " + "  ".join(_fmt(r[c]).ljust(wd) for c, wd in zip(cols, widths)))
        if len(rows) > show:
            print(f"   ... ({len(rows)} rows -> output/{name}.csv)")
    return rows


def _fmt(v):
    return f"{v:,.2f}" if isinstance(v, float) else str(v)


class Timer:
    """Context manager to print elapsed time for a pipeline step."""
    def __init__(self, label):
        self.label = label

    def __enter__(self):
        self.t0 = time.time()
        print(f"[..] {self.label}")
        return self

    def __exit__(self, *exc):
        print(f"[ok] {self.label} ({time.time() - self.t0:.1f}s)")
