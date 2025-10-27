# silver_hourly_delta.py
# Bronze Parquet -> Silver Delta (upsert theo (city, datetime_utc))

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# ==== chỉnh nếu bạn đổi thông số MinIO/Bucket ====
S3_ENDPOINT   = "http://minio:9000"
S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "minio123"
BRONZE_PATH   = "s3a://datalake/bronze/parquet/weather_forecast"
SILVER_PATH   = "s3a://datalake/silver/hourly_forecast_delta"
HMS_REGISTER  = False
HMS_DB        = "silver"
HMS_TABLE     = "hourly_forecast"
# ================================================

spark = (
    SparkSession.builder.appName("silver_hourly_delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

src = spark.read.parquet(BRONZE_PATH)

# Chuẩn cột
for c in src.columns:
    src = src.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))

if "city" not in src.columns:
    src = src.withColumn("city", F.lit("Unknown"))

src = src.withColumn("city", F.initcap(F.trim(F.col("city"))))

# Parse timestamp
ts_col = "datetime_utc" if "datetime_utc" in src.columns else None
if ts_col:
    src = src.withColumn("datetime_utc", F.to_timestamp("datetime_utc"))
else:
    src = src.withColumn("datetime_utc", F.to_timestamp(F.col("event_ts")))

src = src.withColumn("event_date", F.to_date("datetime_utc"))
src = src.withColumn("ingest_ts", F.current_timestamp())

# Ép kiểu số
for col in ["temp","feels_like","humidity","pressure","wind_speed","wind_deg","rain_1h","pop","clouds"]:
    if col in src.columns:
        src = src.withColumn(col, F.col(col).cast("double"))

# Upsert vào Delta theo (city, datetime_utc)
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    tgt = DeltaTable.forPath(spark, SILVER_PATH)
    (tgt.alias("t")
        .merge(src.alias("s"), "t.city = s.city AND t.datetime_utc = s.datetime_utc")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
     )
else:
    (src.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("event_date","city")
        .save(SILVER_PATH)
     )

# (Tuỳ chọn) đăng ký vào Hive Metastore
if HMS_REGISTER:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HMS_DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HMS_DB}.{HMS_TABLE}
        USING DELTA
        LOCATION '{SILVER_PATH}'
    """)

spark.stop()
