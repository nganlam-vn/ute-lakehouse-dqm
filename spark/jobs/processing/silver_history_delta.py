# silver_history_delta.py
# Bronze Parquet -> Silver Delta (full overwrite, partition event_date)

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# ==== chỉnh nếu bạn đổi thông số MinIO/Bucket ====
S3_ENDPOINT   = "http://minio:9000"
S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "minio123"
BRONZE_PATH   = "s3a://datalake/bronze/parquet/weather_history"
SILVER_PATH   = "s3a://datalake/silver/history_delta"
HMS_REGISTER  = False                    # True: đăng ký bảng vào Hive Metastore
HMS_DB        = "silver"                 # schema trong HMS
HMS_TABLE     = "weather_history"        # tên bảng trong HMS
# ================================================

spark = (
    SparkSession.builder.appName("silver_history_delta")
    # Delta extensions (yêu cầu bạn submit kèm gói delta-spark)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # MinIO S3A
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

df = spark.read.parquet(BRONZE_PATH)

# Chuẩn tên cột
for c in df.columns:
    df = df.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))

# Xác định cột thời gian
ts_col = next((c for c in ["datetime_utc","event_ts","timestamp","datetime","time","dt","date"]
               if c in df.columns), None)

if ts_col:
    df = df.withColumn("event_ts", F.to_timestamp(F.col(ts_col)))
else:
    df = df.withColumn("event_ts", F.current_timestamp())

df = df.withColumn("event_date", F.to_date("event_ts"))

# Ép kiểu các cột đo lường thường gặp (nếu tồn tại)
for col in ["temp","feels_like","humidity","pressure","wind_speed","wind_deg","rain_1h","pop","clouds"]:
    if col in df.columns:
        df = df.withColumn(col, F.col(col).cast("double"))

df = df.withColumn("ingest_ts", F.current_timestamp())

# Ghi Delta (full overwrite)
(df.write
   .format("delta")
   .option("overwriteSchema","true")
   .mode("overwrite")
   .partitionBy("event_date")
   .save(SILVER_PATH)
)

# (Tuỳ chọn) đăng ký vào Hive Metastore để Trino/Spark cùng thấy
if HMS_REGISTER:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HMS_DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HMS_DB}.{HMS_TABLE}
        USING DELTA
        LOCATION '{SILVER_PATH}'
    """)

spark.stop()
