# spark/jobs/raw_to_bronze_parquet.py
import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# ---- CLI args ----
# 1: dataset name       (vd: weather_history)
# 2: input path (raw)   (vd: s3a://datalake/raw/weather_data.csv)
# 3: output base (bronze parquet dir) (vd: s3a://datalake/bronze/parquet)
# 4: format: csv|json   (vd: csv)
dataset   = sys.argv[1]
in_path   = sys.argv[2]
out_base  = sys.argv[3]
fmt       = sys.argv[4].lower()

# MinIO S3A (đổi nếu bạn đặt user/pass khác)
S3_ENDPOINT   = "http://minio:9000"
S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "minio123"

spark = (
    SparkSession.builder.appName(f"raw_to_bronze_parquet__{dataset}")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

# ---- Read raw ----
reader = spark.read
if fmt == "csv":
    df = reader.option("header", "true").option("inferSchema", "true").csv(in_path)
elif fmt == "json":
    df = reader.option("multiLine", "true").json(in_path)
else:
    raise SystemExit("format phải là csv hoặc json")

# Chuẩn tên cột
for c in df.columns:
    df = df.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))

# Gắn metadata
df = df.withColumn("_source_path", F.input_file_name())
df = df.withColumn("_ingest_ts", F.current_timestamp())

# Tìm cột thời gian hợp lý để partition
ts_candidates = ["datetime_utc", "timestamp", "datetime", "time", "dt", "date"]
ts_col = next((c for c in ts_candidates if c in df.columns), None)

if ts_col:
    df = df.withColumn("event_ts", F.to_timestamp(F.col(ts_col))) \
           .withColumn("event_date", F.to_date("event_ts"))

# Tùy chọn: ép kiểu các cột số phổ biến nếu tồn tại
num_cols = ["temp","feels_like","humidity","pressure","wind_speed","wind_deg","rain_1h","pop","clouds"]
for c in num_cols:
    if c in df.columns:
        df = df.withColumn(c, F.col(c).cast("double"))

# Chống file nhỏ & ghi Parquet
df = df.coalesce(1)  # lab/dev; prod: dùng auto-optimize/size-based
out_path = f"{out_base}/{dataset}"

writer = (
    df.write.mode("overwrite")   # hoặc "append" nếu bạn muốn tích luỹ
      .option("compression", "snappy")
      .format("parquet")
)

if "event_date" in df.columns:
    writer = writer.partitionBy("event_date")

writer.save(out_path)
spark.stop()
