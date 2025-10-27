# ========== 0) Imports ==========
import os, sys, uuid
from datetime import datetime
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

print("Imports loaded")

# ========== 1) Phiên bản & Packages (Spark 3.5.x) ==========
DELTA_VER   = "3.2.0"   # cho Spark 3.5.x
HADOOP_VER  = "3.3.4"   # nếu có trục trặc, dùng 3.3.2
AWS_SDK_VER = "1.12.262"

# (Trong container Bitnami đã có Java; thường KHÔNG cần set JAVA_HOME)
# os.environ["JAVA_HOME"] = "/path/to/jdk-17"

SUBMIT_ARGS = (
    f"--packages "
    f"org.apache.hadoop:hadoop-aws:{HADOOP_VER},"
    f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VER},"
    f"io.delta:delta-spark_2.12:{DELTA_VER} pyspark-shell"   # <= _2.12 !!!
)
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ["PYSPARK_PYTHON"] = sys.executable

# ========== 2) Spark Session (Delta + Hive) ==========
spark = (
    SparkSession.builder
    .appName("Delta-MinIO-Hive-Spark35")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "4g")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    # Delta Lake for Spark 3.5.x
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Ép warehouse dùng S3A (tránh local file://)
    .config("spark.sql.warehouse.dir", "s3a://warehouse")
    .config("spark.hadoop.hive.metastore.warehouse.dir", "s3a://warehouse")

    # Kích hoạt Hive + chỉ ra HMS
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()

    # (tuỳ chọn) không convert parquet metastore
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    .getOrCreate()
)

# ========== 3) S3A (MinIO) ==========
hc = spark._jsc.hadoopConfiguration()
hc.set("fs.s3a.endpoint", "http://minio:9000")
hc.set("fs.s3a.access.key", "admin")       # đổi theo MinIO của bạn
hc.set("fs.s3a.secret.key", "password")    # đổi theo MinIO của bạn
hc.set("fs.s3a.path.style.access", "true")
hc.set("fs.s3a.connection.ssl.enabled", "false")
hc.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hc.set("fs.s3.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem") 
hc.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
hc.set("fs.s3a.connection.establish.timeout", "60000")
hc.set("fs.s3a.connection.timeout", "60000")
hc.set("fs.s3a.connection.request.timeout", "60000")
hc.set("fs.s3a.attempts.maximum", "10")
hc.set("fs.s3a.threads.keepalivetime", "60")

# ========== 4) Fake data ==========
from faker import Faker
faker = Faker()
def get_customer_data(total_customers=10000):
    rows = []
    now_iso = datetime.now().isoformat()
    for _ in range(total_customers):
        rows.append({
            "customer_id": str(uuid.uuid4()),
            "name": faker.name(),
            "created_at": now_iso,
            "address": faker.address(),
            "state": str(faker.state_abbr()),
            "salary": str(faker.random_int(min=30000, max=100000))
        })
    return rows

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name",        StringType(), True),
    StructField("created_at",  StringType(), True),
    StructField("address",     StringType(), True),
    StructField("state",       StringType(), True),
    StructField("salary",      StringType(), True),
])

df = spark.createDataFrame(get_customer_data(10000), schema=schema)
df.show(1, truncate=True); df.printSchema()

# ========== 5) Ghi Delta ==========
database   = "default"
table_name = "customers_t1"
path = f"s3a://warehouse/{database}/{table_name}"   # đảm bảo bucket 'warehouse' đã tạo trong MinIO

(
    df.write
      .format("delta")
      .partitionBy("state")
      .mode("append")                 # "overwrite" nếu muốn ghi lại toàn bộ
      .option("mergeSchema", "true")
      .save(path)
)

# ========== 6) Đăng ký Hive Metastore (External) ==========
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")
spark.sql(f"""
    CREATE TABLE {database}.{table_name}
    USING DELTA
    LOCATION '{path}'
""")

# lấy version/timestamp mới nhất từ Delta history
last = spark.sql(f"DESCRIBE HISTORY {database}.{table_name} LIMIT 1")
row = last.select("version", "timestamp").collect()[0]
ver = row["version"]
ts  = row["timestamp"]  # TimestampType

spark.sql(f"""
  ALTER TABLE {database}.{table_name}
  SET TBLPROPERTIES (
    'last_commit_version'='{ver}',
    'last_commit_ts'='{ts}'
  )
""")


# Kiểm tra đọc lại
spark.read.format("delta").load(path).show(3, truncate=False)
