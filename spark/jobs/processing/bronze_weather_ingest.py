# -*- coding: utf-8 -*-
# RAW (JSON gz trên MinIO) -> BRONZE (Iceberg tables via Nessie)
# Yêu cầu: spark-defaults.conf đã cấu hình Nessie + S3A như bạn đang dùng.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# --------- Cấu hình đường dẫn RAW (đúng theo script crawl bạn đã dùng) ----------
RAW_OPENWEATHER = "s3a://datalake/raw/weather/openweather/hourly/city=*/dt=*/batch_ts=*.json.gz"
RAW_OPENMETEO   = "s3a://datalake/raw/weather/open-meteo/city=*/dt=*/batch_ts=*.json.gz"

# --------- Tên bảng BRONZE (Nessie catalog) ----------
TBL_BZ_OW  = "nessie.weather.openweather_hourly_bronze"
TBL_BZ_OM  = "nessie.weather.openmeteo_hourly_bronze"
NS_BZ      = "nessie.weather"  # namespace

def create_namespace_if_needed(spark: SparkSession):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS_BZ}")

def write_df_to_iceberg(df, table_name: str, partition_cols=None):
    """
    Ghi vào bảng Iceberg. Nếu bảng chưa có -> tạo với partition được chỉ định.
    """
    if partition_cols is None: partition_cols = []
    # Nếu bảng chưa tồn tại -> tạo
    tables = [r.tableName for r in spark.catalog.listTables(NS_BZ.split('.', 1)[1] if '.' in NS_BZ else NS_BZ)]
    # Lưu ý: listTables trên Spark không trả đủ khi dùng Nessie; ta thử-catch SQL CREATE TABLE
    try:
        spark.table(table_name)
        exists = True
    except Exception:
        exists = False

    if not exists:
        # create empty table with schema của df
        cols = ",\n  ".join([f"`{c}` {t.simpleString()}" for c, t in zip(df.columns, df.schema.fields)])
        part = f"PARTITIONED BY ({', '.join([f'`{c}`' for c in partition_cols])})" if partition_cols else ""
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
              {cols}
            )
            USING iceberg
            {part}
        """)

    # Ghi append
    (df
     .write
     .format("iceberg")
     .mode("append")
     .save(table_name))

def process_openweather_bronze(spark: SparkSession):
    # đọc 1-object JSON (không phải NDJSON)
    df_raw = (spark.read
              .option("multiLine", True)  # đề phòng file ghi dạng JSON pretty/array
              .json(RAW_OPENWEATHER))

    # Lấy city/country/lat/lon/timezone ở cấp top-level
    top = df_raw.select(
        F.col("city").alias("_city_top"),
        F.col("country").alias("_country_top"),
        F.col("lat").alias("_lat_top"),
        F.col("lon").alias("_lon_top"),
        F.col("provider"),
        F.col("ingest_ts_utc"),
        F.col("forecast.list").alias("_list"),
        F.col("forecast.city.timezone").cast("long").alias("_tz_offset")  # seconds (VD: 25200 = UTC+7)
    )

    exploded = top.select(
        "_city_top","_country_top","_lat_top","_lon_top","provider","ingest_ts_utc","_tz_offset",
        F.explode_outer(F.col("_list")).alias("item")
    )

    # item as JSON để bắt safely các field có thể vắng (rain/snow)
    item_json = F.to_json(F.col("item"))
    j = lambda path: F.get_json_object(item_json, path)

    # dt là epoch **UTC**, còn dt_txt là local; field `forecast.city.timezone` = offset seconds
    df_bz = exploded.select(
        F.col("_city_top").cast("string").alias("city"),
        F.col("_country_top").cast("string").alias("country"),
        F.col("_lat_top").cast("double").alias("lat"),
        F.col("_lon_top").cast("double").alias("lon"),
        "provider",
        F.to_timestamp("ingest_ts_utc", "yyyyMMdd'T'HHmmss'Z'").alias("ingest_ts_utc"),
        F.col("_tz_offset").alias("timezone_seconds"),

        F.col("item.dt").cast("long").alias("event_unix_localOffset"),    # epoch (UTC) nhưng dt_txt là local
        F.col("item.dt_txt").cast("string").alias("event_local_text"),

        F.col("item.main.temp").cast("double").alias("temp"),
        F.col("item.main.feels_like").cast("double").alias("feels_like"),
        F.col("item.main.pressure").cast("double").alias("pressure_hpa"),
        F.col("item.main.humidity").cast("double").alias("humidity_pct"),
        F.col("item.visibility").cast("double").alias("visibility_m"),
        F.col("item.clouds.all").cast("double").alias("clouds_pct"),
        F.col("item.wind.speed").cast("double").alias("wind_speed_ms"),
        F.col("item.wind.deg").cast("double").alias("wind_deg"),
        F.col("item.wind.gust").cast("double").alias("wind_gust_ms"),
        F.col("item.pop").cast("double").alias("precip_probability"),

        j("$.rain.1h").cast("double").alias("rain_1h_mm"),
        j("$.snow.1h").cast("double").alias("snow_1h_mm"),
        F.col("item.weather").alias("_weather_arr")
    ).withColumn(
        # event_ts_utc: dt là epoch UTC (seconds) → chuyển timestamp UTC
        "event_ts_utc", F.to_utc_timestamp(F.from_unixtime("event_unix_localOffset"), "UTC")
    ).withColumn(
        # event_ts_local: nếu cần, chuyển về giờ địa phương theo offset trong payload
        "event_ts_local",
        F.from_unixtime(F.col("event_unix_localOffset") + F.col("timezone_seconds"))
    ).withColumn(
        "event_date", F.to_date("event_ts_utc")
    ).withColumn(
        "weather_id",   F.col("_weather_arr").getItem(0).getField("id").cast("int")
    ).withColumn(
        "weather_main", F.col("_weather_arr").getItem(0).getField("main").cast("string")
    ).withColumn(
        "weather_desc_vi", F.col("_weather_arr").getItem(0).getField("description").cast("string")
    ).withColumn(
        "weather_icon", F.col("_weather_arr").getItem(0).getField("icon").cast("string")
    ).drop("_weather_arr")

    # Ghi Iceberg, partition theo ngày UTC
    write_df_to_iceberg(
        df_bz.select(
            "city","country","lat","lon","provider",
            "event_ts_utc","event_ts_local","event_date","timezone_seconds",
            "temp","feels_like","pressure_hpa","humidity_pct","clouds_pct",
            "wind_speed_ms","wind_deg","wind_gust_ms",
            "rain_1h_mm","snow_1h_mm","precip_probability",
            "weather_id","weather_main","weather_desc_vi","weather_icon",
            "visibility_m","ingest_ts_utc","event_local_text"
        ),
        TBL_BZ_OW,
        partition_cols=["event_date"]
    )


def process_openmeteo_bronze(spark: SparkSession):
    df_raw = (spark.read
              .option("multiLine", False)  # NDJSON mỗi dòng 1 object
              .json(RAW_OPENMETEO))

    # ép kiểu số, dựng timestamp UTC từ 'date'
    numeric_cols = [c for c in df_raw.columns if c != "date"]
    df_cast = df_raw
    for c in numeric_cols:
        df_cast = df_cast.withColumn(c, F.col(c).cast("double"))

    df_bz = (df_cast
        .withColumn("event_ts_utc", F.to_timestamp("date", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("event_date", F.to_date("event_ts_utc"))
        .withColumn("provider", F.lit("open-meteo"))
    )

    write_df_to_iceberg(df_bz, TBL_BZ_OM, partition_cols=["event_date"])


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("bronze_weather_ingest")
             # spark-defaults.conf đã có Iceberg + Nessie + S3A; không cần add thêm ở đây
             .getOrCreate())

    # Đảm bảo namespace tồn tại
    create_namespace_if_needed(spark)

    # Xử lý từng nguồn
    process_openweather_bronze(spark)
    process_openmeteo_bronze(spark)

    spark.stop()
