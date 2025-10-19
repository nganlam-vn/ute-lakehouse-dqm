# open-meteo.py
import argparse, sys, os, time, random
from datetime import datetime, timedelta, timezone
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from minio import Minio
import pytz
import os

def utc_now_iso():
    return pd.Timestamp.utcnow().isoformat()

def add_audit_cols(df: pd.DataFrame) -> pd.DataFrame:
    ts = utc_now_iso()
    df = df.copy()
    # Tạo ID tăng dần từ 1 đến n
    df["cd_bronze_id"] = pd.Series(range(1, len(df) + 1), dtype="int64")
    df["dt_record_to_bronze"] = ts
    
    first = ["cd_bronze_id", "dt_record_to_bronze"]
    rest = [c for c in df.columns if c not in first]
    df = df[first + rest]
    return df

def minio_client(endpoint="minio:9000", access_key="minio", secret_key="minio123", secure=False): 
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def save_json(df: pd.DataFrame, path: str):
    df.to_json(path, orient="records", date_format="iso", force_ascii=False)
    print(f"Saved JSON to {path}")

def upload_minio(local_path, bucket="datalake", object_name="bronze/weather-history/openmeteo.json"):
    client = minio_client()
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    size = os.stat(local_path).st_size
    with open(local_path, "rb") as f:
        client.put_object(bucket, object_name, f, size, content_type="application/json")
    print(f"Uploaded to s3a://{bucket}/{object_name}")

def fetch(lat, lon, start_date, end_date, tz="Asia/Bangkok"):
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    client = openmeteo_requests.Client(session=retry_session)
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": start_date, "end_date": end_date,
        "hourly": [
            "temperature_2m","relative_humidity_2m","dew_point_2m",
            "apparent_temperature","rain","wind_speed_10m","wind_direction_10m",
            "wind_gusts_10m","cloud_cover","surface_pressure","pressure_msl",
            "weather_code","soil_temperature_0_to_7cm","soil_moisture_0_to_7cm",
            "et0_fao_evapotranspiration","precipitation"
        ],
        "timezone": tz,
    }
    resp = client.weather_api(url, params=params)[0]
    hr = resp.Hourly()
    data = {"date": pd.date_range(
        start=pd.to_datetime(hr.Time(), unit="s", utc=True),
        end=pd.to_datetime(hr.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hr.Interval()),
        inclusive="left"
    )}
    names = [
        "temperature_2m","relative_humidity_2m","dew_point_2m","apparent_temperature","rain",
        "wind_speed_10m","wind_direction_10m","wind_gusts_10m","cloud_cover","surface_pressure",
        "pressure_msl","weather_code","soil_temperature_0_to_7cm","soil_moisture_0_to_7cm",
        "et0_fao_evapotranspiration","precipitation"
    ]
    for i, n in enumerate(names):
        data[n] = hr.Variables(i).ValuesAsNumpy()
    return pd.DataFrame(data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily load: yesterday (local)")
    parser.add_argument("--lat", type=float, default=10.823)
    parser.add_argument("--lon", type=float, default=106.6296)
    parser.add_argument("--bucket", type=str, default="datalake")
    parser.add_argument("--prefix", type=str, default="bronze/weather-history/")
    parser.add_argument("--outdir", type=str, default=".")
    args = parser.parse_args()

    # --- LẤY NGÀY HÔM QUA (LOCAL) ---
    tz_name = "Asia/Ho_Chi_Minh"
    tz_loc = pytz.timezone(tz_name)
    today_local = datetime.now(tz_loc).date()
    day_local = today_local - timedelta(days=1)         # hôm qua theo giờ local
    day_ymd = day_local.strftime("%Y-%m-%d")
    day_compact = day_local.strftime("%Y%m%d")

    print(f"Daily load for {day_ymd} (local tz: {tz_name})")

    # chỉ lấy đúng 1 ngày: start_date = end_date = hôm qua
    df = fetch(args.lat, args.lon, day_ymd, day_ymd, tz=tz_name)

    if df.empty:
        print("No data returned from Open-Meteo. Skip upload.")
        sys.exit(0)

    df = add_audit_cols(df)

    # Lưu file local
    os.makedirs(args.outdir, exist_ok=True)
    local_file = os.path.join(args.outdir, f"openmeteo_{day_compact}.json")
    save_json(df, local_file)

    # Đường dẫn trên MinIO theo ngày (idempotent, mỗi ngày một partition)
    object_name = f"{args.prefix}date={day_ymd}/openmeteo_{day_compact}.json"
    upload_minio(local_file, bucket=args.bucket, object_name=object_name)
