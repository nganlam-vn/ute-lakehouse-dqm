import os, io, json, time, pathlib
import requests
from datetime import datetime
from minio import Minio

API_KEY = "59637b06c1662ddaf56a43239fc8dea7"

CITIES = [
    {"name": "Ho Chi Minh City", "country": "VN", "slug": "ho-chi-minh"},
    {"name": "Hanoi", "country": "VN", "slug": "hanoi"},
    {"name": "Da Nang", "country": "VN", "slug": "da-nang"},
    {"name": "Hai Phong", "country": "VN", "slug": "hai-phong"},
    {"name": "Can Tho", "country": "VN", "slug": "can-tho"},
    {"name": "Nha Trang", "country": "VN", "slug": "nha-trang"},
    {"name": "Hue", "country": "VN", "slug": "hue"},
    {"name": "Da Lat", "country": "VN", "slug": "da-lat"},
    {"name": "Vung Tau", "country": "VN", "slug": "vung-tau"},
    {"name": "Quy Nhon", "country": "VN", "slug": "quy-nhon"},
]

GEO_URL      = "https://api.openweathermap.org/geo/1.0/direct"
FORECAST_URL = "https://pro.openweathermap.org/data/2.5/forecast/hourly"

# ========= MinIO config =========
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_SECURE     = os.getenv("MINIO_SECURE",     "false").lower() == "true"
BUCKET           = os.getenv("MINIO_BUCKET",     "datalake")
BASE_PREFIX      = os.getenv("MINIO_BASE_PREFIX","bronze/hourly-forecast")

# ========= Helpers =========
def ensure_bucket(client: Minio, bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

def upload_json(client: Minio, bucket: str, object_name: str, obj):
    data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/json",
    )
    print(f"[OK] s3://{bucket}/{object_name} ({len(data)} bytes)")

def get_coords(city, country):
    r = requests.get(GEO_URL, params={"q": f"{city},{country}", "limit": 1, "appid": API_KEY})
    if r.status_code == 200 and len(r.json()) > 0:
        d = r.json()[0]
        return d["lat"], d["lon"]
    return None, None

def get_forecast(lat, lon):
    r = requests.get(
        FORECAST_URL,
        params={"lat": lat, "lon": lon, "appid": API_KEY, "units": "metric", "lang": "vi"},
    )
    if r.status_code == 200:
        return r.json()
    return None

# ========= Main =========
def main():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=MINIO_SECURE)
    ensure_bucket(client, BUCKET)

    utc_now = datetime.utcnow()
    dt = utc_now.strftime("%Y-%m-%d")
    ts = utc_now.strftime("%Y%m%dT%H%M%SZ")

    for city in CITIES:
        lat, lon = get_coords(city["name"], city["country"])
        if not lat or not lon:
            print(f"⚠️ Không tìm được tọa độ cho {city['name']}")
            continue

        print(f"📡 Lấy dữ liệu cho {city['name']} ({lat},{lon}) ...")
        data = get_forecast(lat, lon)
        if data is None:
            print(f"⚠️ Không lấy được dữ liệu cho {city['name']}")
            continue

        payload = {
            "city": city["name"],
            "country": city["country"],
            "lat": lat,
            "lon": lon,
            "provider": "openweather",
            "ingest_ts_utc": ts,
            "forecast": data,
        }

        # Ví dụ path: raw/weather/openweather/hourly/city=hanoi/dt=2025-10-17/batch_ts=20251017T000000Z.json
        key = f"{BASE_PREFIX}/city={city['slug']}/dt={dt}/batch_ts={ts}.json"

        upload_json(client, BUCKET, key, payload)
        time.sleep(1)

if __name__ == "__main__":
    main()
