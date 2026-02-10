import requests
import pyodbc
import json
import os
from datetime import datetime, timedelta

# ======================
# Config
# ======================
API_URL = "https://archive-api.open-meteo.com/v1/archive"
CACHE_DIR = "code/weather_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# ======================
# DB connectie
# ======================
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Jupiler_Pro_League_Matches;"
    "Trusted_Connection=yes;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# ======================
# Helpers
# ======================
def cache_filename(lat, lon, date):
    lat = round(lat, 4)
    lon = round(lon, 4)
    return f"{CACHE_DIR}/{lat}_{lon}_{date}.json"

def load_or_fetch_weather(lat, lon, date):
    filename = cache_filename(lat, lon, date)

    # 📂 Cache hit
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f), False

    # 🌐 Cache miss → API call
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date,
        "end_date": date,
        "hourly": (
            "temperature_2m,precipitation,relativehumidity_2m,"
            "windspeed_10m,winddirection_10m,windgusts_10m,"
            "cloudcover,weathercode,pressure_msl"
        ),
        "timezone": "Europe/Brussels"
    }

    response = requests.get(API_URL, params=params, timeout=20)
    response.raise_for_status()
    data = response.json()

    with open(filename, "w") as f:
        json.dump(data, f)

    return data, True

def extract_hour(data, date, hour):
    timestamp = f"{date}T{hour}:00"
    idx = data["hourly"]["time"].index(timestamp)
    return {
        "temperature_2m": data["hourly"]["temperature_2m"][idx],
        "precipitation": data["hourly"]["precipitation"][idx],
        "relative_humidity_2m": data["hourly"]["relativehumidity_2m"][idx],
        "windspeed_10m": data["hourly"]["windspeed_10m"][idx],
        "winddirection_10m": data["hourly"]["winddirection_10m"][idx],
        "windgusts_10m": data["hourly"]["windgusts_10m"][idx],
        "cloudcover": data["hourly"]["cloudcover"][idx],
        "weathercode": data["hourly"]["weathercode"][idx],
        "pressure_msl": data["hourly"]["pressure_msl"][idx],
    }

def compute_hourly_avg(data, date, hour, window=1):
    """Gemiddelde ±window uur rond opgegeven uur"""
    idxs = [i for i, ts in enumerate(data["hourly"]["time"])
            if f"{date}T" in ts and abs(int(ts[11:13]) - int(hour)) <= window]

    temps = [data["hourly"]["temperature_2m"][i] for i in idxs]
    precs = [data["hourly"]["precipitation"][i] for i in idxs]
    winds = [data["hourly"]["windspeed_10m"][i] for i in idxs]
    gusts = [data["hourly"]["windgusts_10m"][i] for i in idxs]

    return {
        "temperature_avg": round(sum(temps)/len(temps), 1) if temps else None,
        "precipitation_sum": round(sum(precs), 2) if precs else None,
        "windspeed_avg": round(sum(winds)/len(winds), 1) if winds else None,
        "windgusts_max": round(max(gusts), 1) if gusts else None
    }

# ======================
# Wedstrijden ophalen
# ======================
cursor.execute("""
    SELECT w.id, w.wedstrijddatum, w.wedstrijdtijd,
           t.latitude, t.longitude
    FROM Wedstrijden w
    JOIN Teams t ON w.hometeam = t.team
    WHERE t.latitude IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM WedstrijdWeer ww WHERE ww.wedstrijdid = w.id
      )
""")
matches = cursor.fetchall()
total = len(matches)
print(f"🌦️ Start import voor {total} wedstrijden\n")

api_calls = 0
cache_hits = 0

# ======================
# Verwerking
# ======================
for i, (wedstrijd_id, datum, tijd, lat, lon) in enumerate(matches, start=1):
    date_str = datum.strftime("%Y-%m-%d")
    hour_str = tijd.strftime("%H")

    try:
        data, api_used = load_or_fetch_weather(lat, lon, date_str)
        api_calls += int(api_used)
        cache_hits += int(not api_used)

        weather_hour = extract_hour(data, date_str, hour_str)
        weather_avg = compute_hourly_avg(data, date_str, hour_str)

        # INSERT
        cursor.execute("""
            INSERT INTO WedstrijdWeer (
                wedstrijdid,
                temperature_2m,
                precipitation,
                relative_humidity_2m,
                windspeed_10m,
                winddirection_10m,
                windgusts_10m,
                cloudcover,
                weathercode,
                pressure_msl,
                temperature_avg,
                precipitation_sum,
                windspeed_avg,
                windgusts_max
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, wedstrijd_id,
             weather_hour["temperature_2m"],
             weather_hour["precipitation"],
             weather_hour["relative_humidity_2m"],
             weather_hour["windspeed_10m"],
             weather_hour["winddirection_10m"],
             weather_hour["windgusts_10m"],
             weather_hour["cloudcover"],
             weather_hour["weathercode"],
             weather_hour["pressure_msl"],
             weather_avg["temperature_avg"],
             weather_avg["precipitation_sum"],
             weather_avg["windspeed_avg"],
             weather_avg["windgusts_max"])

        if i % 10 == 0 or i == total:
            percent = (i / total) * 100
            print(f"✅ {i}/{total} ({percent:.1f}%) | API calls: {api_calls} | cache hits: {cache_hits}")

    except Exception as e:
        print(f"❌ Wedstrijd {wedstrijd_id}: {e}")

# ======================
# Afronden
# ======================
conn.commit()
cursor.close()
conn.close()

print("\n🎉 Klaar!")
print(f"🌐 API calls gedaan: {api_calls}")
print(f"📂 Cache hits: {cache_hits}")
