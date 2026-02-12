import requests
import pyodbc
import json
import os
from datetime import datetime

API_URL = "https://archive-api.open-meteo.com/v1/archive"
CACHE_DIR = "../data/weather_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def cache_filename(lat, lon, date):
    return f"{CACHE_DIR}/{round(lat,4)}_{round(lon,4)}_{date}.json"

def load_or_fetch_weather(lat, lon, date):
    filename = cache_filename(lat, lon, date)
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f), False

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

    data = requests.get(API_URL, params=params, timeout=20).json()
    with open(filename, "w") as f:
        json.dump(data, f)
    return data, True

def extract_hour(data, date, hour):
    ts = f"{date}T{hour}:00"
    idx = data["hourly"]["time"].index(ts)

    keys = [
        "temperature_2m", "precipitation", "relativehumidity_2m",
        "windspeed_10m", "winddirection_10m", "windgusts_10m",
        "cloudcover", "weathercode", "pressure_msl"
    ]
    return {k: data["hourly"].get(k, [None]*len(data["hourly"]["time"]))[idx] for k in keys}

def compute_hourly_avg(data, date, hour, window=1):
    idxs = [i for i, ts in enumerate(data["hourly"]["time"])
            if f"{date}T" in ts and abs(int(ts[11:13]) - int(hour)) <= window]

    def avg(key, func=sum):
        vals = [data["hourly"].get(key, [None]*len(data["hourly"]["time"]))[i] for i in idxs]
        vals = [v for v in vals if v is not None]
        if not vals:
            return None
        return round(func(vals)/len(vals),1) if func==sum else round(func(vals),1)

    return {
        "temperatuur_gem_c": avg("temperature_2m"),
        "neerslag_som_mm": avg("precipitation", sum),
        "windsnelheid_gem_m_s": avg("windspeed_10m"),
        "windstoten_max_m_s": avg("windgusts_10m", max)
    }

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=JPL_Data;"
    "Trusted_Connection=yes;"
)

with pyodbc.connect(conn_str) as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT w.wedstrijd_id, w.wedstrijd_datum, w.wedstrijd_tijd,
                   t.latitude, t.longitude
            FROM Wedstrijd w
            JOIN Team t ON w.thuis_team = t.team_naam
            WHERE t.latitude IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM WedstrijdWeer ww WHERE ww.wedstrijd_id = w.wedstrijd_id
              )
        """)
        matches = cursor.fetchall()
        total = len(matches)
        print(f"Start import voor {total} wedstrijden")

        api_calls = 0
        cache_hits = 0

        for i, (wedstrijd_id, datum, tijd, lat, lon) in enumerate(matches, start=1):
            date_str = datum.strftime("%Y-%m-%d")
            hour_str = tijd.strftime("%H")

            try:
                data, api_used = load_or_fetch_weather(lat, lon, date_str)
                api_calls += int(api_used)
                cache_hits += int(not api_used)

                weather_hour = extract_hour(data, date_str, hour_str)
                weather_avg = compute_hourly_avg(data, date_str, hour_str)

                cursor.execute("""
                    INSERT INTO WedstrijdWeer (
                        wedstrijd_id,
                        temperatuur_c,
                        neerslag_mm,
                        relatieve_luchtvochtigheid_pct,
                        windsnelheid_m_s,
                        windrichting_graden,
                        windstoten_m_s,
                        bewolking_pct,
                        weercode,
                        luchtdruk_hpa,
                        temperatuur_gem_c,
                        neerslag_som_mm,
                        windsnelheid_gem_m_s,
                        windstoten_max_m_s
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                wedstrijd_id,
                weather_hour["temperature_2m"],
                weather_hour["precipitation"],
                weather_hour["relativehumidity_2m"],
                weather_hour["windspeed_10m"],
                weather_hour["winddirection_10m"],
                weather_hour["windgusts_10m"],
                weather_hour["cloudcover"],
                weather_hour["weathercode"],
                weather_hour["pressure_msl"],
                weather_avg["temperatuur_gem_c"],
                weather_avg["neerslag_som_mm"],
                weather_avg["windsnelheid_gem_m_s"],
                weather_avg["windstoten_max_m_s"]
                )

                if i % 10 == 0 or i == total:
                    print(f"{i}/{total} verwerkt | API: {api_calls} | cache: {cache_hits}")

            except Exception as e:
                print(f"Fout bij wedstrijd {wedstrijd_id}: {e}")

        conn.commit()

print("Import klaar!")
print(f"API calls: {api_calls}, Cache hits: {cache_hits}")
