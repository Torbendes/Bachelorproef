import requests
import pyodbc
import json
import os
from datetime import datetime, timedelta

API_URL = "https://archive-api.open-meteo.com/v1/archive"
CACHE_DIR = "../data/weather_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def cache_filename(lat, lon, date):
    return f"{CACHE_DIR}/{round(lat,4)}_{round(lon,4)}_{date}.json"

def load_or_fetch_day(lat, lon, date):
    filename = cache_filename(lat, lon, date)
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f), False

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date,
        "end_date": date,
        "hourly": "temperature_2m,precipitation,windgusts_10m",
        "timezone": "Europe/Brussels"
    }
    r = requests.get(API_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()["hourly"]

    with open(filename, "w") as f:
        json.dump(data, f)

    return data, True

def load_or_fetch_weather(lat, lon, start_date, end_date):
    current = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date)
    merged = {"time": [], "temperature_2m": [], "precipitation": [], "windgusts_10m": []}
    api_used_total = False

    while current <= end:
        date_str = current.date().isoformat()
        day_data, api_used = load_or_fetch_day(lat, lon, date_str)
        api_used_total = api_used_total or api_used

        for key in merged.keys():
            if key in day_data:
                merged[key].extend(day_data[key])

        current += timedelta(days=1)

    return merged, api_used_total

def compute_training_weather(hourly, start_ts, end_ts):
    temps, precs, gusts = [], [], []
    rain_hours, heat_hours = 0, 0

    for i, ts in enumerate(hourly["time"]):
        t = datetime.fromisoformat(ts)
        if start_ts <= t < end_ts:
            temp = hourly["temperature_2m"][i]
            precip = hourly["precipitation"][i]
            gust = hourly["windgusts_10m"][i]

            temps.append(temp)
            precs.append(precip)
            gusts.append(gust)

            if precip > 0:
                rain_hours += 1
            if temp >= 20:
                heat_hours += 1

    if not temps:
        return (None, None, None, None, None)

    return (
        round(sum(temps)/len(temps), 1),
        round(sum(precs), 2),
        round(max(gusts), 1),
        rain_hours,
        heat_hours
    )

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
                  SELECT 1
                  FROM WedstrijdWeerVooraf ww
                  WHERE ww.wedstrijd_id = w.wedstrijd_id
              )
        """)
        matches = cursor.fetchall()
        total = len(matches)

        print(f"Start import trainingsweer (48u vooraf) voor {total} wedstrijden")

        api_calls, cache_hits = 0, 0

        for i, (wid, datum, tijd, lat, lon) in enumerate(matches, start=1):
            match_dt = datetime.combine(datum, tijd)
            start_prev48 = match_dt - timedelta(hours=48)

            start_date = start_prev48.date().isoformat()
            end_date = match_dt.date().isoformat()

            try:
                hourly, api_used = load_or_fetch_weather(lat, lon, start_date, end_date)
                api_calls += int(api_used)
                cache_hits += int(not api_used)

                training_weather = compute_training_weather(hourly, start_prev48, match_dt)

                cursor.execute("""
                    INSERT INTO WedstrijdWeerVooraf (
                        wedstrijd_id,
                        temperatuur_gem_laatste48_c,
                        neerslag_som_laatste48_mm,
                        windstoten_max_laatste48_m_s,
                        regen_uren_laatste48,
                        hitte_uren_laatste48
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                """, wid, *training_weather)

                if i % 10 == 0 or i == total:
                    print(f"{i}/{total} verwerkt | API: {api_calls} | cache: {cache_hits}")

            except Exception as e:
                print(f"Fout bij wedstrijd {wid}: {e}")

        conn.commit()

print("Import klaar!")
print(f"API calls: {api_calls}, Cache hits: {cache_hits}")
