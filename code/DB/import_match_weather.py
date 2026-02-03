import requests
import pyodbc
from datetime import datetime

# DB connectie
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Jupiler_Pro_League_Matches;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

API_URL = "https://archive-api.open-meteo.com/v1/archive"

# Cache om dubbele API-calls te vermijden
weather_cache = {}

def fetch_weather(lat, lon, date, hour):
    cache_key = (lat, lon, date, hour)
    if cache_key in weather_cache:
        return weather_cache[cache_key]

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

    timestamp = f"{date}T{hour}:00"
    idx = data["hourly"]["time"].index(timestamp)

    result = (
        data["hourly"]["temperature_2m"][idx],
        data["hourly"]["precipitation"][idx],
        data["hourly"]["relativehumidity_2m"][idx],
        data["hourly"]["windspeed_10m"][idx],
        data["hourly"]["winddirection_10m"][idx],
        data["hourly"]["windgusts_10m"][idx],
        data["hourly"]["cloudcover"][idx],
        data["hourly"]["weathercode"][idx],
        data["hourly"]["pressure_msl"][idx],
    )

    weather_cache[cache_key] = result
    return result

# Wedstrijden ophalen
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

print(f"Start import van weerdata voor {total} wedstrijden...\n")

for i, (wedstrijd_id, datum, tijd, lat, lon) in enumerate(matches, start=1):
    date_str = datum.strftime("%Y-%m-%d")
    hour_str = tijd.strftime("%H")

    try:
        weather = fetch_weather(lat, lon, date_str, hour_str)

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
                pressure_msl
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, wedstrijd_id, *weather)

        if i % 10 == 0 or i == total:
            percent = (i / total) * 100
            print(f"✅ {i}/{total} wedstrijden verwerkt ({percent:.1f}%)")

    except Exception as e:
        print(f"❌ Fout bij wedstrijd {wedstrijd_id}: {e}")

conn.commit()
cursor.close()
conn.close()

print("\nAlle beschikbare weerdata succesvol opgeslagen!")
