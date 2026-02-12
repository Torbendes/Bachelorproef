import requests
import pyodbc
import time
from datetime import datetime

# ======================
# CONFIG
# ======================
API_KEY = "2a62f800503644f9bdd1dd08f4f0f95b"
BASE_URL = "https://api.football-data.org/v4/competitions"
HEADERS = {
    "X-Auth-Token": API_KEY
}

eu_competitions = ["CL", "EL", "EC"]
start_season = 2019
current_year = datetime.now().year

# ======================
# DB CONNECTIE
# ======================
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Jupiler_Pro_League_Matches;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

# ======================
# DATA OPHALEN PER SEIZOEN
# ======================
for comp in eu_competitions:
    for season in range(start_season, current_year + 1):

        print(f"\n📡 Ophalen: {comp} - seizoen {season}")

        url = f"{BASE_URL}/{comp}/matches"
        params = {"season": season}

        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=15)

            if response.status_code == 403:
                print(f"⛔ Geen toegang tot {comp} seizoen {season} (403)")
                continue

            if response.status_code == 429:
                print("⏳ Rate limit bereikt. Wachten 60 seconden...")
                time.sleep(60)
                continue

            response.raise_for_status()
            data = response.json()

        except Exception as e:
            print(f"❌ Fout bij ophalen: {e}")
            continue

        matches = data.get("matches", [])
        print(f"✔ {len(matches)} wedstrijden gevonden")

        for m in matches:
            match_id = m["id"]
            match_date = datetime.fromisoformat(
                m["utcDate"].replace("Z", "+00:00")
            ).date()
            home = m["homeTeam"]["name"]
            away = m["awayTeam"]["name"]

            try:
                cursor.execute("""
                    INSERT INTO EuropeesFixture
                    (id, competition, season, match_date, home_team, away_team)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                match_id,
                comp,
                season,
                match_date,
                home,
                away
                )
            except pyodbc.IntegrityError:
                # Match bestaat al (primary key)
                pass
            except Exception as e:
                print(f"⚠️ Insert fout: {e}")

        conn.commit()

        # Rate limit bescherming (gratis plan = 10 requests/min)
        time.sleep(6)

print("\n🎉 Europese fixtures 2019 → nu verwerkt!")
cursor.close()
conn.close()
