import os
import csv
import pyodbc
from datetime import datetime

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Jupiler_Pro_League_Matches;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

def safe_int_or_none(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def get_or_create_team(team_name):
    cursor.execute(
        "SELECT team FROM Teams WHERE team = ?",
        team_name
    )
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO Teams (team) VALUES (?);",
            team_name
        )

def parse_date(date_str):
    return datetime.strptime(date_str, "%d/%m/%Y").date()

def clear_database():
    cursor.execute("DELETE FROM WedstrijdStatistieken;")
    cursor.execute("DELETE FROM Wedstrijden;")
    cursor.execute("DELETE FROM Teams;")
    conn.commit()

DATA_DIR = "../data"
clear_database()

for file_name in os.listdir(DATA_DIR):
    if not file_name.endswith(".csv"):
        continue

    with open(os.path.join(DATA_DIR, file_name), newline="", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            home_team = row["HomeTeam"]
            away_team = row["AwayTeam"]

            get_or_create_team(home_team)
            get_or_create_team(away_team)

            cursor.execute(
                """
                INSERT INTO Wedstrijden
                (div, wedstrijddatum, wedstrijdtijd, hometeam, awayteam)
                OUTPUT INSERTED.id
                VALUES (?, ?, ?, ?, ?);
                """,
                row["Div"],
                parse_date(row["Date"]),
                row["Time"],
                home_team,
                away_team
            )

            wedstrijd_id = cursor.fetchone()[0]

            cursor.execute(
                """
                INSERT INTO WedstrijdStatistieken (
                    wedstrijdid,
                    full_time_home_goals,
                    full_time_away_goals,
                    full_time_result,
                    half_time_home_goals,
                    half_time_away_goals,
                    half_time_result,
                    home_shots,
                    away_shots,
                    home_shots_on_target,
                    away_shots_on_target,
                    home_fouls,
                    away_fouls,
                    home_corners,
                    away_corners,
                    home_yellow_cards,
                    away_yellow_cards,
                    home_red_cards,
                    away_red_cards
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                wedstrijd_id,
                safe_int_or_none(row["FTHG"]),
                safe_int_or_none(row["FTAG"]),
                row["FTR"],
                safe_int_or_none(row["HTHG"]),
                safe_int_or_none(row["HTAG"]),
                row["HTR"],
                safe_int_or_none(row["HS"]),
                safe_int_or_none(row["AS"]),
                safe_int_or_none(row["HST"]),
                safe_int_or_none(row["AST"]),
                safe_int_or_none(row["HF"]),
                safe_int_or_none(row["AF"]),
                safe_int_or_none(row["HC"]),
                safe_int_or_none(row["AC"]),
                safe_int_or_none(row["HY"]),
                safe_int_or_none(row["AY"]),
                safe_int_or_none(row["HR"]),
                safe_int_or_none(row["AR"])
            )

    conn.commit()

cursor.close()
conn.close()

print("Import van alle voetbaldata is succesvol afgerond!")
