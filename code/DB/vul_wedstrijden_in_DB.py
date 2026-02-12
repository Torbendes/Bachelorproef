import os
import csv
import pyodbc
from datetime import datetime

def safe_int_or_none(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def parse_date(date_str):
    return datetime.strptime(date_str, "%d/%m/%Y").date()

DATA_DIR = "../data/matches"

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=JPL_Data;"
    "Trusted_Connection=yes;"
)

with pyodbc.connect(conn_str) as conn:
    with conn.cursor() as cursor:
        for file_name in os.listdir(DATA_DIR):
            if not file_name.endswith(".csv"):
                continue

            with open(os.path.join(DATA_DIR, file_name), newline="", encoding="utf-8-sig") as csvfile:
                reader = csv.DictReader(csvfile)

                for row in reader:
                    thuis_team = row["HomeTeam"]
                    uit_team = row["AwayTeam"]

                    cursor.execute(
                        """
                        INSERT INTO Wedstrijd
                        (divisie, wedstrijd_datum, wedstrijd_tijd, thuis_team, uit_team)
                        OUTPUT INSERTED.wedstrijd_id
                        VALUES (?, ?, ?, ?, ?);
                        """,
                        row["Div"],
                        parse_date(row["Date"]),
                        row["Time"],
                        thuis_team,
                        uit_team
                    )
                    wedstrijd_id = cursor.fetchone()[0]

                    cursor.execute(
                        """
                        INSERT INTO WedstrijdStatistiek (
                            wedstrijd_id,
                            doelpunten_thuis_voltijd,
                            doelpunten_uit_voltijd,
                            resultaat_voltijd,
                            doelpunten_thuis_halftijd,
                            doelpunten_uit_halftijd,
                            resultaat_halftijd,
                            schoten_thuis,
                            schoten_uit,
                            schoten_op_doel_thuis,
                            schoten_op_doel_uit,
                            overtredingen_thuis,
                            overtredingen_uit,
                            corners_thuis,
                            corners_uit,
                            gele_kaarten_thuis,
                            gele_kaarten_uit,
                            rode_kaarten_thuis,
                            rode_kaarten_uit
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

print("Import van alle voetbalmatchen is klaar!")
