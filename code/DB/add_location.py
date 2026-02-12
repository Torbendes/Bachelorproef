import pyodbc

# Databaseverbinding (SQL Server)
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Jupiler_Pro_League_Matches;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

# Voeg capacity kolom toe als die nog niet bestaat
try:
    cursor.execute("ALTER TABLE Teams ADD capacity INT;")
except:
    pass  # negeer fout als kolom al bestaat

# Teamnamen met hun latitude, longitude en capaciteit
team_info = {
    "Genk": (50.9653, 5.3820, 23718),
    "Kortrijk": (50.8202, 3.2640, 9399),
    "Cercle Brugge": (51.2093, 3.2247, 29062),
    "Standard": (50.6313, 5.5670, 27670),
    "St Truiden": (50.8100, 5.1623, 14600),
    "Mouscron": (50.7370, 3.2356, 12800),
    "Waregem": (50.8705, 3.3955, 12414),
    "Mechelen": (51.0250, 4.4772, 16672),
    "Waasland-Beveren": (51.2130, 4.2363, 8190),
    "Club Brugge": (51.2093, 3.2247, 29062),
    "Anderlecht": (50.8270, 4.3145, 22500),
    "Oostende": (51.2301, 2.9205, 8432),
    "Charleroi": (50.4114, 4.4446, 15000),
    "Gent": (51.0543, 3.7174, 20175),
    "Eupen": (50.6291, 6.0365, 8363),
    "Antwerp": (51.2194, 4.4025, 21000),
    "Oud-Heverlee Leuven": (50.8779, 4.7009, 10020),
    "Beerschot VA": (51.2090, 4.4040, 12500),
    "Seraing": (50.6014, 5.5708, 8207),
    "St. Gilloise": (50.8364, 4.3455, 9400),
    "Westerlo": (51.0783, 4.9692, 8035),
    "RWD Molenbeek": (50.8503, 4.3333, 12266),
    "Dender": (50.9478, 4.0531, 6429),
    "RAAL La Louviere": (50.4540, 4.1760, 8050)
}

for team, (lat, lon, capacity) in team_info.items():
    cursor.execute(
        """
        UPDATE Teams
        SET latitude = ?, longitude = ?, capacity = ?
        WHERE team = ?
        """,
        lat, lon, capacity, team
    )

conn.commit()
cursor.close()
conn.close()

print("Alle teams zijn bijgewerkt met latitude, longitude en capaciteit.")
