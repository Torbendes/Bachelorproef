import pyodbc

# Databaseverbinding (SQL Server)
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Jupiler_Pro_League_Matches;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

# Teamnamen met hun latitude en longitude (voorbeeldcoördinaten)
team_locations = {
    "Genk": (50.9653, 5.3820),
    "Kortrijk": (50.8202, 3.2640),
    "Cercle Brugge": (51.2093, 3.2247),
    "Standard": (50.6313, 5.5670),
    "St Truiden": (50.8100, 5.1623),
    "Mouscron": (50.7370, 3.2356),
    "Waregem": (50.8705, 3.3955),
    "Mechelen": (51.0250, 4.4772),
    "Waasland-Beveren": (51.2130, 4.2363),
    "Club Brugge": (51.2093, 3.2247),
    "Anderlecht": (50.8270, 4.3145),
    "Oostende": (51.2301, 2.9205),
    "Charleroi": (50.4114, 4.4446),
    "Gent": (51.0543, 3.7174),
    "Eupen": (50.6291, 6.0365),
    "Antwerp": (51.2194, 4.4025),
    "Oud-Heverlee Leuven": (50.8779, 4.7009),
    "Beerschot VA": (51.2090, 4.4040),
    "Seraing": (50.6014, 5.5708),
    "St. Gilloise": (50.8364, 4.3455),
    "Westerlo": (51.0783, 4.9692),
    "RWD Molenbeek": (50.8503, 4.3333),
    "Dender": (50.9478, 4.0531),
    "RAAL La Louviere": (50.4540, 4.1760)
}

for team, (lat, lon) in team_locations.items():
    cursor.execute(
        """
        UPDATE Teams
        SET latitude = ?, longitude = ?
        WHERE team = ?
        """,
        lat, lon, team
    )

conn.commit()
cursor.close()
conn.close()

print("Alle teams zijn bijgewerkt met latitude en longitude.")
