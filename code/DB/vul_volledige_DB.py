import subprocess
import sys

files = [
    "jpl_data_schema.sql",
    "voeg_teams_toe.py",
    "vul_wedstrijden_in_DB.py",
    "importeer_wedstrijd_weer.py",
    "importeer_weer_vooraf_48u.py"
]

for file in files:
    print(f"Start: {file}")

    if file.endswith(".sql"):
        result = subprocess.run(
            ["sqlcmd", "-S", "localhost", "-E", "-i", file]
        )
    else:
        result = subprocess.run([sys.executable, file])

    if result.returncode != 0:
        print(f"Fout bij uitvoeren van {file}")
        break

    print(f"Klaar: {file}")

print("Alle scripts uitgevoerd.")
