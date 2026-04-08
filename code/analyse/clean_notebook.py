import json

# =============================
# CONFIGURATIE
# =============================
INPUT_NOTEBOOK = "jpl_analyse.ipynb"
OUTPUT_NOTEBOOK = "jpl_analyse_clean.ipynb"

MAX_TEXT_LINES = 25          # max toegelaten regels tekst-output
MAX_TEXT_CHARS = 1500        # extra veiligheid
REMOVE_HTML = True           # verwijder HTML-tabellen volledig

# =============================
# NOTEBOOK INLADEN
# =============================
with open(INPUT_NOTEBOOK, "r", encoding="utf-8") as f:
    nb = json.load(f)

# =============================
# OUTPUTS FILTEREN
# =============================
for cell in nb.get("cells", []):
    if cell.get("cell_type") != "code":
        continue

    clean_outputs = []

    for out in cell.get("outputs", []):

        # ---- 1️⃣ HTML outputs (grote tabellen) ----
        if REMOVE_HTML and out.get("data", {}).get("text/html"):
            continue

        # ---- 2️⃣ Stream / text output ----
        if out.get("output_type") in ["stream", "execute_result", "display_data"]:

            text = ""
            if "text" in out:
                text = "".join(out["text"])
            elif "data" in out and "text/plain" in out["data"]:
                text = "".join(out["data"]["text/plain"])

            # Controle op lengte
            if text:
                lines = text.count("\n")
                chars = len(text)

                if lines > MAX_TEXT_LINES or chars > MAX_TEXT_CHARS:
                    continue  # te lang → skippen

        # ✅ Output is acceptabel
        clean_outputs.append(out)

    cell["outputs"] = clean_outputs
    cell["execution_count"] = cell.get("execution_count")

# =============================
# OPSLAAN
# =============================
with open(OUTPUT_NOTEBOOK, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=2, ensure_ascii=False)

print(f"✅ Selectieve opschoning klaar:\n→ {OUTPUT_NOTEBOOK}")