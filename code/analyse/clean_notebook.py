import json

# =============================
# CONFIGURATIE
# =============================
INPUT_NOTEBOOK = "jpl_analyse.ipynb"
OUTPUT_NOTEBOOK = "jpl_analyse_clean.ipynb"

MAX_TEXT_LINES = 25
MAX_TEXT_CHARS = 1500
REMOVE_HTML = True

IMAGE_KEYS = {
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/svg+xml"
}

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

        data = out.get("data", {})

        # ---- 1️⃣ HTML outputs ----
        if REMOVE_HTML and "text/html" in data:
            continue

        # ---- 2️⃣ AFBEELDINGEN STRIKT VERWIJDEREN ----
        if any(key in data for key in IMAGE_KEYS):
            continue

        # extra safety: soms zit image in metadata/empty display_data
        if out.get("output_type") == "display_data" and any(
            isinstance(v, str) and v.startswith("iVBOR") for v in data.values()
        ):
            continue

        # ---- 3️⃣ STREAM / TEXT ----
        if out.get("output_type") in ["stream", "execute_result", "display_data"]:

            text = ""

            if "text" in out:
                text = "".join(out["text"])
            elif "text/plain" in data:
                text = "".join(data["text/plain"])

            if text:
                lines = text.count("\n")
                chars = len(text)

                if lines > MAX_TEXT_LINES or chars > MAX_TEXT_CHARS:
                    continue

        clean_outputs.append(out)

    cell["outputs"] = clean_outputs
    cell["execution_count"] = cell.get("execution_count")

# =============================
# OPSLAAN
# =============================
with open(OUTPUT_NOTEBOOK, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=2, ensure_ascii=False)

print(f"✅ Afbeeldingen + grote outputs verwijderd:\n→ {OUTPUT_NOTEBOOK}")