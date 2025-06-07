import json
import math
import os

# 📁 Dossier contenant les JSON (structure correcte)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIR_JSON = os.path.join(BASE_DIR, "output", "insights_enriched_all")

def clean_json(obj):
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    elif isinstance(obj, dict):
        return {k: clean_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_json(i) for i in obj]
    else:
        return obj

def clean_and_save_json_file(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        cleaned = clean_json(data)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cleaned, f, indent=2, ensure_ascii=False)
        print(f"[✓] Nettoyé : {os.path.basename(path)}")
    except Exception as e:
        print(f"[✗] Erreur pour {os.path.basename(path)} → {e}")

if __name__ == "__main__":
    for file in os.listdir(DIR_JSON):
        if file.endswith(".json"):
            full_path = os.path.join(DIR_JSON, file)
            clean_and_save_json_file(full_path)
