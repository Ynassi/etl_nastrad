import os
import json
import re
import pandas as pd
from tqdm import tqdm

# ===  Répertoires de base ===
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DIR_JSON = os.path.join(BASE_DIR, "output", "insights_enriched_all")
SUMMARY_JSON_PATH = os.path.join(BASE_DIR, "data", "news_summaries_full.json")
SENTIMENT_CSV_PATH = os.path.join(BASE_DIR, "data", "sentiment_news_summary_full.csv")
FINAL_MERGED_CSV_PATH = os.path.join(BASE_DIR, "data", "df_final_merged.csv")


# === Chargement des fichiers source ===
with open(SUMMARY_JSON_PATH, "r") as f:
    summaries = json.load(f)

sentiment_df = pd.read_csv(
    SENTIMENT_CSV_PATH,
    sep=",",
    quotechar='"',
    escapechar="\\",
    encoding="utf-8",
    on_bad_lines="skip"
)

# === Fonction de parsing alternatif (format brut "titre (URL) → label")
def parse_headlines_from_string(source_str):
    headlines = []
    try:
        raw_items = source_str.split(" / ")
        for item in raw_items:
            parts = item.split(" → ")
            if len(parts) != 2:
                continue
            meta_part, label = parts
            label = label.strip().upper()

            match = re.match(r"^(.*?)\s*\((https?://[^\s)]+)\)$", meta_part.strip())
            if match:
                title = match.group(1).strip()
                url = match.group(2).strip()
                headlines.append({
                    "title": title,
                    "url": url,
                    "label": label
                })
    except Exception as e:
        print(f"⚠️ Parsing personnalisé échoué : {e}")
    return headlines

# === Charger la date d’extraction
try:
    df_merged = pd.read_csv(FINAL_MERGED_CSV_PATH)
    extraction_date = df_merged["ExtractionDate"].dropna().iloc[-1]
    print(f"✅ Date d'extraction détectée : {extraction_date}")
except Exception as e:
    raise RuntimeError(f"❌ Impossible de charger la date d'extraction : {e}")

# === Indexer sur colonne ticker
possible_ticker_cols = [col for col in sentiment_df.columns if 'ticker' in col.lower()]
if possible_ticker_cols:
    sentiment_df.set_index(possible_ticker_cols[0], inplace=True)
else:
    raise ValueError("⚠️ Colonne 'ticker' introuvable dans le CSV. Colonnes disponibles : " + str(sentiment_df.columns.tolist()))

# === Boucle sur les fichiers JSON ===
for filename in tqdm(os.listdir(DIR_JSON), desc="Fusion sentiment + date"):
    if not filename.endswith(".json"):
        continue

    ticker = filename.replace(".json", "")
    filepath = os.path.join(DIR_JSON, filename)

    # Charger le JSON existant
    with open(filepath, "r") as f:
        data = json.load(f)

    summary = summaries.get(ticker)
    if ticker not in sentiment_df.index or summary is None:
        continue

    row = sentiment_df.loc[ticker]

    # === Parsing des headlines
    headlines = []
    if pd.notna(row["source"]) and isinstance(row["source"], str):
        try:
            parsed = json.loads(row["source"])
            if isinstance(parsed, list):
                for item in parsed:
                    title = item.get("title", "").strip()
                    url = item.get("url", "").strip()
                    label = item.get("label", "").strip().upper()
                    if title and url and label:
                        headlines.append({
                            "title": title,
                            "url": url,
                            "label": label
                        })
            else:
                raise ValueError("Pas une liste JSON")
        except Exception:
            headlines = parse_headlines_from_string(row["source"])

    # === Injection dans le JSON final
    data["news_sentiment"] = {
        "summary": summary,
        "sentiment_score": round(float(row["sentiment_score"]), 3),
        "label": row["gpt_label"],  # 🔁 On lit "gpt_label" mais...
        "positive_ratio": round(float(row["positive_ratio"]), 2),
        "neutral_ratio": round(float(row["neutral_ratio"]), 2),
        "negative_ratio": round(float(row["negative_ratio"]), 2),
        "bullet_positive_count": int(row["bullet_positive_count"]),
        "bullet_negative_count": int(row["bullet_negative_count"]),
        "headlines": headlines
    }

    data["extraction_date"] = extraction_date

    # Sauvegarde du JSON enrichi
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

print("✅ Fusion terminée avec enrichissement GPT.")
