import os
import time
import random
import requests
import pandas as pd
import urllib.parse
import warnings
from tqdm import tqdm
from transformers import pipeline

# 📁 Dossiers
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ⚠️ Suppression des warnings inutiles
warnings.filterwarnings("ignore", category=FutureWarning)

# 📊 Chargement des données
df_meta = pd.read_csv(os.path.join(DATA_DIR, "df_final_merged.csv"))

# 🔍 Fonction Google Suggest
def get_google_suggestions(query):
    url = f"https://suggestqueries.google.com/complete/search?client=firefox&q={urllib.parse.quote(query)}"
    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            return r.json()[1]
    except Exception:
        return []
    return []

# 📊 Score de popularité basé sur des mots-clés financiers
def score_suggestions(suggestions):
    if not suggestions:
        return 0, None
    keywords = ["forecast", "stock", "price", "split", "dividend", "buy", "invest", "growth"]
    score = sum(any(k in s.lower() for k in keywords) for s in suggestions)
    return score / len(suggestions), suggestions[0] if suggestions else None

# 🧠 Analyse de sentiment avec FinBERT
def analyze_sentiment_batch(suggestions):
    if not suggestions:
        return 0, 0, 0
    try:
        results = finbert(suggestions, truncation=True)
        counts = {"positive": 0, "negative": 0, "neutral": 0}
        for res in results:
            label = res["label"].lower()
            if label in counts:
                counts[label] += 1
        total = sum(counts.values())
        return (
            round(counts["positive"] / total, 3) if total else 0,
            round(counts["negative"] / total, 3) if total else 0,
            round(counts["neutral"] / total, 3) if total else 0
        )
    except:
        return 0, 0, 0

# 🚀 Script principal
if __name__ == "__main__":
    print("🔁 Chargement du modèle FinBERT...")
    finbert = pipeline("sentiment-analysis", model="ProsusAI/finbert", device=-1)

    results = []
    print(f"🔍 Lancement de l’analyse des suggestions sur {len(df_meta)} entreprises...")

    for _, row in tqdm(df_meta.iterrows(), total=len(df_meta), desc="Google Suggest + Sentiment", ncols=100):
        company_name = str(row["Company"]).strip()
        ticker = str(row["Ticker"]).strip()
        queries = [ticker, f"{ticker} stock", company_name, f"{company_name} stock"]
        all_suggestions = []

        for q in queries:
            suggestions = get_google_suggestions(q)
            all_suggestions.extend(suggestions)
            time.sleep(random.uniform(0.4, 0.7))  # éviter blocage

        all_suggestions = list(set(all_suggestions))
        trend_score, top_suggestion = score_suggestions(all_suggestions)
        pos, neg, neut = analyze_sentiment_batch(all_suggestions)

        results.append({
            "Ticker": ticker,
            "Company": company_name,
            "nb_suggestions": len(all_suggestions),
            "trend_suggestion_score": round(trend_score, 3),
            "positive_ratio": pos,
            "negative_ratio": neg,
            "neutral_ratio": neut,
            "first_suggestion": top_suggestion,
            "raw_suggestions": ", ".join(all_suggestions[:10])
        })

    # 📄 Export des résultats bruts
    df_out = pd.DataFrame(results)
    df_out.to_csv(os.path.join(OUTPUT_DIR, "sentiment_trends_suggestions.csv"), index=False)

    # 🔗 Fusion avec ta table d'origine
    df_merged = df_meta.merge(df_out, on=["Ticker", "Company"], how="left")
    df_merged.to_csv(os.path.join(OUTPUT_DIR, "df_final_merged_enriched.csv"), index=False)

    print("✅ Export CSV terminé :")
    print("→ 🔹 Suggestions seules : sentiment_trends_suggestions.csv")
    print("→ 🔸 Table enrichie     : df_final_merged_enriched.csv")
