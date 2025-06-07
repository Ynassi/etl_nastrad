import os
import time
import random
import pandas as pd
import requests
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import numpy as np

# 📁 Répertoires
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ✅ Chargement de la table complète et filtrage des entreprises US (SP500 + AltScreen)
df_all = pd.read_csv(os.path.join(DATA_DIR, "df_final_merged.csv"))
df_us = df_all[df_all["IndexSource"].isin(["SP500", "AltScreen"])]
df_us = df_us[["Ticker", "Company"]].dropna().drop_duplicates()
tickers_info = df_us.to_dict(orient="records")

# 🔣 FinBERT setup
MODEL_NAME = "yiyanghkust/finbert-tone"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
model.eval()

# 🧠 Prédiction du sentiment
def predict_sentiment(texts):
    inputs = tokenizer(texts, return_tensors="pt", padding=True, truncation=True, max_length=512)
    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.nn.functional.softmax(outputs.logits, dim=-1).numpy()
    preds = np.argmax(probs, axis=1)
    return preds, probs

# 🔍 Scraper Stocktwits + FinBERT (sans fallback)
def scrape_stocktwits(ticker, company):
    headers = {"User-Agent": "Mozilla/5.0"}
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return {
                "Ticker": ticker,
                "Company": company,
                "message_count": 0,
                "positive_ratio": None,
                "neutral_ratio": None,
                "negative_ratio": None,
                "Source": None
            }

        data = response.json()
        messages = data.get("messages", [])
        if not messages:
            return {
                "Ticker": ticker,
                "Company": company,
                "message_count": 0,
                "positive_ratio": None,
                "neutral_ratio": None,
                "negative_ratio": None,
                "Source": None
            }

        texts = [msg["body"] for msg in messages if "body" in msg]
        ids = [msg["id"] for msg in messages if "id" in msg]

        preds, probs = predict_sentiment(texts)

        total = len(preds)
        positives = sum(1 for p in preds if p == 2)
        neutrals = sum(1 for p in preds if p == 1)
        negatives = sum(1 for p in preds if p == 0)

        source_entries = [
            f"https://stocktwits.com/message/{msg_id} : {round(float(max(prob)), 3)}"
            for msg_id, prob in zip(ids, probs)
        ]

        return {
            "Ticker": ticker,
            "Company": company,
            "message_count": total,
            "positive_ratio": round(positives / total, 2),
            "neutral_ratio": round(neutrals / total, 2),
            "negative_ratio": round(negatives / total, 2),
            "Source": " // ".join(source_entries)
        }

    except Exception as e:
        print(f"[Erreur] {ticker} – {e}")
        return {
            "Ticker": ticker,
            "Company": company,
            "message_count": 0,
            "positive_ratio": None,
            "neutral_ratio": None,
            "negative_ratio": None,
            "Source": None
        }

# 🚀 Exécution principale
def main():
    print(f"🔍 Analyse Stocktwits US – {len(tickers_info)} tickers")
    results = []

    for entry in tqdm(tickers_info, desc="Stocktwits US + FinBERT", ncols=100):
        ticker = entry["Ticker"]
        company = entry["Company"]
        result = scrape_stocktwits(ticker, company)
        results.append(result)
        time.sleep(random.uniform(1.2, 2.0))

    df = pd.DataFrame(results)
    df.to_csv(os.path.join(OUTPUT_DIR, "sentiment_social_us_full.csv"), index=False)
    print("✅ Export terminé → output/sentiment_social_us_full.csv")

if __name__ == "__main__":
    main()
