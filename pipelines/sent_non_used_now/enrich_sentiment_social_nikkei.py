import os
import time
import random
import pandas as pd
import requests
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import numpy as np

# 🔄 Fallback Reddit
def search_reddit_messages(query, limit=20):
    headers = {'User-Agent': 'Mozilla/5.0'}
    url = f"https://www.reddit.com/search.json?q={query}&limit={limit}&sort=new"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        results = response.json()["data"]["children"]
        return [post["data"]["title"] + " " + post["data"].get("selftext", "") for post in results]
    except Exception as e:
        print(f"[Reddit fallback error] {e}")
        return []

# 📁 Répertoires
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 📊 Chargement des entreprises du Nikkei 225
df = pd.read_csv(os.path.join(DATA_DIR, "df_final_merged.csv"))
df_jp = df[df["IndexSource"] == "Nikkei225"][["Ticker", "Company"]].dropna()
tickers_info = df_jp.to_dict(orient="records")

# 🔁 Mapping Ticker → ADR
adr_mapping = {
    "7203.T": "TM", "6758.T": "SONY", "7267.T": "HMC", "8306.T": "MUFG", "8411.T": "MFG",
    "8604.T": "NMR", "8316.T": "SMFG", "8591.T": "IX", "4502.T": "TAK", "9984.T": "SFTBY",
    "6971.T": "KYOCY", "7751.T": "CAJ", "8035.T": "TOELY", "6501.T": "HTHIY", "7752.T": "RICOY",
    "6752.T": "PCRFY", "6753.T": "SHCAY", "9983.T": "FRCOY", "8058.T": "MSBHY", "8053.T": "SSUMY",
    "8031.T": "MITSY", "8001.T": "ITOCY", "8002.T": "MARUY", "4503.T": "ASPHF", "4568.T": "DSNKY",
    "7733.T": "OCPNY", "6954.T": "FANUY", "6301.T": "KMTUY", "7201.T": "NSANY", "4901.T": "FUJIY"
}

# 🔁 Mapping Ticker → Symboles connus sur Stocktwits (personnalisé si différent des ADR)
stocktwits_mapping = {
    "9984.T": "SFTBF",  # Softbank
    "6502.T": "TOSYY",  # Toshiba
    "7202.T": "ISUZY",  # Isuzu
    "7269.T": "SZKMY",  # Suzuki
    "7211.T": "MZDAY",  # Mazda
}

# 🔣 FinBERT
MODEL = "yiyanghkust/finbert-tone"
tokenizer = AutoTokenizer.from_pretrained(MODEL)
model = AutoModelForSequenceClassification.from_pretrained(MODEL)
model.eval()

def clean_name(name):
    name = name.lower()
    for stop in ["inc.", "inc", "corp.", "corp", "group", "ltd", "co.", "plc", ",", ".", "holdings", "kabushiki"]:
        name = name.replace(stop, "")
    return name.strip()

def predict_sentiment(texts):
    inputs = tokenizer(texts, return_tensors="pt", padding=True, truncation=True, max_length=512)
    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.nn.functional.softmax(outputs.logits, dim=-1).numpy()
    preds = np.argmax(probs, axis=1)
    return preds, probs

def scrape_stocktwits(ticker, company):
    headers = {"User-Agent": "Mozilla/5.0"}

    queries = [ticker]

    adr = adr_mapping.get(ticker)
    if adr:
        queries.append(adr)

    alt = stocktwits_mapping.get(ticker)
    if alt:
        queries.append(alt)

    queries += [ticker.replace(".", ""), clean_name(company), clean_name(company).split(" ")[0]]

    tried = set()

    for query in queries:
        if not isinstance(query, str) or query in tried:
            continue
        tried.add(query)

        url = f"https://api.stocktwits.com/api/2/streams/symbol/{query}.json"
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code != 200:
                continue

            data = r.json()
            messages = data.get("messages", [])
            if not messages:
                continue

            texts = [m["body"] for m in messages if "body" in m]
            ids = [m["id"] for m in messages if "id" in m]
            if not texts:
                continue

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
                "Ticker_used": query,
                "Company": company,
                "message_count": total,
                "positive_ratio": round(positives / total, 2),
                "neutral_ratio": round(neutrals / total, 2),
                "negative_ratio": round(negatives / total, 2),
                "Source": " // ".join(source_entries)
            }

        except Exception:
            continue

    # 🔄 Fallback Reddit
    alt_messages = search_reddit_messages(company)
    if alt_messages:
        preds, probs = predict_sentiment(alt_messages)
        total = len(preds)
        positives = sum(1 for p in preds if p == 2)
        neutrals = sum(1 for p in preds if p == 1)
        negatives = sum(1 for p in preds if p == 0)

        return {
            "Ticker": ticker,
            "Ticker_used": "Reddit",
            "Company": company,
            "message_count": total,
            "positive_ratio": round(positives / total, 2),
            "neutral_ratio": round(neutrals / total, 2),
            "negative_ratio": round(negatives / total, 2),
            "Source": "Reddit"
        }

    print(f"[Aucun résultat] {ticker} – {company}")
    return {
        "Ticker": ticker,
        "Ticker_used": None,
        "Company": company,
        "message_count": 0,
        "positive_ratio": None,
        "neutral_ratio": None,
        "negative_ratio": None,
        "Source": None
    }

def main():
    print("🔍 Analyse Stocktwits + Reddit + FinBERT – Nikkei 225")
    results = []
    for entry in tqdm(tickers_info, desc="Stocktwits Nikkei225", ncols=100):
        result = scrape_stocktwits(entry["Ticker"], entry["Company"])
        results.append(result)
        time.sleep(random.uniform(1.1, 1.8))

    df = pd.DataFrame(results)
    df.to_csv(os.path.join(OUTPUT_DIR, "sentiment_social_nikkei_all.csv"), index=False)
    print("✅ Export → sentiment_social_nikkei_all.csv")

if __name__ == "__main__":
    main()
