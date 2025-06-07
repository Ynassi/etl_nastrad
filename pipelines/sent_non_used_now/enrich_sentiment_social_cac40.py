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

# 📁 Dossiers
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 📊 Chargement CAC40
df = pd.read_csv(os.path.join(DATA_DIR, "df_final_merged.csv"))
df_cac40 = df[df["IndexSource"] == "CAC40"][["Ticker", "Company"]].dropna()
tickers_info = df_cac40.to_dict(orient="records")

# 🔁 Mapping Ticker → ADR (ticker alternatif US)
ticker_mapping = {
    "OR.PA": "LRLCY", "SAN.PA": "SNY", "KER.PA": "PPRUY", "TTE.PA": "TTE", "AIR.PA": "EADSY",
    "BN.PA": "DANOY", "CS.PA": "AXAHY", "BNP.PA": "BNPQY", "GLE.PA": "SCGLY", "ORA.PA": "ORAN",
    "VIE.PA": "VEOEY", "ML.PA": "MGDDY", "RNO.PA": "RNLSY", "SAF.PA": "SAFRY", "CAP.PA": "CGEMY",
    "PUB.PA": "PUBGY", "DSY.PA": "DASTY", "ALO.PA": "ALSMY", "ENGI.PA": "ENGIY", "RI.PA": "PDRDY",
    "EL.PA": "ESLOY", "HO.PA": "THLLY", "MC.PA": "LVMUY", "RMS.PA": "HESAY", "CA.PA": "CRRFY",
    "ACA.PA": "CRARY", "LR.PA": "LGRDY", "SGO.PA": "CODYY", "SU.PA": "SBGSY", "DG.PA": "VCISY"
}

# 🔣 FinBERT
MODEL = "yiyanghkust/finbert-tone"
tokenizer = AutoTokenizer.from_pretrained(MODEL)
model = AutoModelForSequenceClassification.from_pretrained(MODEL)
model.eval()

def clean_name(name):
    name = name.lower()
    for stop in ["inc.", "inc", "corp.", "corp", "group", "sa", "ltd", "plc", ",", ".", "se"]:
        name = name.replace(stop, "")
    return name.strip()

def predict_sentiment(texts):
    inputs = tokenizer(texts, return_tensors="pt", padding=True, truncation=True, max_length=512)
    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.nn.functional.softmax(outputs.logits, dim=-1).numpy()
    preds = np.argmax(probs, axis=1)
    return preds, probs

# 🔍 Analyse Stocktwits avec fallback complet + Reddit
def scrape_stocktwits(ticker, company):
    headers = {"User-Agent": "Mozilla/5.0"}
    alt_ticker = ticker_mapping.get(ticker, None)

    queries = [ticker]
    if alt_ticker:
        queries.append(alt_ticker)
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

    # 🔁 Fallback ultime : recherche API nom
    try:
        search_q = clean_name(company)
        r = requests.get(
            f"https://api.stocktwits.com/api/2/search/symbols.json?q={search_q}",
            headers=headers,
            timeout=8
        )
        if r.status_code == 200:
            results = r.json().get("symbols", [])
            if results:
                alt_symbol = results[0]["symbol"]
                print(f"[Fallback API] {ticker} → Test avec '{alt_symbol}'")
                return scrape_stocktwits(alt_symbol, company)
    except Exception as e:
        print(f"[Fallback API erreur] {company} → {e}")

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

# 🚀 Main
def main():
    print("🔍 Analyse Stocktwits + Reddit + FinBERT – CAC40")
    results = []
    for entry in tqdm(tickers_info, desc="Stocktwits CAC40", ncols=100):
        result = scrape_stocktwits(entry["Ticker"], entry["Company"])
        results.append(result)
        time.sleep(random.uniform(1.2, 2.0))

    df = pd.DataFrame(results)
    df.to_csv(os.path.join(OUTPUT_DIR, "sentiment_social_cac40.csv"), index=False)
    print("✅ Export → sentiment_social_cac40.csv")

if __name__ == "__main__":
    main()
