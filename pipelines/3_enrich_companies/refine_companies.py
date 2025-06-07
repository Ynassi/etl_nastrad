import os
import json
import yfinance as yf
import pandas as pd
from tqdm import tqdm

# 📁 Dossier contenant les JSON enrichis (par ticker)
DIR_JSON = "output/insights_enriched_all"

def enrich_visual_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="6mo", interval="1d", auto_adjust=False)

        if "Close" not in hist.columns or hist["Close"].dropna().empty:
            raise ValueError("Colonne 'Close' absente ou vide")

        hist = hist.dropna(subset=["Close"])
        hist.index = pd.to_datetime(hist.index, errors="coerce")
        hist = hist[hist.index.notnull()]

        sparkline = [
            {"date": idx.strftime("%Y-%m-%d"), "price": round(close, 2)}
            for idx, close in hist["Close"].items()
        ]

        info = stock.info
        price = info.get("regularMarketPrice")
        previous_close = info.get("previousClose")

        if price is None or previous_close is None:
            raise ValueError("Prix actuels manquants")

        change = round(price - previous_close, 2)
        percent_change = round((change / previous_close) * 100, 2)

        return {
            "sparkline": sparkline,
            "current_price_data": {
                "price": round(price, 2),
                "change": change,
                "percent_change": percent_change
            }
        }

    except Exception as e:
        print(f"⚠️ Erreur enrichissement {ticker} : {e}")
        return None


def main():
    for filename in tqdm(os.listdir(DIR_JSON), desc="Enrichissement visual_data (180j)"):
        if not filename.endswith(".json"):
            continue

        ticker = filename.replace(".json", "")
        filepath = os.path.join(DIR_JSON, filename)

        try:
            with open(filepath, "r") as f:
                data = json.load(f)

            visual_data = enrich_visual_data(ticker)
            if not visual_data:
                continue

            data["visual_data"] = visual_data

            with open(filepath, "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

        except Exception as e:
            print(f"❌ Erreur sur {ticker} → {e}")

if __name__ == "__main__":
    main()
