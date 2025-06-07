import os
import json
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Charger les variables d’environnement
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
DATA_FOLDER = "data"

INDICES = {
    "spy": "SPY",
    "ewq": "EWQ",
    "ewj": "EWJ",
}

def fetch_from_alpha_vantage(symbol):
    print(f"→ Alpha Vantage : {symbol}")
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": ALPHA_VANTAGE_API_KEY,
    }
    response = requests.get(url, params=params)
    data = response.json()
    if "Time Series (Daily)" in data:
        return {
            "source": "alpha_vantage",
            "symbol": symbol,
            "raw_data": data
        }
    raise ValueError(data.get("Note") or data.get("Information") or "Réponse invalide Alpha Vantage")

def fetch_from_twelve_data(symbol):
    print(f"→ Twelve Data : {symbol}")
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "1day",
        "outputsize": 30,
        "apikey": TWELVE_DATA_API_KEY,
    }
    response = requests.get(url, params=params)
    data = response.json()
    if "values" in data:
        return {
            "source": "twelve_data",
            "symbol": symbol,
            "raw_data": data
        }
    raise ValueError(data.get("message") or "Réponse invalide Twelve Data")

def normalize_series(raw_data):
    if "Time Series (Daily)" in raw_data:
        time_series = raw_data["Time Series (Daily)"]
        dates = list(time_series.keys())[:30]
        dates.sort()
        return {
            "labels": dates,
            "data": [float(time_series[date]["4. close"]) for date in dates]
        }

    elif "values" in raw_data:
        values = raw_data["values"][:30][::-1]
        return {
            "labels": [v["datetime"] for v in values],
            "data": [float(v["close"]) for v in values]
        }

    return {"labels": [], "data": []}


def save_to_json(name, result):
    os.makedirs(DATA_FOLDER, exist_ok=True)
    filepath = os.path.join(DATA_FOLDER, f"{name}_data.json")

    normalized = normalize_series(result["raw_data"])

    with open(filepath, "w") as f:
        json.dump({
            "symbol": name,
            "source": result["source"],
            "updated_at": datetime.utcnow().isoformat(),
            "labels": normalized["labels"],
            "data": normalized["data"]
        }, f, indent=2)

    print(f"✅ {name.upper()} data saved from {result['source']} (normalized)")


def update_all_index_data():
    for name, symbol in INDICES.items():
        try:
            print(f"\n🔎 Traitement : {symbol}")
            try:
                result = fetch_from_alpha_vantage(symbol)
            except Exception as e_alpha:
                print(f"⚠️ Alpha Vantage failed: {e_alpha}")
                result = fetch_from_twelve_data(symbol)
            save_to_json(name, result)
        except Exception as e:
            print(f"❌ Erreur {symbol} : {e}")

if __name__ == "__main__":
    update_all_index_data()
