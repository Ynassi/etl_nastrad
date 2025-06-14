import os
import json
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dotenv import load_dotenv

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OVERVIEW_FOLDER = os.path.join(BASE_DIR, "data", "overview")
os.makedirs(OVERVIEW_FOLDER, exist_ok=True)
load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"))


# 🔹 1. Fear & Greed Index
def get_fear_greed():
    try:
        url = "https://api.alternative.me/fng/?limit=1&format=json"
        response = requests.get(url, timeout=10)
        data = response.json()
        entry = data.get("data", [{}])[0]
        score = int(entry.get("value", 0)) if entry.get("value") else None
        label = entry.get("value_classification", "N/A")
        return {"score": score, "label": label}
    except Exception as e:
        return {"score": None, "label": "Erreur", "error": str(e)}

# 🔹 2. VIX
def get_vix():
    try:
        vix = yf.Ticker("^VIX")
        hist = vix.history(period="1mo")
        latest = hist["Close"].iloc[-1]
        return {"value": round(latest, 2)}
    except Exception as e:
        return {"value": None, "error": str(e)}

# 🔹 3. Indices comparés
def get_index_comparison():
    tickers = {
        "S&P500": "^GSPC",
        "CAC40": "^FCHI",
        "Nikkei225": "^N225"
    }
    df_final = pd.DataFrame()
    for name, ticker in tickers.items():
        data = yf.download(ticker, period="1mo", interval="1d", progress=False)[["Close"]].reset_index()
        data.columns = ["Date", name]
        data["Date"] = pd.to_datetime(data["Date"]).dt.date
        if df_final.empty:
            df_final = data
        else:
            df_final = pd.merge(df_final, data, on="Date", how="outer")
    df_final = df_final.sort_values("Date").dropna(how="all", subset=["S&P500", "CAC40", "Nikkei225"])
    return df_final

# 🔹 4 & 5. Données sectorielles
def get_sector_data_fmp():
    sector_etfs = {
        "Technology": "XLK", "Healthcare": "XLV", "Financial Services": "XLF",
        "Consumer Cyclical": "XLY", "Consumer Defensive": "XLP", "Communication Services": "XLC",
        "Energy": "XLE", "Industrials": "XLI", "Real Estate": "XLRE",
        "Materials": "XLB", "Utilities": "XLU"
    }
    SECTOR_NAME_MAP = {
        "Technology": "Information Technology", "Healthcare": "Health Care",
        "Financial Services": "Financials", "Consumer Cyclical": "Consumer Discretionary",
        "Consumer Defensive": "Consumer Staples", "Communication Services": "Communication Services",
        "Energy": "Energy", "Industrials": "Industrials", "Real Estate": "Real Estate",
        "Materials": "Materials", "Utilities": "Utilities"
    }
    end_date = datetime.today()
    start_date = end_date - timedelta(days=365)
    heatmap, performance, volatility = {}, {}, {}

    data = yf.download(
        list(sector_etfs.values()), start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"), interval="1d", group_by="ticker",
        auto_adjust=True, progress=False
    )

    for sector, ticker in sector_etfs.items():
        try:
            df = data[ticker]["Close"].dropna()
            if len(df) < 30:
                raise ValueError("Pas assez de données")
            last_price = df.iloc[-1]
            prev_day = df.iloc[-2]
            prev_week = df.iloc[-6] if len(df) >= 7 else None
            prev_month = df.iloc[-21] if len(df) >= 22 else None
            start_of_year = df[df.index >= datetime(end_date.year, 1, 2)]
            first_ytd_price = start_of_year.iloc[0] if not start_of_year.empty else None

            def pct(current, previous): return round(((current - previous) / previous) * 100, 5) if previous else None
            def abs_diff(current, previous): return round(current - previous, 2) if previous else None

            sector_name = SECTOR_NAME_MAP[sector]
            heatmap[sector_name] = pct(last_price, prev_day)
            performance[sector_name] = {
                "1d": pct(last_price, prev_day), "1w": pct(last_price, prev_week),
                "1m": pct(last_price, prev_month), "YTD": pct(last_price, first_ytd_price),
                "abs_1d": abs_diff(last_price, prev_day), "abs_1w": abs_diff(last_price, prev_week),
                "abs_1m": abs_diff(last_price, prev_month), "abs_YTD": abs_diff(last_price, first_ytd_price)
            }
            returns = df.pct_change().dropna()[-30:]
            volatility[sector_name] = round(returns.std() * 100, 3)

        except Exception as e:
            sector_name = SECTOR_NAME_MAP[sector]
            print(f"Erreur secteur {sector_name} : {e}")
            heatmap[sector_name] = None
            performance[sector_name] = {k: None for k in ["1d", "1w", "1m", "YTD", "abs_1d", "abs_1w", "abs_1m", "abs_YTD"]}
            volatility[sector_name] = None

    return heatmap, performance, volatility

# 🔹 Résumé narratif
def generate_headline_summary(sector_perf):
    try:
        best_sector = max(sector_perf.items(), key=lambda x: x[1]["1d"] or -9999)
        worst_sector = min(sector_perf.items(), key=lambda x: x[1]["1d"] or 9999)
        best_name, best_val = best_sector[0], best_sector[1]["1d"]
        worst_name, worst_val = worst_sector[0], worst_sector[1]["1d"]
        if best_val is None or worst_val is None:
            return "Résumé indisponible (valeurs manquantes)."
        return (
            f"Le secteur {best_name} affiche la meilleure performance journalière (+{best_val}%). "
            f"À l'inverse, {worst_name} recule légèrement ({worst_val}%)."
        )
    except:
        return "Résumé non généré (erreur)."

# 🔹 News via Finnhub
def get_news():
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        return [{"title": "Clé API Finnhub manquante"}]
    try:
        return requests.get(
            f"https://finnhub.io/api/v1/news?category=general&token={api_key}",
            timeout=10
        ).json()[:10]
    except Exception as e:
        return [{"title": "Erreur Finnhub", "error": str(e)}]

# 🔹 Sparklines
def get_sparklines():
    tickers = {"S&P500": "^GSPC", "CAC40": "^FCHI", "Nikkei225": "^N225"}
    result = {}
    for name, ticker in tickers.items():
        try:
            df = yf.download(ticker, period="5d", interval="1h", progress=False)
            if "Close" in df.columns:
                result[name] = df["Close"].dropna().astype(float).tolist()
            else:
                result[name] = {"error": "Colonne 'Close' absente"}
        except Exception as e:
            result[name] = {"error": str(e)}
    return result

# 🔧 Fonctions de sauvegarde
def save_json(data, name):
    with open(os.path.join(OVERVIEW_FOLDER, f"{name}.json"), "w") as f:
        json.dump(data, f, indent=2)

def save_csv(df, name):
    df.to_csv(os.path.join(OVERVIEW_FOLDER, f"{name}.csv"), index=False)

# 🔹 MAIN
if __name__ == "__main__":
    print("📊 Génération des données overview en cours...")
    save_json(get_fear_greed(), "fear_greed")
    save_json(get_vix(), "vix")
    save_csv(get_index_comparison(), "indices")
    heatmap, performance, volatility = get_sector_data_fmp()
    save_json(heatmap, "sector_heatmap")
    save_json(performance, "sector_performance")
    save_json(volatility, "sector_volatility")
    save_json(get_news(), "news")
    save_json(get_sparklines(), "index_sparklines")
    save_json({"headline_summary": generate_headline_summary(performance)}, "headline_summary")
    save_json({"generated_at": datetime.now().isoformat()}, "generated_at")
    print("✅ Tous les fichiers overview ont été générés dans /data/overview/")
