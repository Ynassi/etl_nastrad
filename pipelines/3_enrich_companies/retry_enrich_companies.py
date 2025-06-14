import os
import json
import time
import random
import pandas as pd
import yfinance as yf
from tqdm import tqdm

#  Répertoires (toujours depuis la racine du projet)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "insights_enriched_all")
ERROR_FILE = os.path.join(BASE_DIR, "errors_to_retry.json")

os.makedirs(OUTPUT_DIR, exist_ok=True)

#  Charger les tickers en erreur
try:
    with open(ERROR_FILE, "r") as f:
        tickers_failed = json.load(f)
    print(f" {len(tickers_failed)} tickers à réessayer trouvés dans errors_to_retry.json")
except Exception as e:
    print(f"❌ Impossible de lire {ERROR_FILE} → {e}")
    tickers_failed = []

#  Chargement des données sources
df = pd.read_csv(os.path.join(DATA_DIR, "df_final_merged.csv"))
df = df[df["Ticker"].isin(tickers_failed)]

#  Fonction d'enrichissement
def get_company_enriched_data(ticker, row):
    try:
        ticker_obj = yf.Ticker(ticker)
        try:
            info = ticker_obj.info
        except Exception as e:
            print(f"[WARNING] {ticker}: info inaccessible → {e}")
            info = {}

        return {
            "ticker": ticker,
            "name": row.get("Company") or info.get("longName", ticker),
            "sector": row.get("Sector") or info.get("sector"),
            "market_cap": row.get("MarketCap"),
            "source_list": row.get("IndexSource"),
            "fundamentals": {
                "PE": row.get("PE"),
                "PB": row.get("PB"),
                "EV_Revenue": row.get("EV_Revenue"),
                "ROE": row.get("ROE"),
                "ProfitMargin": row.get("ProfitMargin"),
                "GrossMargin": row.get("GrossMargin"),
                "DividendYield": info.get("dividendYield"),
                "DebtEquity": info.get("debtToEquity"),
                "CurrentRatio": info.get("currentRatio"),
                "QuickRatio": info.get("quickRatio"),
                "FreeCashFlow": info.get("freeCashflow"),
                "OperatingCashFlow": info.get("operatingCashflow"),
                "FCF_Margin": (
                    info.get("freeCashflow") / info.get("totalRevenue")
                    if info.get("freeCashflow") and info.get("totalRevenue") else None
                ),
                "ROA": info.get("returnOnAssets"),
                "PEG": info.get("pegRatio"),
                "EV_EBITDA": info.get("enterpriseToEbitda"),
                "BuybackYield": info.get("buybackYield"),
                "PriceToFCF": (
                    info.get("marketCap") / info.get("freeCashflow")
                    if info.get("marketCap") and info.get("freeCashflow") else None
                )
            },
            "technical_indicators": {
                "RSI_14": row.get("RSI_14"),
                "Momentum_10": row.get("Momentum_10"),
                "MACD": row.get("MACD"),
                "BB_Percent": row.get("BB_Percent"),
                "SMA20_above_SMA50": row.get("SMA20_above_SMA50")
            },
            "scores": {
                "ValueScore": row.get("ValueScore"),
                "QualityScore": row.get("QualityScore"),
                "SignalScore": row.get("SignalScore")
            },
            "volatility": row.get("Volatility"),
            "beta": row.get("Beta") or info.get("beta"),
            "return_6m": row.get("Return_6M"),
            "analyst_rating": {
                "recommendation": info.get("recommendationKey"),
                "analyst_count": info.get("numberOfAnalystOpinions"),
                "target_mean_price": info.get("targetMeanPrice")
            }
        }

    except Exception as e:
        print(f"[ERROR enrich] {ticker} → {e}")
        return None

# Retry enrichissement
errors_still_failing = []
for _, row in tqdm(df.iterrows(), total=len(df), desc="🔁 Retry enrich"):
    ticker = row["Ticker"]
    output_path = os.path.join(OUTPUT_DIR, f"{ticker}.json")

    if os.path.exists(output_path):
        print(f" {ticker} déjà enrichi → skip")
        continue

    data = get_company_enriched_data(ticker, row)

    if data:
        try:
            with open(output_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Erreur écriture {ticker} → {e}")
            errors_still_failing.append(ticker)
    else:
        errors_still_failing.append(ticker)

    time.sleep(random.uniform(0.6, 1.1))

#  Résumé
print(f"\n {len(df) - len(errors_still_failing)} tickers enrichis avec succès.")
if errors_still_failing:
    print(f" {len(errors_still_failing)} erreurs persistantes : {errors_still_failing}")
    with open(ERROR_FILE, "w") as f:
        json.dump(errors_still_failing, f)
else:
    print(" Tous les tickers ont été enrichis avec succès.")
    if os.path.exists(ERROR_FILE):
        os.remove(ERROR_FILE)
