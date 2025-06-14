import os
import json
import time
import random
import pandas as pd
import yfinance as yf
from tqdm import tqdm

# 📁 Répertoires
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "insights_enriched_all")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 📄 Chargement complet
df = pd.read_csv(os.path.join(DATA_DIR, "df_final_merged.csv"))
tickers = df["Ticker"].dropna().unique()

# 🔧 Fonction d'enrichissement

def get_company_enriched_data(ticker, row):
    try:
        info = yf.Ticker(ticker).info

        def get_with_fallback(field, yfinance_key=None):
            value = row.get(field)
            if pd.isna(value) or value is None:
                return info.get(yfinance_key or field)
            return value

        return {
            "ticker": ticker,
            "name": get_with_fallback("Company", "longName") or ticker,
            "sector": get_with_fallback("Sector", "sector"),
            "market_cap": get_with_fallback("MarketCap", "marketCap"),
            "source_list": row.get("IndexSource"),
            "fundamentals": {
                "PE": get_with_fallback("PE", "trailingPE"),
                "PB": get_with_fallback("PB", "priceToBook"),
                "EV_Revenue": get_with_fallback("EV_Revenue", "enterpriseToRevenue"),
                "ROE": get_with_fallback("ROE", "returnOnEquity"),
                "ProfitMargin": get_with_fallback("ProfitMargin", "profitMargins"),
                "GrossMargin": get_with_fallback("GrossMargin", "grossMargins"),
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
            "beta": get_with_fallback("Beta", "beta"),
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

# 🚀 Génération des JSON
errors = []
for _, row in tqdm(df.iterrows(), total=len(df), desc="Enrichissement global"):
    ticker = row["Ticker"]
    data = get_company_enriched_data(ticker, row)

    if data:
        output_path = os.path.join(OUTPUT_DIR, f"{ticker}.json")
        try:
            with open(output_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"❌ Erreur écriture {ticker} → {e}")
            errors.append(ticker)
    else:
        errors.append(ticker)

    # 💤 Anti-blocage API
    time.sleep(random.uniform(0.6, 1.1))

# 📊 Résumé
print(f"\n✅ {len(df) - len(errors)} fichiers générés.")
if errors:
    print(f"❌ {len(errors)} erreurs (exemples : {errors[:5]})")

with open("errors_to_retry.json", "w") as f:
    json.dump(errors, f)
