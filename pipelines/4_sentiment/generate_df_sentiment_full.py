import os
import json
import pandas as pd
from tqdm import tqdm

# === 📁 Répertoires ===
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DIR_JSON = os.path.join(BASE_DIR, "output", "insights_enriched_all")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "df_sentiment_full.csv")

# === 🧠 Fonction pour cap type ===
def cap_type(source_list):
    return "BigCap" if source_list in ["SP500", "CAC40", "Nikkei225"] else "SmallCap"

# === 📄 Lecture JSON et extraction des données ===
records = []

for filename in tqdm(os.listdir(DIR_JSON), desc="📄 Lecture des JSON enrichis"):
    if not filename.endswith(".json"):
        continue

    filepath = os.path.join(DIR_JSON, filename)

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        fundamentals = data.get("fundamentals", {})
        tech = data.get("technical_indicators", {})
        analyst = data.get("analyst_rating", {})
        sentiment = data.get("news_sentiment", {})
        current_price_data = data.get("visual_data", {}).get("current_price_data", {})

        records.append({
            "ticker": data.get("ticker"),
            "name": data.get("name"),
            "sector": data.get("sector"),
            "market_cap": data.get("market_cap"),
            "CapType": cap_type(data.get("source_list", "")),
            "PE": fundamentals.get("PE"),
            "PB": fundamentals.get("PB"),
            "ROE": fundamentals.get("ROE"),
            "ROA": fundamentals.get("ROA"),
            "ProfitMargin": fundamentals.get("ProfitMargin"),
            "GrossMargin": fundamentals.get("GrossMargin"),
            "FCF_Margin": fundamentals.get("FCF_Margin"),
            "DividendYield": fundamentals.get("DividendYield"),
            "DebtEquity": fundamentals.get("DebtEquity"),
            "CurrentRatio": fundamentals.get("CurrentRatio"),
            "QuickRatio": fundamentals.get("QuickRatio"),
            "PriceToFCF": fundamentals.get("PriceToFCF"),
            "EV_Revenue": fundamentals.get("EV_Revenue"),
            "EV_EBITDA": fundamentals.get("EV_EBITDA"),
            "Beta": data.get("beta"),
            "Volatility": data.get("volatility"),
            "return_6m": data.get("return_6m"),
            "RSI_14": tech.get("RSI_14"),
            "Momentum_10": tech.get("Momentum_10"),
            "MACD": tech.get("MACD"),
            "BB_Percent": tech.get("BB_Percent"),
            "SMA20_above_SMA50": tech.get("SMA20_above_SMA50"),
            "sentiment_score": sentiment.get("sentiment_score"),
            "sentiment_label": sentiment.get("label"),
            "positive_ratio": sentiment.get("positive_ratio"),
            "neutral_ratio": sentiment.get("neutral_ratio"),
            "negative_ratio": sentiment.get("negative_ratio"),
            "bullet_positive_count": sentiment.get("bullet_positive_count"),
            "bullet_negative_count": sentiment.get("bullet_negative_count"),
            "recommendation": analyst.get("recommendation"),
            "analyst_count": analyst.get("analyst_count"),
            "target_mean_price": analyst.get("target_mean_price"),
            "current_price": current_price_data.get("price"),
            "percent_change": current_price_data.get("percent_change"),
            "extraction_date": data.get("extraction_date")
        })

    except Exception as e:
        print(f"❌ Erreur avec {filename} : {e}")

# === 📊 Création et sauvegarde du DataFrame
df = pd.DataFrame(records)
os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)
df.to_csv(OUTPUT_PATH, index=False)
print(f"\n✅ DataFrame enregistré : {OUTPUT_PATH} (shape: {df.shape})")
