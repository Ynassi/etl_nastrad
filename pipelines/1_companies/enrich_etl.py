from finvizfinance.screener.overview import Overview
from yahooquery import Screener
import pandas as pd
from datetime import datetime
import ta
import yfinance as yf
import numpy as np
from tqdm import tqdm
from sklearn.preprocessing import MinMaxScaler
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

### --- 1. Récupération des tickers --- ###
def get_growth_candidates(limit=350):
    filters_dict = {
        'Market Cap.': 'Mid ($2bln to $10bln)',
        'Sales growthqtr over qtr': 'Over 10%',
        'Net Profit Margin': 'Over 10%',
        'Average Volume': 'Over 100K'
    }
    screener = Overview()
    screener.set_filter(filters_dict=filters_dict)
    df = screener.screener_view().head(limit)
    df['Source'] = 'Finviz-Growth'
    df['Date'] = datetime.today().strftime('%Y-%m-%d')
    return df[['Ticker', 'Source', 'Date']]

def get_value_candidates(limit=350):
    filters_dict = {
        'P/E': 'Under 20',
        'P/B': 'Under 2',
        'EPS growththis year': 'Positive (>0%)',
        'Average Volume': 'Over 100K'
    }
    screener = Overview()
    screener.set_filter(filters_dict=filters_dict)
    df = screener.screener_view().head(limit)
    df['Source'] = 'Finviz-Value'
    df['Date'] = datetime.today().strftime('%Y-%m-%d')
    return df[['Ticker', 'Source', 'Date']]

def get_yahoo_growth_candidates(limit=200):
    s = Screener()
    results = s.get_screeners('ms_technology', count=limit)
    quotes = results.get('ms_technology', {}).get('quotes', [])
    return pd.DataFrame([{
        "Ticker": q['symbol'],
        "Source": "Yahoo-Tech",
        "Date": datetime.today().strftime('%Y-%m-%d')
    } for q in quotes if 'symbol' in q])

def save_merged_candidates(df_list, final_limit=700):
    merged = pd.concat(df_list, ignore_index=True)
    merged.drop_duplicates(subset='Ticker', inplace=True)
    merged = merged.head(final_limit)
    output_path = os.path.join(DATA_DIR, "tickers_to_enrich.csv")
    merged.to_csv(output_path, index=False)
    print(f"✅ {len(merged)} tickers fusionnés sauvegardés dans {output_path}")

### --- 2. Enrichissement via yfinance --- ###
def enrich_tickers_with_yfinance():
    input_path = os.path.join(DATA_DIR, "tickers_to_enrich.csv")
    output_path = os.path.join(DATA_DIR, "df_final_enriched.csv")

    df = pd.read_csv(input_path)
    tickers = df["Ticker"].dropna().unique().tolist()
    enriched_rows = []

    for ticker in tqdm(tickers, desc="🔍 Enriching tickers"):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            hist = stock.history(period="6mo", interval="1d")
            if hist is None or len(hist) < 50:
                continue

            hist = hist.dropna()
            bb = ta.volatility.BollingerBands(hist['Close'])
            enriched_rows.append({
                "Ticker": ticker,
                "Sector": info.get("sector", "Other"),
                "Return_6M": hist['Close'].iloc[-1] / hist['Close'].iloc[0] - 1,
                "Volatility": hist['Close'].pct_change().std(),
                "Beta": info.get("beta"),
                "PE": info.get("trailingPE"),
                "PB": info.get("priceToBook"),
                "EV_Revenue": info.get("enterpriseToRevenue"),
                "GrossMargin": info.get("grossMargins"),
                "ProfitMargin": info.get("profitMargins"),
                "ROE": info.get("returnOnEquity"),
                "MarketCap": info.get("marketCap"),
                "EV_EBITDA": info.get("enterpriseValue") / info.get("ebitda") if info.get("enterpriseValue") and info.get("ebitda") else np.nan,
                "FCF_Yield": info.get("freeCashflow") / info.get("marketCap") if info.get("freeCashflow") and info.get("marketCap") else np.nan,
                "Interest_Coverage": info.get("ebit") / info.get("interestExpense") if info.get("ebit") and info.get("interestExpense") else np.nan,
                "Price_Sales": info.get("marketCap") / info.get("totalRevenue") if info.get("marketCap") and info.get("totalRevenue") else np.nan,
                "Net_Debt_Equity": (info.get("totalDebt") - info.get("totalCash")) / info.get("totalStockholderEquity") if info.get("totalDebt") and info.get("totalCash") and info.get("totalStockholderEquity") else np.nan,
                "RSI_14": ta.momentum.RSIIndicator(hist['Close']).rsi().iloc[-1],
                "SMA20_above_SMA50": int(ta.trend.SMAIndicator(hist['Close'], 20).sma_indicator().iloc[-1] > ta.trend.SMAIndicator(hist['Close'], 50).sma_indicator().iloc[-1]),
                "MACD": ta.trend.MACD(hist['Close']).macd().iloc[-1],
                "Momentum_10": ta.momentum.ROCIndicator(hist['Close'], window=10).roc().iloc[-1],
                "BB_Percent": (hist['Close'].iloc[-1] - bb.bollinger_lband().iloc[-1]) / (bb.bollinger_hband().iloc[-1] - bb.bollinger_lband().iloc[-1])
            })
        except:
            continue

    pd.DataFrame(enriched_rows).to_csv(output_path, index=False)
    print(f"✅ Enrichissement terminé : {len(enriched_rows)} tickers sauvegardés dans {output_path}")

### --- 3. Nettoyage final --- ###
def clean_enriched_data():
    input_path = os.path.join(DATA_DIR, "df_final_enriched.csv")
    output_path = os.path.join(DATA_DIR, "df_final_enriched.csv")

    df = pd.read_csv(input_path)
    df['Ticker'] = df['Ticker'].astype(str).str.upper().str.strip()
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df = df.loc[:, df.isna().mean() <= 0.5]

    for col in ['Beta', 'ROE', 'PE', 'PB', 'EV_Revenue', 'Return_6M']:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    sector_mapping = {
        "Communications": "Communication Services",
        "Retail": "Consumer Discretionary",
        "Foods": "Consumer Staples",
        "Gas": "Energy",
        "Insurance": "Financials",
        "Pharmaceuticals": "Health Care",
        "Construction": "Industrials",
        "Machinery": "Industrials",
        "Chemicals": "Materials",
        "Real estate": "Real Estate",
        "Electric power": "Utilities"
    }

    if "Sector" in df.columns:
        df["Sector"] = df["Sector"].astype(str).str.strip().replace(sector_mapping).fillna("Other")
    else:
        df["Sector"] = "Other"

    def safe_score(df, cols, inverse=False):
        valid = df[cols].replace([np.inf, -np.inf], np.nan).dropna()
        if valid.empty:
            return pd.Series(np.nan, index=df.index)
        scaled = MinMaxScaler().fit_transform(valid)
        score = pd.Series(scaled.mean(axis=1), index=valid.index)
        full_score = pd.Series(np.nan, index=df.index)
        full_score.loc[score.index] = score
        return 1 - full_score if inverse else full_score

    df['ValueScore'] = safe_score(df, ['PE', 'PB', 'EV_Revenue'], inverse=True)
    df['SignalScore'] = safe_score(df, ['RSI_14', 'MACD', 'Momentum_10'], inverse=True)
    quality_score = safe_score(df, ['ROE', 'GrossMargin', 'ProfitMargin'])
    df['QualityScore'] = quality_score * (1 - df['Beta'].clip(0, 2) / 4) if 'Beta' in df.columns else quality_score

    df['IndexSource'] = "AltScreen"
    df = df.drop_duplicates(subset='Ticker').reset_index(drop=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Nettoyage terminé : {len(df)} lignes sauvegardées dans {output_path}")

### --- 4. Lancement complet --- ###
if __name__ == "__main__":
    df_growth = get_growth_candidates()
    df_value = get_value_candidates()
    df_yahoo = get_yahoo_growth_candidates()
    save_merged_candidates([df_growth, df_value, df_yahoo])
    enrich_tickers_with_yfinance()
    clean_enriched_data()
