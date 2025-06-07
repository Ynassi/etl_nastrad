import pandas as pd
import numpy as np
import yfinance as yf
from tqdm import tqdm
from datetime import datetime

### --- 1. Enrichir df_final avec ratios manquants --- ###
def fetch_missing_ratios(tickers):
    data = []
    for ticker in tqdm(tickers, desc="🔢 Fetching ratios"):
        try:
            info = yf.Ticker(ticker).info
            ev = info.get("enterpriseValue")
            ebitda = info.get("ebitda")
            fcf = info.get("freeCashflow")
            mcap = info.get("marketCap")
            revenue = info.get("totalRevenue")

            data.append({
                "Ticker": ticker,
                "EV_EBITDA": ev / ebitda if ev and ebitda else np.nan,
                "FCF_Yield": fcf / mcap if fcf and mcap else np.nan,
                "Price_Sales": mcap / revenue if mcap and revenue else np.nan
            })
        except:
            continue
    return pd.DataFrame(data)

### --- 2. Ajouter Company et ExtractionDate aux tickers alternatifs --- ###
def enrich_company_and_date(df, ticker_col="Ticker"):
    tickers = df[ticker_col].dropna().unique().tolist()
    names = []
    for ticker in tqdm(tickers, desc="🏷️ Fetching company names"):
        try:
            info = yf.Ticker(ticker).info
            name = info.get("shortName", np.nan)
            names.append((ticker, name))
        except:
            names.append((ticker, np.nan))
    name_df = pd.DataFrame(names, columns=["Ticker", "Company"])
    name_df["ExtractionDate"] = datetime.today().strftime('%Y-%m-%d')
    df = df.merge(name_df, on="Ticker", how="left")
    return df

### --- 3. Merge final des deux jeux --- ###
def merge_final_and_enriched(
    path_final="data/df_final.csv",
    path_enriched="data/df_final_enriched.csv",
    output_path="data/df_final_merged.csv"
):
    df_final = pd.read_csv(path_final)
    df_enriched = pd.read_csv(path_enriched)

    df_final['Ticker'] = df_final['Ticker'].astype(str).str.upper().str.strip()
    df_enriched['Ticker'] = df_enriched['Ticker'].astype(str).str.upper().str.strip()

    # Étape 1 : enrichir df_final avec les ratios manquants
    enrichment_df = fetch_missing_ratios(df_final['Ticker'].unique().tolist())
    df_final = df_final.merge(enrichment_df, on="Ticker", how="left")

    # Étape 2 : enrichir df_enriched avec Company et ExtractionDate
    if "Company" not in df_enriched.columns or "ExtractionDate" not in df_enriched.columns:
        df_enriched = enrich_company_and_date(df_enriched)

    # Étape 3 : harmoniser les colonnes
    if "IndexSource" not in df_final.columns:
        df_final["IndexSource"] = "SP500_CAC_NIKKEI"
    all_columns = list(set(df_enriched.columns).union(df_final.columns))
    for col in all_columns:
        if col not in df_final.columns:
            df_final[col] = np.nan
        if col not in df_enriched.columns:
            df_enriched[col] = np.nan
    df_final = df_final[all_columns]
    df_enriched = df_enriched[all_columns]

    # Étape 4 : fusionner
    df_merged = pd.concat([df_final, df_enriched], ignore_index=True)
    df_merged.drop_duplicates(subset="Ticker", inplace=True)
    df_merged.reset_index(drop=True, inplace=True)

    df_merged.to_csv(output_path, index=False)
    print(f"✅ Fusion réussie : {len(df_merged)} tickers sauvegardés dans {output_path}")

if __name__ == "__main__":
    merge_final_and_enriched()
