import pandas as pd
import yfinance as yf
from time import sleep
import ta
import os
from bs4 import BeautifulSoup
from sklearn.preprocessing import MinMaxScaler
import os
import sqlite3
import requests
from datetime import datetime
import sys
sys.stdout.reconfigure(line_buffering=True)

def run_pipeline_etl():

    # -------------------------------
    # 1) IMPORT S&P500
    # -------------------------------
    print("[ETL] Étape 1/14 – Import de la liste S&P500 (Wikipedia)")
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        df_sp500 = tables[0][['Symbol', 'Security', 'GICS Sector']]
        df_sp500.columns = ['Ticker', 'Company', 'Sector']
        df_sp500['Ticker'] = df_sp500['Ticker'].str.replace('.', '-', regex=False)
        df_sp500['Prefix4'] = df_sp500['Company'].str[:17].str.upper()
        print(f"{len(df_sp500)} tickers extraits depuis Wikipedia")
    except Exception as e:
        print("Erreur lors du chargement de la table Wikipedia :", e)
        return

    # ----------------------------------------
    # 2) Extraction des données fondamentales.
    # ----------------------------------------
    print("[ETL] Étape 2/14 – Préparation initiale du DataFrame (renommage, préfixes) S&P500 ")

    def get_features(ticker):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="6mo", interval="1d")
            if hist.shape[0] < 50:
                return None, "Pas assez d'historique"

            returns = hist['Close'].pct_change().dropna()
            return_6m = hist['Close'].iloc[-1] / hist['Close'].iloc[0] - 1
            volatility = returns.std()

            info = stock.info
            if not info or 'trailingPE' not in info:
                return None, "Fondamentaux clés manquants"

            row = {
                'Ticker': ticker,
                'Return_6M': return_6m,
                'Volatility': volatility,
                'Beta': info.get('beta'),
                'PE': info.get('trailingPE'),
                'PB': info.get('priceToBook'),
                'EV_Revenue': info.get('enterpriseToRevenue'),
                'GrossMargin': info.get('grossMargins'),
                'ProfitMargin': info.get('profitMargins'),
                'ROE': info.get('returnOnEquity'),
                'MarketCap': info.get('marketCap')
            }

            missing_fields = [k for k, v in row.items() if k != 'Ticker' and v in [None, 'None']]
            return row, ", ".join(missing_fields) if missing_fields else None

        except Exception as e:
            return None, str(e)

    # Extraction principale
    tickers = df_sp500['Ticker'].tolist()
    data = []
    error_log = []

    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] - {ticker}")
        result, error = get_features(ticker)
        if result:
            data.append(result)
            if error:
                error_log.append({'Ticker': ticker, 'MissingFields': error})
        else:
            error_log.append({'Ticker': ticker, 'MissingFields': error})
        sleep(0.5)

    df_features = pd.DataFrame(data)
    df_missing = pd.DataFrame(error_log)
    df_final = pd.merge(df_features, df_sp500, on="Ticker", how="left")
    df_final = df_final.dropna(subset=["Return_6M", "Volatility"]).reset_index(drop=True)

    # ----------------------------------------------------------
    # 3) Réintégration manuelle des tickers manquants
    # ----------------------------------------------------------
    print("[ETL] Étape 3/14 – Extraction des données fondamentales (boucle sur les tickers S&P500)")

    tickers_missing = [
        "ABBV","ABT","ACGL","ALB","AOS","AZO","BA","BAX","BKNG","BLK","BSX","BXP","CAG",
        "CAH","CCI","CRWD","CZR","DELL","DPZ","EIX","EL","EQR","EW","EXE","EXR","FDX",
        "FE","FICO","FSLR","GEV","HLT","HPQ","INTC","INTU","KEY","KMI","KVUE","LNT",
        "LOW","MAR","MCD","MCK","MDLZ","MO","MRK","MRNA","MS","MSCI","MTCH","MTD","NI",
        "OKE","ORLY","OTIS","PARA","PFE","PKG","PM","PPG","RF","RJF","SBAC","SBUX",
        "SCHW","SJM","SOLV","STX","STZ","TDG","TFC","TKO","TRGP","TTWO","TXN","V",
        "VLTO","VRSN","VRTX","VTRS","WBA","WBD","WDC","WRB","WTW","WYNN","YUM","ZTS"
        ]
    fields = ['Return_6M', 'Volatility', 'Beta', 'PE', 'PB', 'EV_Revenue', 'GrossMargin', 'ProfitMargin', 'ROE', 'MarketCap']
    final_rows = []
    missing_fields_report = []

    for i, t in enumerate(tickers_missing):
        try:
            stock = yf.Ticker(t)
            info = stock.info
            hist = stock.history(period="6mo", interval="1d")
            if hist.shape[0] < 50:
                continue

            return_6m = hist['Close'].iloc[-1] / hist['Close'].iloc[0] - 1
            volatility = hist['Close'].pct_change().std()

            row = {
                'Ticker': t,
                'Return_6M': return_6m,
                'Volatility': volatility,
                'Beta': info.get('beta'),
                'PE': info.get('trailingPE'),
                'PB': info.get('priceToBook'),
                'EV_Revenue': info.get('enterpriseToRevenue'),
                'GrossMargin': info.get('grossMargins'),
                'ProfitMargin': info.get('profitMargins'),
                'ROE': info.get('returnOnEquity'),
                'MarketCap': info.get('marketCap')
            }

            available_data = [k for k, v in row.items() if k != 'Ticker' and v not in [None, 'None']]
            missing_data = [k for k in row if row[k] in [None, 'None']]

            if available_data:
                final_rows.append(row)
                if missing_data:
                    missing_fields_report.append({'Ticker': t, 'MissingFields': ", ".join(missing_data)})

        except Exception as e:
            missing_fields_report.append({'Ticker': t, 'MissingFields': str(e)})
        sleep(1)

    df_reintegrated = pd.DataFrame(final_rows)
    df_reintegrated = df_reintegrated.merge(df_sp500, on="Ticker", how="left")
    df_final = pd.concat([df_final, df_reintegrated], ignore_index=True)
    df_missing_report = pd.DataFrame(missing_fields_report)

    # -------------------------------
    # 4) Nettoyage et traitement
    # -------------------------------
    print("[ETL] Étape 4/14 – Construction du DataFrame df_fundamentals S&P500")

    for col in ['ROE', 'PE', 'EV_Revenue']:
        if col in df_final.columns:
            df_final[col] = df_final.groupby('Sector')[col].transform(lambda x: x.fillna(x.median()))

    df_final['Beta'] = df_final['Beta'].fillna(df_final['Beta'].median())
    df_final['Return_6M'] = df_final['Return_6M'].fillna(df_final['Return_6M'].mean())
    df_final['IndexSource'] = 'SP500'
    df_final = df_final.drop_duplicates(subset='Ticker', keep='first').reset_index(drop=True)

    # -------------------------------
    # 5) Indicateurs techniques
    # -------------------------------
    print("[ETL] Étape 5/14 – Nettoyage des fondamentaux (suppression NaN et doublons) S&P500")
    df_tech = []
    for i, ticker in enumerate(df_final['Ticker']):
        try:
            print(f"[{i+1}/{len(df_final)}] - {ticker}")
            stock = yf.Ticker(ticker)
            hist = stock.history(period="6mo", interval="1d")
            if len(hist) < 50:
                continue

            hist = hist.dropna()
            hist['RSI'] = ta.momentum.RSIIndicator(hist['Close'], window=14).rsi()
            hist['SMA_20'] = ta.trend.SMAIndicator(hist['Close'], window=20).sma_indicator()
            hist['SMA_50'] = ta.trend.SMAIndicator(hist['Close'], window=50).sma_indicator()
            hist['MACD'] = ta.trend.MACD(hist['Close']).macd()
            hist['MOM_10'] = ta.momentum.ROCIndicator(hist['Close'], window=10).roc()
            bb = ta.volatility.BollingerBands(hist['Close'])
            hist['BB_PCT'] = (hist['Close'] - bb.bollinger_lband()) / (bb.bollinger_hband() - bb.bollinger_lband())

            row = {
                'Ticker': ticker,
                'RSI_14': hist['RSI'].iloc[-1],
                'SMA20_above_SMA50': int(hist['SMA_20'].iloc[-1] > hist['SMA_50'].iloc[-1]),
                'MACD': hist['MACD'].iloc[-1],
                'Momentum_10': hist['MOM_10'].iloc[-1],
                'BB_Percent': hist['BB_PCT'].iloc[-1]
            }
            df_tech.append(row)
        except Exception as e:
            print(f"Erreur indicateurs {ticker}: {e}")
        sleep(0.5)

    df_tech_indicators = pd.DataFrame(df_tech)
    df_final = df_final.merge(df_tech_indicators, on="Ticker", how="left")

    print(f" Fusion réussie. df_final = {df_final.shape}")
    print(" Aperçu des colonnes ajoutées :")
    print(df_tech_indicators.columns.tolist())

    # -------------------------------
    # 6) Secteurs
    # -------------------------------
    print("[ETL] Étape 6/14 – Ajout des métadonnées sociétés CAC40 (secteur, nom, etc.)")

    if "Sector" not in df_final.columns:
     raise ValueError(" La colonne 'Sector' est absente de df_final.")

    df_final["Sector"] = df_final["Sector"].astype(str).str.strip()

    unique_sectors = sorted(df_final["Sector"].dropna().unique())

    print(f" {len(unique_sectors)} valeurs uniques dans la colonne 'Sector' :\n")
    for s in unique_sectors:
     print(f"- {s}")


    tickers_cac40 = [
        "ACA.PA", "AI.PA", "AIR.PA", "ALO.PA", "ORA.PA", "CS.PA", "BNP.PA", "CAP.PA",
        "CA.PA", "SGO.PA", "SAN.PA", "BN.PA", "EN.PA", "EL.PA", "ENGI.PA", "HO.PA",
        "KER.PA", "OR.PA", "LR.PA", "MC.PA", "ML.PA", "MT.AS", "RI.PA", "RMS.PA",
        "PUB.PA", "RNO.PA", "SAF.PA", "STLA.PA", "STM.PA", "SU.PA", "SW.PA", "GLE.PA",
        "VIE.PA", "VIV.PA", "URW.AS", "FR.PA", "TTE.PA", "STMPA.PA", "DJI.PA", "BOU.PA"
    ]

    print("[ETL] Étape 7/14 – Ajout des données fondamentales cac40")
    # 7. Données fondamentales
    fundamental_data = []

    for i, ticker in enumerate(tickers_cac40):
        print(f"[{i+1}/{len(tickers_cac40)}] - {ticker}")
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            row = {
                'Ticker': ticker,
                'PE': info.get('trailingPE'),
                'PB': info.get('priceToBook'),
                'EV_Revenue': info.get('enterpriseToRevenue'),
                'ROE': info.get('returnOnEquity'),
                'ProfitMargin': info.get('profitMargins'),
                'GrossMargin': info.get('grossMargins'),
                'Beta': info.get('beta'),
                'MarketCap': info.get('marketCap')
            }
            fundamental_data.append(row)
        except Exception as e:
            print(f"Erreur pour {ticker} : {e}")
        sleep(1)

    df_fundamentaux = pd.DataFrame(fundamental_data)
    print("[ETL] Étape8/14 – Ajout des données techniques Cac40")
    # 8. Données techniques
    technical_data = []

    for i, ticker in enumerate(tickers_cac40):
        print(f"Calcul technique [{i+1}/{len(tickers_cac40)}] - {ticker}")
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="6mo", interval="1d")
            if len(hist) < 50:
                continue
            hist = hist.dropna()
            hist['RSI'] = ta.momentum.RSIIndicator(hist['Close'], window=14).rsi()
            hist['SMA_20'] = ta.trend.SMAIndicator(hist['Close'], window=20).sma_indicator()
            hist['SMA_50'] = ta.trend.SMAIndicator(hist['Close'], window=50).sma_indicator()
            hist['MACD'] = ta.trend.MACD(hist['Close']).macd()
            hist['Momentum_10'] = ta.momentum.ROCIndicator(hist['Close'], window=10).roc()
            bb = ta.volatility.BollingerBands(hist['Close'])
            hist['BB_PCT'] = (hist['Close'] - bb.bollinger_lband()) / (bb.bollinger_hband() - bb.bollinger_lband())

            technical_data.append({
                'Ticker': ticker,
                'RSI_14': hist['RSI'].iloc[-1],
                'SMA20_above_SMA50': int(hist['SMA_20'].iloc[-1] > hist['SMA_50'].iloc[-1]),
                'MACD': hist['MACD'].iloc[-1],
                'Momentum_10': hist['Momentum_10'].iloc[-1],
                'BB_Percent': hist['BB_PCT'].iloc[-1]
            })

        except Exception as e:
            print(f"Erreur technique pour {ticker}: {e}")
        sleep(0.5)

    df_techniques = pd.DataFrame(technical_data)

    # 9. Merge fondamentaux + techniques
    print("[ETL] Étape 9/14 – Merge fondamentaux + techniques cac40)")
    df_cac40_full = pd.merge(df_fundamentaux, df_techniques, on="Ticker", how="outer")
    df_cac40_full['IndexSource'] = 'CAC40'


    # 4. Returns + volatility
    returns_data_cac = []

    for i, row in df_cac40_full.iterrows():
        ticker = row['Ticker']
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="6mo", interval="1d")
            if len(hist) < 50:
                continue
            return_6m = hist['Close'].iloc[-1] / hist['Close'].iloc[0] - 1
            volatility = hist['Close'].pct_change().std()
            returns_data_cac.append({
                'Ticker': ticker,
                'Return_6M': return_6m,
                'Volatility': volatility
            })
        except:
            continue

    df_returns_cac = pd.DataFrame(returns_data_cac)
    df_cac40_full = df_cac40_full.merge(df_returns_cac, on='Ticker', how='left')

    # 5. Remplissage valeurs manquantes fondamentales
    cols_fondamentales = ['PE', 'PB', 'EV_Revenue', 'ROE', 'GrossMargin', 'ProfitMargin', 'Beta', 'Return_6M']
    for col in cols_fondamentales:
        if col in df_cac40_full.columns:
            median_value = df_cac40_full[col].median()
            df_cac40_full[col] = df_cac40_full[col].fillna(median_value)

    # 6. Mapping secteurs
    print("[ETL] Étape 10/14 – Secteurs cac40)")
    secteurs_cac40 = {
        "ACA.PA": "Financials", "AI.PA": "Industrials", "AIR.PA": "Industrials",
        "ALO.PA": "Information Technology", "ORA.PA": "Communication Services", "CS.PA": "Industrials",
        "BNP.PA": "Financials", "CAP.PA": "Information Technology", "CA.PA": "Financials",
        "SGO.PA": "Materials", "SAN.PA": "Health Care", "BN.PA": "Financials",
        "EN.PA": "Consumer Staples", "EL.PA": "Consumer Staples", "ENGI.PA": "Energy",
        "HO.PA": "Consumer Staples", "KER.PA": "Consumer Discretionary", "OR.PA": "Consumer Staples",
        "LR.PA": "Consumer Discretionary", "MC.PA": "Consumer Discretionary", "ML.PA": "Industrials",
        "MT.AS": "Materials", "RI.PA": "Consumer Staples", "RMS.PA": "Consumer Discretionary",
        "PUB.PA": "Communication Services", "RNO.PA": "Consumer Discretionary",
        "SAF.PA": "Industrials", "STLA.PA": "Consumer Discretionary",
        "STM.PA": "Information Technology", "SU.PA": "Energy", "SW.PA": "Financials",
        "GLE.PA": "Financials", "VIE.PA": "Utilities", "VIV.PA": "Communication Services",
        "URW.AS": "Real Estate", "FR.PA": "Industrials", "TTE.PA": "Energy",
        "STMPA.PA": "Industrials", "DJI.PA": "Information Technology", "BOU.PA": "Industrials"
    }
    df_cac40_full['Sector'] = df_cac40_full['Ticker'].map(secteurs_cac40)

    def remove_invalid_tickers(df, ticker_column='Ticker', min_days=50):
        tickers_invalides = []
        for ticker in df[ticker_column]:
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(period="6mo", interval="1d")
                if len(hist) < min_days:
                    tickers_invalides.append(ticker)
            except Exception:
                tickers_invalides.append(ticker)

        df_clean = df[~df[ticker_column].isin(tickers_invalides)].reset_index(drop=True)
        print(f"Tickers invalides supprimés : {tickers_invalides}")
        print(f"Taille finale du DataFrame : {df_clean.shape}")
        return df_clean, tickers_invalides

    print("[ETL] Étape 11/14 – Nettoyage cac40)")
    # 11. Suppression tickers invalides si nécessaire
    df_cac40_full, tickers_invalides = remove_invalid_tickers(df_cac40_full, ticker_column='Ticker', min_days=50)

    df_cac40_full['IndexSource'] = 'CAC40'
    df_final = pd.concat([df_final, df_cac40_full], ignore_index=True)
    print(f"📦 Taille finale du DataFrame fusionné : {df_final.shape}")


    # ------------------------------
    # 12) Nikkei 225 - Import
    # ------------------------------
    print("[ETL] Étape 12/14 – Import Nikkei225)")
    def import_nikkei225_data(df_final, min_hist_days=50):

        # Étape 1 - Scraper les tickers du Nikkei 225
        url = "https://en.wikipedia.org/wiki/Nikkei_225"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        df_rows = []
        current_sector = None
        in_components = False

        for h2 in soup.find_all("h2"):
            if "Components" in h2.text:
                in_components = True
                break

        if not in_components:
            raise ValueError("Balise <h2> contenant 'Components' introuvable.")

        for tag in h2.find_all_next():
            if tag.name == "h2":
                break
            if tag.name == "h3":
                current_sector = tag.get_text(strip=True).replace("[edit]", "")
            elif tag.name == "ul" and current_sector:
                for li in tag.find_all("li"):
                    try:
                        links = li.find_all("a")
                        company_name = links[0].get_text(strip=True)
                        for a in reversed(links):
                            code = a.get_text(strip=True)
                            if code.isdigit():
                                ticker = f"{code}.T" 
                                df_rows.append({
                                    "Ticker": ticker,
                                    "Company": company_name,
                                    "Sector": current_sector
                                })
                                break
                    except Exception:
                        continue

        df_nikkei = pd.DataFrame(df_rows).drop_duplicates().reset_index(drop=True)
        print(f"{len(df_nikkei)} entreprises extraites avec tickers depuis le Nikkei 225")

        # Étape 2 - Données fondamentales
        fundamentals = []
        for i, row in df_nikkei.iterrows():
            ticker = row['Ticker']
            try:
                print(f"[{i+1}/{len(df_nikkei)}] - Fondamentaux : {ticker}")
                stock = yf.Ticker(ticker)
                info = stock.info

                fundamentals.append({
                    'Ticker': ticker,
                    'PE': info.get('trailingPE'),
                    'PB': info.get('priceToBook'),
                    'EV_Revenue': info.get('enterpriseToRevenue'),
                    'ROE': info.get('returnOnEquity'),
                    'ProfitMargin': info.get('profitMargins'),
                    'GrossMargin': info.get('grossMargins'),
                    'Beta': info.get('beta'),
                    'MarketCap': info.get('marketCap')
                })
            except Exception as e:
                print(f"Erreur fondamentaux pour {ticker} : {e}")
            sleep(0.3)

        df_funda_japan = pd.DataFrame(fundamentals)

        # Étape 3 - Indicateurs techniques
        tech_indicators = []
        for i, ticker in enumerate(df_nikkei['Ticker']):
            try:
                print(f"[{i+1}/{len(df_nikkei)}] - Techniques : {ticker}")
                stock = yf.Ticker(ticker)
                hist = stock.history(period="6mo", interval="1d")

                if len(hist) < min_hist_days:
                    continue

                hist = hist.dropna()
                rsi = ta.momentum.RSIIndicator(hist['Close'], window=14).rsi().iloc[-1]
                sma20 = ta.trend.SMAIndicator(hist['Close'], window=20).sma_indicator().iloc[-1]
                sma50 = ta.trend.SMAIndicator(hist['Close'], window=50).sma_indicator().iloc[-1]
                macd = ta.trend.MACD(hist['Close']).macd().iloc[-1]
                momentum = ta.momentum.ROCIndicator(hist['Close'], window=10).roc().iloc[-1]
                bb = ta.volatility.BollingerBands(hist['Close'])
                bb_pct = (hist['Close'].iloc[-1] - bb.bollinger_lband().iloc[-1]) / (
                    bb.bollinger_hband().iloc[-1] - bb.bollinger_lband().iloc[-1])

                tech_indicators.append({
                    'Ticker': ticker,
                    'RSI_14': rsi,
                    'SMA20_above_SMA50': int(sma20 > sma50),
                    'MACD': macd,
                    'Momentum_10': momentum,
                    'BB_Percent': bb_pct
                })

            except Exception as e:
                print(f"Erreur technique pour {ticker} : {e}")
            sleep(0.3)

        df_tech_japan = pd.DataFrame(tech_indicators)

        # Étape 4 - Return & volatility data

        returns_data = []

        for i, ticker in enumerate(df_nikkei['Ticker']):
            try:
                print(f"[{i+1}/{len(df_nikkei)}] - Return & Volatility : {ticker}")
                stock = yf.Ticker(ticker)
                hist = stock.history(period="6mo", interval="1d")

                if len(hist) < min_hist_days:
                    continue

                return_6m = hist['Close'].iloc[-1] / hist['Close'].iloc[0] - 1
                volatility = hist['Close'].pct_change().std()

                returns_data.append({
                    'Ticker': ticker,
                    'Return_6M': return_6m,
                    'Volatility': volatility
                })
            except Exception as e:
                print(f"Erreur Return/Volatility pour {ticker} : {e}")
            sleep(0.3)

        df_returns_japan = pd.DataFrame(returns_data)

        # Étape 5 - Fusion et ajout du flag
        df_japan_full = df_nikkei.merge(df_funda_japan, on='Ticker', how='left')
        df_japan_full = df_japan_full.merge(df_tech_japan, on='Ticker', how='left')
        df_japan_full['IndexSource'] = 'Nikkei225'

        # Étape 6 - Fusion avec df_final existant
        # Fusion des returns avec df_japan_full AVANT concaténation
        if 'Ticker' in df_returns_japan.columns:
            df_japan_full = df_japan_full.merge(df_returns_japan, on='Ticker', how='left')
        else:
            print("❌ ERREUR : df_returns_japan ne contient pas la colonne 'Ticker'")

        # Puis seulement maintenant on concatène dans df_final
        df_final = pd.concat([df_final, df_japan_full], ignore_index=True)
        print(f"✅ df_final mis à jour avec Nikkei 225 : {df_final.shape}")
        print(f"df_final mis à jour avec Nikkei 225 : {df_final.shape}")

        return df_final
    df_final = import_nikkei225_data(df_final)


    print("[ETL] Étape 13/14 – Nettoyge final)")
    # --------------------
    # 14) NETTOYAGE FINAL
    # --------------------

    # 1 Compter les valeurs manquantes par colonne
    missing_summary = df_final.isna().sum()
    missing_percent = (missing_summary / len(df_final)) * 100
    missing_df = pd.DataFrame({
        "MissingCount": missing_summary,
        "MissingPercent": missing_percent
    }).sort_values("MissingPercent", ascending=False)

    print("📊 Valeurs manquantes par colonne :")
    print(missing_df[missing_df["MissingCount"] > 0])

    # 2 Extraire les lignes avec Company manquant
    df_missing_company = df_final[(df_final['Company'].isna()) & (df_final['IndexSource'] == 'CAC40')]

    # 3 Boucle pour aller chercher 'shortName' depuis yfinance
    company_names = []

    for i, row in df_missing_company.iterrows():
        ticker = row['Ticker']
        try:
            print(f" [{i+1}] Récupération du nom pour {ticker}")
            stock = yf.Ticker(ticker)
            name = stock.info.get("shortName", None)
            company_names.append({'Ticker': ticker, 'Company': name})
        except Exception as e:
            print(f" Erreur pour {ticker}: {e}")
            company_names.append({'Ticker': ticker, 'Company': None})

    # 4 Création d'un DataFrame des noms récupérés
    df_names = pd.DataFrame(company_names)

    # 5 Fusion dans df_final pour mise à jour des noms
    df_final = df_final.merge(df_names, on='Ticker', how='left', suffixes=('', '_new'))
    df_final['Company'] = df_final['Company'].fillna(df_final['Company_new'])
    df_final.drop(columns=['Company_new'], inplace=True)

    # 6 Beta 
    df_final['Beta'] = df_final.groupby('Sector')['Beta'].transform(lambda x: x.fillna(x.median()))

    # 7 'PE', 'ROE', 'EV_Revenue'

    colonnes_essentielles = ['PE', 'ROE', 'EV_Revenue']

    df_final = df_final.dropna(subset=colonnes_essentielles).reset_index(drop=True)
    sparsity_threshold = 0.5
    cols_to_keep = missing_percent[missing_percent <= sparsity_threshold * 100].index.tolist()
    df_final = df_final[cols_to_keep]

    df_final = df_final.reset_index(drop=True)
    print(f"🧼 Nettoyage terminé. Dimensions finales : {df_final.shape}")

    # 8 Scale Value Score.

    scaler = MinMaxScaler()

    # --- VALUE SCORE (inverse : plus bas PE/PB/EV mieux c’est)
    value_cols = ['PE', 'PB', 'EV_Revenue']
    value_scaled = pd.DataFrame(scaler.fit_transform(df_final[value_cols]), columns=value_cols)
    df_final['ValueScore'] = 1 - value_scaled.mean(axis=1)  # inversion logique

    # --- QUALITY SCORE (ROE, GrossMargin, ProfitMargin) + ajustement Beta
    quality_cols = ['ROE', 'GrossMargin', 'ProfitMargin']
    quality_scaled = pd.DataFrame(scaler.fit_transform(df_final[quality_cols]), columns=quality_cols)
    df_final['QualityScore'] = quality_scaled.mean(axis=1)

    # Pénalité sur la volatilité si Beta > 1
    df_final['QualityScore'] *= (1 - df_final['Beta'].clip(lower=0, upper=2) / 4)

    # --- SIGNAL SCORE (inversé si RSI élevé)
    signal_cols = ['RSI_14', 'MACD', 'Momentum_10']
    signal_scaled = pd.DataFrame(scaler.fit_transform(df_final[signal_cols]), columns=signal_cols)
    df_final['SignalScore'] = 1 - signal_scaled.mean(axis=1)

    # 9 Sector Mapping.

    sector_mapping = {
        # Communication Services
        "Communication Services": "Communication Services",
        "Communications": "Communication Services",
        
        # Consumer Discretionary
        "Consumer Discretionary": "Consumer Discretionary",
        "Retail": "Consumer Discretionary",
        "Automotive": "Consumer Discretionary",
        "Air transport": "Consumer Discretionary",
        "Land transport": "Consumer Discretionary",
        "Marine transport": "Consumer Discretionary",
        "Railway/bus": "Consumer Discretionary",
        "Services": "Consumer Discretionary",
        "Textiles & apparel": "Consumer Discretionary",
        "Trading companies": "Consumer Discretionary",

        # Consumer Staples
        "Consumer Staples": "Consumer Staples",
        "Foods": "Consumer Staples",
        "Fishery": "Consumer Staples",
        "Pulp & paper": "Consumer Staples",

        # Energy
        "Energy": "Energy",
        "Gas": "Energy",
        "Petroleum": "Energy",
        "Electric power": "Energy",

        # Financials
        "Financials": "Financials",
        "Banking": "Financials",
        "Insurance": "Financials",
        "Securities": "Financials",
        "Other financial services": "Financials",

        # Health Care
        "Health Care": "Health Care",
        "Pharmaceuticals": "Health Care",

        # Industrials
        "Industrials": "Industrials",
        "Construction": "Industrials",
        "Machinery": "Industrials",
        "Shipbuilding": "Industrials",
        "Steel": "Industrials",
        "Precision instruments": "Industrials",
        "Warehousing": "Industrials",
        "Other manufacturing": "Industrials",

        # Information Technology
        "Information Technology": "Information Technology",
        "Electric machinery": "Information Technology",

        # Materials
        "Materials": "Materials",
        "Chemicals": "Materials",
        "Glass & ceramics": "Materials",
        "Mining": "Materials",
        "Nonferrous metals": "Materials",
        "Rubber": "Materials",

        # Real Estate
        "Real Estate": "Real Estate",
        "Real estate": "Real Estate",

        # Utilities
        "Utilities": "Utilities"
     }
    # Nettoyage de base
    df_final["Sector"] = df_final["Sector"].astype(str).str.strip()

    # Application du mapping
    df_final["Sector"] = df_final["Sector"].map(sector_mapping).fillna("Other")

    # Vérification finale
    print("Harmonisation des secteurs terminée. Valeurs uniques finales :\n")
    print(df_final["Sector"].value_counts())

    # 10. Suppression colonne inutile 'Prefix4' si présente
    if 'Prefix4' in df_final.columns:
        df_final.drop(columns=['Prefix4'], inplace=True)

    # 11. Ajout de la date d’extraction
    today_str = datetime.now().strftime("%Y-%m-%d")
    df_final['ExtractionDate'] = today_str

    # Vérification finale
    print(f"✅ Colonne 'Prefix4' supprimée (si existait) et 'ExtractionDate' ajoutée : {today_str}")

    os.makedirs("data", exist_ok=True)
    df_final.to_csv("data/df_final.csv", index=False)

    conn = sqlite3.connect("data/stock_analysis.db")
    df_final.to_sql("companies", conn, if_exists="replace", index=False)
    conn.close()

    print(" Données stockées dans la base SQLite (stock_analysis.db).")
    print("[ETL] Étape 14/14 – Extraction terminée.)")

if __name__ == "__main__":
    run_pipeline_etl()
