import os
import pandas as pd
import requests
from tqdm import tqdm
from secedgar.cik_lookup import CIKLookup

#  Configuration des chemins
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
CSV = os.path.join(ROOT, 'data', 'df_final_merged.csv')
OUT_HTML = os.path.join(ROOT, 'data', 'reports', 'us')
os.makedirs(OUT_HTML, exist_ok=True)

print(f" Les fichiers HTML seront sauvegardés dans : {OUT_HTML}")

#  Chargement des tickers (SP500 + AltScreen) pour les compagnies US uniquement
df = pd.read_csv(CSV)
tickers_us = df[
    (df['IndexSource'].isin(['SP500', 'AltScreen']))
]['Ticker'].dropna().unique().tolist()

print(f"🔎 {len(tickers_us)} tickers américains trouvés (SP500 + AltScreen)")

#  Récupération du mapping CIK
lookup = CIKLookup(tickers_us, user_agent="nastrad@example.com")
cik_map = lookup.lookup_dict
HEADERS = {'User-Agent': 'nastrad@example.com'}

#  Téléchargement des rapports (10-K / 20-F + 2 derniers 10-Q)
print(" Téléchargement des rapports annuels et trimestriels...")
for ticker, cik in tqdm(cik_map.items(), desc="Traitement des entreprises"):
    cik_padded = cik.zfill(10)
    search_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"

    try:
        r = requests.get(search_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        info = r.json()
        filings = info.get('filings', {}).get('recent', {})
        forms = filings.get('form', [])
        acc_nums = filings.get('accessionNumber', [])
        docs = filings.get('primaryDocument', [])
    except Exception as e:
        print(f"[❌] {ticker} : erreur JSON → {e}")
        continue

    downloaded = {"10-K": 0, "20-F": 0, "10-Q": 0}

    for form, acc, doc in zip(forms, acc_nums, docs):
        if form not in ("10-K", "20-F", "10-Q"):
            continue

        if form in ("10-K", "20-F") and downloaded["10-K"] + downloaded["20-F"] >= 1:
            continue
        if form == "10-Q" and downloaded["10-Q"] >= 2:
            continue

        acc_clean = acc.replace('-', '')
        html_url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
            f"{acc_clean}/{doc}"
        )
        out_html = os.path.join(OUT_HTML, f"{ticker}_{form}_{acc}.html")

        try:
            rr = requests.get(html_url, headers=HEADERS, timeout=15)
            rr.raise_for_status()
            with open(out_html, 'w', encoding='utf-8') as f:
                f.write(rr.text)
            downloaded[form] += 1
            print(f"✅ {ticker} : {form} sauvegardé → {out_html}")
        except Exception as e:
            print(f"⚠️ {ticker} : échec {form} → {e}")

        if downloaded["10-Q"] >= 2 and (downloaded["10-K"] + downloaded["20-F"] >= 1):
            break

print("🚀 Téléchargement terminé pour toutes les compagnies américaines.")
