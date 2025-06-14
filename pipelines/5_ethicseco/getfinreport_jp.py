from playwright.sync_api import sync_playwright
import os
import time
import pandas as pd

# Charger les tickers japonais depuis le fichier fusionné
DF_PATH = "../../data/df_final_merged.csv"
df = pd.read_csv(DF_PATH)
tickers = df[df["IndexSource"] == "Nikkei225"]["Ticker"].astype(str).str.replace(".T", "", regex=False).unique().tolist()

# Répertoire de sortie final
OUTPUT_DIR = os.path.abspath("../../data/reports/Nikkei225")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Fonction principales 
def download_latest_pdf(page, ticker):
    for i in range(2):  
        try:
            selector = f"#\\31 201_{i} > td:nth-child(2) > div > div > a"
            link = page.query_selector(selector)
            if link:
                print(f"📥 Lien cliquable détecté: {i}")
                with page.expect_popup() as popup_info:
                    link.click()
                pdf_page = popup_info.value
                pdf_url = pdf_page.url
                print(f"🔗 Redirigé vers: {pdf_url}")

                # Télécharger le contenu PDF
                response = page.request.get(pdf_url)
                filepath = os.path.join(OUTPUT_DIR, f"{ticker}_{i}.pdf")
                with open(filepath, "wb") as f:
                    f.write(response.body())
                print(f"✅ PDF enregistré: {filepath}")
                return
        except Exception as e:
            print(f"⚠️ Problème avec le lien PDF {i}: {e}")
    print(f"❌ Aucun PDF récupéré pour {ticker}")

def run(ticker):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        print(f"\n🔍 Ticker {ticker}")
        page.goto("https://www2.jpx.co.jp/tseHpFront/JJK020030Action.do")

        # Étape 1 : remplir le champ "Code" pour le ticker
        page.fill("#bodycontents > div.pagecontents > form > div.boxOptListed03 > table > tbody > tr:nth-child(3) > td > span > input[type=text]", ticker)
        page.click("input[name='searchButton']")
        page.wait_for_timeout(2000)

        # Étape 2 : cliquer sur la ligne résultat pour accéder aux infos société
        try:
            page.click("#bodycontents > div.pagecontents > form > table > tbody > tr:nth-child(3) > td:nth-child(7) > input")
            page.wait_for_timeout(2000)
        except:
            print(f"❌ Aucun résultat cliquable pour le ticker {ticker}")
            browser.close()
            return

        # Étape 3 : cliquer sur l’onglet "Timely disclosure information"
        try:
            page.click("a[href=\"javascript:changeTab('2');\"]")
            page.wait_for_timeout(1500)
        except Exception as e:
            print(f"⚠️ Impossible de cliquer sur l'onglet disclosure: {e}")
            browser.close()
            return

        # Étape 4 : cliquer pour afficher les états financiers
        try:
            page.click("#closeUpKaiJi0_open > tbody > tr > th > input")
            page.wait_for_timeout(1000)
        except Exception as e:
            print(f"⚠️ Impossible d'ouvrir la section des états financiers: {e}")
            browser.close()
            return

        # Étape 5 : cliquer sur les liens PDF et récupérer l'URL ouverte
        download_latest_pdf(page, ticker)

        browser.close()

if __name__ == "__main__":
    for t in tickers:
        run(t)
