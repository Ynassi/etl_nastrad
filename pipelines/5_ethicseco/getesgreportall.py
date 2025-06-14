import os
import time
import requests
import pandas as pd
import re
from urllib.parse import urlparse, urljoin
from playwright.sync_api import sync_playwright

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
REPORTS_DIR = os.path.join(DATA_DIR, "reports", "esg")
CSV_PATH = os.path.join(DATA_DIR, "df_final_merged.csv")
FOUND_CSV = os.path.join(DATA_DIR, "found.csv")
MISSING_CSV = os.path.join(DATA_DIR, "missing.csv")

os.makedirs(REPORTS_DIR, exist_ok=True)
df = pd.read_csv(CSV_PATH)
found_reports = []
missing_reports = []

def save_file(url, ticker, count, index, ext="pdf"):
    folder = os.path.join(REPORTS_DIR, index, ticker)
    os.makedirs(folder, exist_ok=True)
    filename = os.path.join(folder, f"{ticker}{count}.{ext}")
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            with open(filename, "wb") as f:
                f.write(response.content)
            print(f"\n📥 Fichier enregistré : {filename}")
            found_reports.append({"Ticker": ticker, "Index": index, "Type": ext, "URL": url})
            return True
        else:
            print(f"\n❌ Erreur HTTP : {url}")
            return False
    except Exception as e:
        print(f"\n⚠️ Erreur téléchargement {url} : {e}")
        return False

def query_search_engines(page, query, retries=2):
    for attempt in range(retries):
        try:
            print(f"🔍 Bing Query (tentative {attempt+1}): {query}")
            page.goto(f'https://www.bing.com/search?q={query}')
            time.sleep(4)
            results = page.locator("li.b_algo")
            urls = []
            for i in range(results.count()):
                try:
                    element = results.nth(i)
                    url = element.locator("a").first.get_attribute("href")
                    if url and "bing.com" not in url:
                        urls.append(url)
                except Exception as e:
                    print(f"⚠️ Erreur parsing résultat {i} : {e}")
            if urls:
                return urls
        except Exception as e:
            print(f"⚠️ Bing search échouée : {e}")
            time.sleep(3)
    return []

def fallback_duckduckgo(page, query):
    try:
        print(f"🔁 DuckDuckGo fallback : {query}")
        page.goto(f'https://duckduckgo.com/?q={query}&ia=web')
        time.sleep(4)
        results = page.locator("a.result__a")
        urls = []
        for i in range(results.count()):
            try:
                url = results.nth(i).get_attribute("href")
                if url:
                    urls.append(url)
            except:
                continue
        return urls
    except Exception as e:
        print(f"⚠️ DuckDuckGo échoué : {e}")
        return []

def search_esg_report(ticker, company, index):
    clean_company = re.sub(r'[^a-zA-Z0-9 ]', '', company)
    clean_ticker = re.sub(r'[^a-zA-Z0-9 ]', '', ticker)
    query = f'{clean_ticker} {clean_company} sustainability global impact carbon footprint esg report filetype:pdf 2025'
    pdf_count = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        urls = query_search_engines(page, query)
        if not urls:
            urls = fallback_duckduckgo(page, query)

        for url in urls:
            if pdf_count >= 2:
                break

            # HEAD check + fallback domain fix
            try:
                print(f"\n🔎 HEAD check pour : {url}")
                head = requests.head(url, timeout=5, allow_redirects=True)
                content_type = head.headers.get("Content-Type", "")
                print(f"ℹ️ Content-Type: {content_type}")
                if 'application/pdf' in content_type:
                    if save_file(url, ticker, pdf_count + 1, index, ext="pdf"):
                        pdf_count += 1
                        continue
            except:
                # Retry with domain corrections
                for prefix in ["https://www.", "https://"]:
                    alt = url.replace("https://www.my.", prefix).replace("https://my.", prefix)
                    try:
                        if save_file(alt, ticker, pdf_count + 1, index, ext="pdf"):
                            pdf_count += 1
                            break
                    except:
                        continue

            # Direct PDF fallback
            if ".pdf" in url.lower():
                try:
                    print(f"⚡ Tentative brute de download : {url}")
                    if save_file(url, ticker, pdf_count + 1, index, ext="pdf"):
                        pdf_count += 1
                        continue
                except Exception as e:
                    print(f"⚠️ Download direct échoué : {e}")

            # Page exploration fallback
            try:
                print(f"\n🔄 Fallback via page web : {url}")
                page.goto(url)
                time.sleep(3)
                print(f"📄 Titre de la page : {page.title()}")

                pdf_links = page.locator("a[href*='.pdf']")
                for i in range(pdf_links.count()):
                    if pdf_count >= 2:
                        break
                    href = pdf_links.nth(i).get_attribute("href")
                    if href:
                        full_url = urljoin(url, href)
                        if save_file(full_url, ticker, pdf_count + 1, index, ext="pdf"):
                            pdf_count += 1

                all_links = page.locator("a[href]")
                for i in range(all_links.count()):
                    if pdf_count >= 2:
                        break
                    href = all_links.nth(i).get_attribute("href")
                    if href and 'pdf' in href.lower():
                        full_url = urljoin(url, href)
                        try:
                            resp = requests.get(full_url, timeout=10, stream=True)
                            if resp.status_code == 200 and 'application/pdf' in resp.headers.get("Content-Type", ""):
                                if save_file(full_url, ticker, pdf_count + 1, index, ext="pdf"):
                                    pdf_count += 1
                        except Exception as e:
                            print(f"⚠️ Download lien générique échoué {full_url} : {e}")
                            continue

                fallback_direct_pdf = url.split('?')[0]
                if pdf_count < 2 and (fallback_direct_pdf.endswith(".pdf") or 'pdf' in fallback_direct_pdf.lower()):
                    try:
                        resp = requests.get(fallback_direct_pdf, timeout=10)
                        if resp.status_code == 200 and 'application/pdf' in resp.headers.get("Content-Type", ""):
                            if save_file(fallback_direct_pdf, ticker, pdf_count + 1, index, ext="pdf"):
                                pdf_count += 1
                    except Exception as e:
                        print(f"⚠️ Fallback direct PDF échoué : {e}")

                img_links = page.locator("img[src$='.png'], img[src$='.jpg'], img[src$='.jpeg']")
                for i in range(min(img_links.count(), 3)):
                    if pdf_count >= 2:
                        break
                    src = img_links.nth(i).get_attribute("src")
                    if src:
                        full_img_url = urljoin(url, src)
                        if save_file(full_img_url, ticker, f"{pdf_count + 1}_img", index, ext=src.split('.')[-1]):
                            print(f"🖼️ Image ESG sauvegardée pour {ticker}")
                            pdf_count += 1
            except Exception as e:
                print(f"⚠️ Erreur fallback page web pour {url} : {e}")

        if pdf_count == 0:
            print(f"❌ Aucun rapport ESG trouvé pour {ticker}")
            missing_reports.append({"Ticker": ticker, "Index": index})
        browser.close()

def run():
    for _, row in df.iterrows():
        ticker = str(row['Ticker'])
        index = row['IndexSource']
        company = row['Company'] if 'Company' in row else ''
        search_esg_report(ticker, company, index)

    pd.DataFrame(found_reports).to_csv(FOUND_CSV, index=False)
    pd.DataFrame(missing_reports).to_csv(MISSING_CSV, index=False)

if __name__ == "__main__":
    run()
