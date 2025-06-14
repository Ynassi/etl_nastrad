import os
import time
import urllib.parse
import pandas as pd
import feedparser
import requests
import json
import re
from transformers import pipeline
from tqdm import tqdm
from deep_translator import GoogleTranslator

#  Dossiers
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_PATH = os.path.join(DATA_DIR, "sentiment_news_summary_full.csv")
JSON_PATH = os.path.join(DATA_DIR, "news_summaries_full.json")

#  Tickers
df = pd.read_csv(os.path.join(DATA_DIR, "df_final_merged.csv"))
tickers = df["Ticker"].dropna().unique()  # ✅ toute la table

#  FinBERT
print("🔁 Chargement FinBERT...")
finbert = pipeline("sentiment-analysis", model="ProsusAI/finbert", device=-1)

#  Mistral API externe
MISTRAL_API = "https://fzhvs2csuz2ezl-8000.proxy.runpod.net/analyze"

#  Traduction
def translate_to_english(text):
    try:
        return GoogleTranslator(source='auto', target='en').translate(text)
    except Exception as e:
        print(f"[TRANSLATION ERROR] {e}")
        return text

#  RSS fetch
def get_rss_entries(ticker):
    query = urllib.parse.quote(f"{ticker} stock")
    rss_url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(rss_url)
    return [{"title": e.title.strip(), "url": e.link.strip(), "date": e.get("published", "")[:10]} for e in feed.entries[:20]]

#  Appel Mistral
def call_mistral_summary_global(titles):
    try:
        full_text = "\n".join(titles)
        response = requests.post(MISTRAL_API, json={"task": "news_summary_global", "input": full_text}, timeout=90)
        if response.status_code == 200:
            data = response.json()
            print("[MISTRAL DEBUG FULL RESPONSE]", data)
            if isinstance(data, dict) and "outputs" in data:
                return data["outputs"][0].strip()
    except Exception as e:
        print(f"[Mistral Summary ERROR] {e}")
    return ""

#  Bullets
def extract_bullet_points(summary):
    try:
        response = requests.post(MISTRAL_API, json={"task": "news_bullet_points", "input": summary}, timeout=90)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and "outputs" in data:
                bullets = data["outputs"][0].strip().split("\n")
                return [b.strip("-• ").strip() for b in bullets if b.strip()]
    except Exception as e:
        print(f"[Bullet Extraction ERROR] {e}")
    return []

#  FinBERT sur bullets
def analyze_bullet_points(bullets):
    try:
        results = finbert(bullets)
        labels = [r["label"].upper() for r in results]
        counts = {"POSITIVE": 0, "NEGATIVE": 0, "NEUTRAL": 0}
        for label in labels:
            if label in counts:
                counts[label] += 1
        return counts
    except Exception as e:
        print(f"[FinBERT Bullet Analysis ERROR] {e}")
        return {"POSITIVE": 0, "NEGATIVE": 0, "NEUTRAL": 0}

#  FinBERT résumé traduit
def classify_summary_with_finbert(summary):
    try:
        sentences = re.split(r'(?<=[.!?]) +', summary)
        sentences = [s.strip() for s in sentences if len(s.strip()) > 20][:8]
        if not sentences:
            return "NEUTRAL"

        results = finbert(sentences)
        labels = [r["label"].upper() for r in results]

        counts = {"POSITIVE": 0, "NEGATIVE": 0, "NEUTRAL": 0}
        for label in labels:
            if label in counts:
                counts[label] += 1

        pos, neg, neu = counts["POSITIVE"], counts["NEGATIVE"], counts["NEUTRAL"]
        print(f"[SUMMARY LABEL] {counts}", end=" → ")

        if pos > neg:
            return "POSITIVE"
        elif neg > pos:
            return "NEGATIVE"
        elif pos == neg and pos > 0:
            return "NEUTRAL"
        elif pos == 0 and neg == 0 and neu > 0:
            return "NEUTRAL"
        else:
            return "NEUTRAL"
    except Exception as e:
        print(f"[FinBERT Summary Classification ERROR] {e}")
        return "NEUTRAL"

#  Pipeline principal
results = []
mistral_json = {}

print(f"\n Analyse des {len(tickers)} tickers...\n")
for ticker in tqdm(tickers, desc="Tickers"):
    entries = get_rss_entries(ticker)
    if not entries:
        continue

    titles = [e["title"] for e in entries]
    urls = [e["url"] for e in entries]

    try:
        sentiments = finbert(titles, batch_size=8)
    except:
        sentiments = [{"label": "NEUTRAL"}] * len(titles)

    counts = {"POSITIVE": 0, "NEGATIVE": 0, "NEUTRAL": 0}
    sources = []
    for title, url, sent in zip(titles, urls, sentiments):
        label = sent["label"].upper()
        if label in counts:
            counts[label] += 1
        sources.append(f"{title} ({url}) → {label.title()}")

    mistral_summary = call_mistral_summary_global(titles).strip()
    mistral_json[ticker] = mistral_summary

    bullets = extract_bullet_points(mistral_summary)
    bullet_counts = analyze_bullet_points(bullets)

    translated_summary = translate_to_english(mistral_summary)
    mistral_label = classify_summary_with_finbert(translated_summary)

    print(f"({mistral_label}) → {mistral_summary[:150]}...")

    total = sum(counts.values())
    score = (counts["POSITIVE"] - counts["NEGATIVE"]) / total if total else None

    results.append({
        "Ticker": ticker,
        "news_count": total,
        "sentiment_score": round(score, 3) if total else None,
        "positive_ratio": round(counts["POSITIVE"] / total, 2) if total else None,
        "negative_ratio": round(counts["NEGATIVE"] / total, 2) if total else None,
        "neutral_ratio": round(counts["NEUTRAL"] / total, 2) if total else None,
        "mistral_label": mistral_label,
        "bullet_positive_count": bullet_counts["POSITIVE"],
        "bullet_negative_count": bullet_counts["NEGATIVE"],
        "source": " / ".join(sources)
    })

    time.sleep(1)

#  Exports
df_out = pd.DataFrame(results)
df_out.to_csv(CSV_PATH, index=False, encoding="utf-8")
print(f"\n✅ Export CSV : {CSV_PATH}")

with open(JSON_PATH, "w", encoding="utf-8") as f:
    json.dump(mistral_json, f, indent=2, ensure_ascii=False)
print(f"✅ Export JSON : {JSON_PATH}")