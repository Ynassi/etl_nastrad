import os
import re
import json
import time
import asyncio
import feedparser
import urllib.parse
import pandas as pd
from tqdm.asyncio import tqdm
from transformers import pipeline
from deep_translator import GoogleTranslator
from aiohttp import ClientTimeout
from typing import Union, List
from openai import AsyncOpenAI
from dotenv import load_dotenv

# 🧪 Chargement du fichier .env à la racine du projet
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# 🔐 Authentification OpenAI
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError("❌ OPENAI_API_KEY introuvable. Vérifie ton fichier .env ou ta variable d’environnement.")
client = AsyncOpenAI(api_key=api_key)

# 📁 Répertoires
DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_PATH = os.path.join(DATA_DIR, "sentiment_news_summary_full.csv")
JSON_PATH = os.path.join(DATA_DIR, "news_summaries_full.json")

# 📄 Chargement des tickers
df = pd.read_csv(os.path.join(DATA_DIR, "df_final_merged.csv"))
tickers = df["Ticker"].dropna().unique().tolist()

# ⚙️ Configuration asynchrone
SEMAPHORE = asyncio.Semaphore(6)
TIMEOUT = ClientTimeout(total=60)

# 🤖 Chargement du modèle FinBERT
finbert = pipeline("sentiment-analysis", model="ProsusAI/finbert", device=-1)

# 📚 PROMPT LIBRARY
PROMPT_LIBRARY = {
    "news_summary_global": lambda text: f"""Tu es un assistant financier expert en analyse fondamentale. 
Tu dois lire un ensemble de 15 à 20 titres d’actualités économiques récentes concernant une entreprise cotée.

🎯 Ta mission : Résume les **tendances clés** de ces nouvelles en une **synthèse courte** (3-4 phrases maximum), orientée **investisseur francophone**.

🧠 Objectif :
- Faire ressortir les **signaux forts** (positifs, négatifs ou mixtes)
- Identifier des événements marquants
- Déterminer si l’actualité globale influence le **sentiment de marché**

✍️ Format attendu :
Une synthèse fluide, rédigée en français, comme une brève d’analyste. Pas de liste.

Voici la liste des titres :
{text}
""",
    "news_bullet_points": lambda text: f"""Tu es un analyste financier senior spécialisé en analyse fondamentale.

Voici un résumé global de l’actualité récente concernant une entreprise cotée :
{text}

🎯 Ta mission :
Identifie uniquement les informations susceptibles d’avoir un impact sur le prix de l’action, comme :
- Résultats financiers
- Recommandations d’analystes
- Dividendes, M&A, spin-offs
- Contrats majeurs, rumeurs, régulations

✍️ Format attendu :
Une liste de bullet points concis, en français, orientés bourse (max 5).

Bullet points :
"""
}

# 🧠 GPT async (nouvelle API)
async def call_openai(task: str, input_text: str) -> str:
    prompt = PROMPT_LIBRARY[task](input_text)
    async with SEMAPHORE:
        try:
            response = await client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "Tu es un analyste financier professionnel."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=400
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"[GPT ERROR] ({task}) → {e}")
            return ""

# 🌍 Traduction
def translate(text):
    try:
        return GoogleTranslator(source='auto', target='en').translate(text)
    except:
        return text

# 🔍 Bullet parsing
def extract_bullets(text):
    lines = text.strip().split("\n")
    return [re.sub(r"^[-•*]\s*", "", l.strip()) for l in lines if l.strip()]

# 📈 FinBERT
def analyze_bullets(bullets):
    try:
        results = finbert(bullets)
        counts = {"POSITIVE": 0, "NEGATIVE": 0, "NEUTRAL": 0}
        for r in results:
            label = r["label"].upper()
            if label in counts:
                counts[label] += 1
        return counts
    except:
        return {"POSITIVE": 0, "NEGATIVE": 0, "NEUTRAL": 0}

def classify_summary(summary):
    try:
        sentences = re.split(r'(?<=[.!?]) +', summary)
        sentences = [s.strip() for s in sentences if len(s.strip()) > 20][:8]
        if not sentences:
            return "NEUTRAL"
        results = finbert(sentences)
        pos = sum(1 for r in results if r["label"].upper() == "POSITIVE")
        neg = sum(1 for r in results if r["label"].upper() == "NEGATIVE")
        return "POSITIVE" if pos > neg else "NEGATIVE" if neg > pos else "NEUTRAL"
    except:
        return "NEUTRAL"

# 🔎 RSS fetch
def get_rss_entries(ticker):
    query = urllib.parse.quote(f"{ticker} stock")
    url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)
    return [{"title": e.title.strip(), "url": e.link.strip()} for e in feed.entries[:20]]

# 🔁 Ticker handler
async def process_ticker(ticker):
    entries = get_rss_entries(ticker)
    if not entries:
        return None

    titles = [e["title"] for e in entries]
    urls = [e["url"] for e in entries]

    try:
        sentiments = finbert(titles, batch_size=8)
    except:
        sentiments = [{"label": "NEUTRAL"}] * len(titles)

    counts = {"POSITIVE": 0, "NEGATIVE": 0, "NEUTRAL": 0}
    for s in sentiments:
        label = s["label"].upper()
        if label in counts:
            counts[label] += 1

    sources = [f"{t} ({u}) → {s['label'].title()}" for t, u, s in zip(titles, urls, sentiments)]

    summary = await call_openai("news_summary_global", "\n".join(titles))
    bullets_raw = await call_openai("news_bullet_points", summary)
    bullets = extract_bullets(bullets_raw)

    translated = translate(summary)
    label = classify_summary(translated)
    bullet_counts = analyze_bullets(bullets)

    total = sum(counts.values())
    score = (counts["POSITIVE"] - counts["NEGATIVE"]) / total if total else None

    return {
        "Ticker": ticker,
        "news_count": total,
        "sentiment_score": round(score, 3) if score else None,
        "positive_ratio": round(counts["POSITIVE"] / total, 2) if total else None,
        "negative_ratio": round(counts["NEGATIVE"] / total, 2) if total else None,
        "neutral_ratio": round(counts["NEUTRAL"] / total, 2) if total else None,
        "gpt_label": label,
        "bullet_positive_count": bullet_counts["POSITIVE"],
        "bullet_negative_count": bullet_counts["NEGATIVE"],
        "source": " / ".join(sources),
        "summary": summary
    }

# 🚀 Main loop
async def main():
    results = []
    summaries_json = {}

    for batch in tqdm([tickers[i:i+20] for i in range(0, len(tickers), 20)], desc="Batches"):
        tasks = [process_ticker(t) for t in batch]
        completed = await asyncio.gather(*tasks)
        for r in completed:
            if r:
                results.append(r)
                summaries_json[r["Ticker"]] = r["summary"]
        await asyncio.sleep(0.5)

    df_out = pd.DataFrame(results)
    df_out.drop(columns=["summary"]).to_csv(CSV_PATH, index=False)
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(summaries_json, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Export CSV : {CSV_PATH}")
    print(f"✅ Export JSON : {JSON_PATH}")

if __name__ == "__main__":
    asyncio.run(main())
