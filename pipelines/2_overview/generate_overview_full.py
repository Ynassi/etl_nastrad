import os
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

# 📁 Chemins
BASE_DIR = Path(__file__).resolve().parents[2]  
DATA_FOLDER = BASE_DIR / "data" / "overview"
SUMMARY_FILE = DATA_FOLDER / "headline_summary.json"
os.makedirs(DATA_FOLDER, exist_ok=True)

# 🔐 Clé API
load_dotenv(dotenv_path=BASE_DIR / ".env")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

def load_json(filename):
    path = DATA_FOLDER / filename
    if not path.exists():
        raise FileNotFoundError(f"❌ Fichier {filename} manquant dans /data/overview")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_prompt():
    fear_greed = load_json("fear_greed.json")
    vix = load_json("vix.json")
    spy = load_json("spy_data.json")
    ewq = load_json("ewq_data.json")
    ewj = load_json("ewj_data.json")
    sector_perf = load_json("sector_performance.json")
    sector_vol = load_json("sector_volatility.json")
    sparklines = load_json("index_sparklines.json")
    news = load_json("news.json")

    prompt = (
        "Tu es un analyste financier professionnel. Rédige un **résumé narratif clair, stratégique et synthétique** "
        "de la situation actuelle des marchés, à destination d’un investisseur expérimenté.\n\n"
        "Tu dois répondre à :\n"
        "1. Quel est le **sentiment global** des marchés (calme, peur, euphorie, etc.) ?\n"
        "2. Quels indices mondiaux sont les plus solides aujourd’hui (SPY, EWQ, EWJ) ?\n"
        "3. Quels secteurs sont à privilégier ? Y a-t-il une **volatilité anormale** ?\n\n"
        f"📌 Indicateurs :\n"
        f"- Fear & Greed Index : {fear_greed['score']} ({fear_greed['label']})\n"
        f"- VIX : {vix['value']}\n"
        f"- SPY (S&P 500) : {spy['data'][-1]:.2f} au {spy['labels'][-1]}\n"
        f"- EWQ (CAC 40) : {ewq['data'][-1]:.2f} au {ewq['labels'][-1]}\n"
        f"- EWJ (Nikkei 225) : {ewj['data'][-1]:.2f} au {ewj['labels'][-1]}\n\n"
        "🔥 Top secteurs aujourd’hui (variation 1d) :\n"
    )

    # Performances sectorielles
    top_perf = sorted(sector_perf.items(), key=lambda x: x[1].get("1d", 0) or -999, reverse=True)[:3]
    for sector, values in top_perf:
        prompt += f"- {sector} : {values.get('1d', 0):.2f}%\n"

    # Volatilité sectorielle
    prompt += "\n⚠️ Secteurs les plus volatils :\n"
    top_vol = sorted(sector_vol.items(), key=lambda x: x[1] or -1, reverse=True)[:3]
    for sector, vol in top_vol:
        prompt += f"- {sector} : {vol:.2f}\n"

    # Tendances
    prompt += "\n📉 Tendances des indices (delta approx. sur période) :\n"
    for symbol, values in sparklines.items():
        if isinstance(values, list) and len(values) >= 2:
            delta = values[-1] - values[0]
            tendance = "hausse" if delta > 0 else "baisse"
            prompt += f"- {symbol} : {tendance} ({delta:.2f} points)\n"

    # News
    prompt += "\n📰 Nouvelles économiques :\n"
    for article in news[:3]:
        prompt += f"- {article.get('headline', 'Titre inconnu')} ({article.get('source', 'Source inconnue')})\n"

    prompt += (
        "\n✅ Génère un **paragraphe unique**, fluide, avec un ton analytique, sans phrases vagues ni redondance."
    )

    return prompt

def generate_summary():
    prompt = build_prompt()
    print("⏳ Appel à GPT-3.5 pour génération du résumé...")
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "Tu es un analyste financier expert. Réponds uniquement en français."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.7,
        max_tokens=350
    )
    return response.choices[0].message.content.strip()

def save_summary(text):
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.utcnow().isoformat(),
            "summary": text
        }, f, indent=2, ensure_ascii=False)
    print(f"✅ Résumé sauvegardé dans {SUMMARY_FILE.name}")

def main():
    try:
        summary = generate_summary()
        save_summary(summary)
    except Exception as e:
        print(f"❌ Erreur génération résumé : {e}")

if __name__ == "__main__":
    main()
