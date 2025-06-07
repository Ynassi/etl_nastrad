from generate_overview_data import *
from generate_overview_summary import generate_summary, save_summary
from datetime import datetime
import time


def run_full_overview_pipeline():
    print("🚀 Lancement pipeline overview complet...")

    # 1. Génération des données JSON
    print("📊 Étape 1/2 : Génération des données...")
    save_json(get_fear_greed(), "fear_greed")
    save_json(get_vix(), "vix")
    save_csv(get_index_comparison(), "indices")

    heatmap, performance, volatility = get_sector_data_fmp()
    save_json(heatmap, "sector_heatmap")
    save_json(performance, "sector_performance")
    save_json(volatility, "sector_volatility")

    save_json(get_news(), "news")
    save_json(get_sparklines(), "index_sparklines")

    summary = generate_headline_summary(performance)
    save_json({"headline_summary": summary}, "headline_summary")
    save_json({"generated_at": datetime.now().isoformat()}, "generated_at")

    time.sleep(1)  # ⏱️ petite pause sécurité

    # 2. Génération du résumé GPT
    print("🧠 Étape 2/2 : Résumé GPT en cours...")
    text = generate_summary()
    save_summary(text)

    print("✅ Résumé overview généré et synchronisé avec les dernières données.")

if __name__ == "__main__":
    run_full_overview_pipeline()
