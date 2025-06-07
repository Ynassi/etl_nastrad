import os
import tarfile
from datetime import datetime

def archive_nastrad_daily_snapshot():
    # Format de date
    today_str = datetime.today().strftime("%d-%m-%Y")
    
    # Chemins
    output_base = "output/history"
    archive_dir = os.path.join(output_base, today_str)
    os.makedirs(archive_dir, exist_ok=True)
    
    archive_path = os.path.join(archive_dir, "na_strad_snapshot.tar.gz")
    
    # Fichiers/dossiers à inclure dans l'archive
    targets = [
        "data/df_final_merged.csv",
        "data/fear_greed.json",
        "data/vix.json",
        "data/news.json",
        "data/sector_heatmap.json",
        "data/sector_performance.json",
        "data/sector_volatility.json",
        "data/headline_summary.json",
        "data/news_summaries_full.json",
        "output/insights_enriched_all",
        "output/df_sentiment_full.csv"
    ]
    
    print(f"📦 Création de l'archive : {archive_path}")
    
    # Création de l'archive
    with tarfile.open(archive_path, "w:gz") as tar:
        for target in targets:
            if os.path.exists(target):
                tar.add(target, arcname=os.path.relpath(target))
                print(f"  ✅ Ajouté : {target}")
            else:
                print(f"  ⚠️ Fichier manquant : {target}")

    print(f"✅ Archive complétée : {archive_path}")

if __name__ == "__main__":
    archive_nastrad_daily_snapshot()
