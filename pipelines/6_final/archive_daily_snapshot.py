import os
import tarfile
from datetime import datetime

def archive_nastrad_daily_snapshot():
    # Format de date du jour
    today_str = datetime.today().strftime("%d-%m-%Y")

    # Base du projet = 2 niveaux au-dessus du script
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    # Dossier de sortie pour archives
    output_base = os.path.join(BASE_DIR, "output", "history")
    archive_dir = os.path.join(output_base, today_str)
    os.makedirs(archive_dir, exist_ok=True)

    archive_path = os.path.join(archive_dir, "na_strad_snapshot.tar.gz")

    # Fichiers et dossiers à inclure dans l’archive
    targets = [
        os.path.join(BASE_DIR, "data", "df_final_merged.csv"),
        os.path.join(BASE_DIR, "data", "overview", "fear_greed.json"),
        os.path.join(BASE_DIR, "data", "overview", "vix.json"),
        os.path.join(BASE_DIR, "data", "overview", "news.json"),
        os.path.join(BASE_DIR, "data", "overview", "sector_heatmap.json"),
        os.path.join(BASE_DIR, "data", "overview", "sector_performance.json"),
        os.path.join(BASE_DIR, "data", "overview", "sector_volatility.json"),
        os.path.join(BASE_DIR, "data", "overview", "headline_summary.json"),
        os.path.join(BASE_DIR, "data", "news_summaries_full.json"),
        os.path.join(BASE_DIR, "output", "insights_enriched_all"),
        os.path.join(BASE_DIR, "data", "df_sentiment_full.csv"),
    ]

    print(f"Création de l'archive : {archive_path}")

    with tarfile.open(archive_path, "w:gz") as tar:
        for target in targets:
            if os.path.exists(target):
                # Relatif à la racine pour archive propre
                arcname = os.path.relpath(target, start=BASE_DIR)
                tar.add(target, arcname=arcname)
                print(f"  ✅ Ajouté : {arcname}")
            else:
                print(f"  ⚠️ Fichier manquant : {target}")

    print(f" Archive complétée : {archive_path}")

if __name__ == "__main__":
    archive_nastrad_daily_snapshot()
