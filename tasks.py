import subprocess
import time
from tqdm import tqdm
import sys
import os
import argparse

# 📁 Se positionner dans le dossier racine du projet
os.chdir(os.path.dirname(__file__))

PIPELINES = [
    {"name": " 1️⃣ ETL Indices majeurs (S&P500, CAC40, Nikkei)", "script": "pipelines/1_companies/etl_pipeline.py", "steps": 14},
    {"name": " 2️⃣ Enrichissement Small Caps", "script": "pipelines/1_companies/enrich_etl.py", "steps": 2},
    {"name": " 3️⃣ Fusion finale des données", "script": "pipelines/1_companies/merge_uniform.py", "steps": 2},

    # ✅ Changement d'ordre ici
    {"name": " 4️⃣ Index Data", "script": "pipelines/2_overview/Index_data.py", "steps": 2},
    {"name": " 5️⃣ Données Overview Générales", "script": "pipelines/2_overview/generate_overview_full.py", "steps": 5},

    {"name": " 6️⃣ Enrichissement Compagnies", "script": "pipelines/3_enrich_companies/enrich_companies.py", "steps": 3},
    {"name": " 7️⃣ Raffinement Compagnies", "script": "pipelines/3_enrich_companies/refine_companies.py", "steps": 2},

    {"name": " 8️⃣ Analyse News (GPT)", "script": "pipelines/4_sentiment/enrich_sent_gpt.py", "steps": 3},
    {"name": " 9️⃣ Fusion News GPT", "script": "pipelines/4_sentiment/merge_news_gpt.py", "steps": 2},
    {"name": " 🔟 Nettoyage JSON", "script": "pipelines/6_final/clean_json_files.py", "steps": 1},

    {"name": " 🔁 Génération df_sentiment_full", "script": "pipelines/4_sentiment/generate_df_sentiment_full.py", "steps": 2},
    {"name": " 📦 Archivage Snapshot", "script": "pipelines/6_final/archive_daily_snapshot.py", "steps": 1}
]

def run_pipeline_with_progress(pipeline):
    name = pipeline["name"]
    script = pipeline["script"]
    steps = pipeline["steps"]
    step_percent = 100 // steps

    print(f"\n🔧 Démarrage de : {name}")
    progress_bar = tqdm(total=100, desc=name, bar_format="{l_bar}{bar}| {n_fmt}%", ncols=100)

    process = subprocess.Popen(
        ["python", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True
    )

    progress = 0
    for line in process.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        if any(kw in line.lower() for kw in [
            "étape", "step", "extraction", "nettoyage", "short", "midterm", "résumé", "enrich",
            "fiche", "json", "lecture", "sauvegarde", "df", "csv", "chargement"
        ]):
            progress = min(progress + step_percent, 100)
            progress_bar.n = progress
            progress_bar.refresh()

    process.stdout.close()
    returncode = process.wait()
    progress_bar.n = 100
    progress_bar.refresh()
    progress_bar.close()

    if returncode == 0:
        print(f"{name} terminé avec succès.")
        return True
    else:
        print(f"Erreur pendant l'exécution de {name}. Code de retour : {returncode}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-from", type=int, default=0, help="Indice du pipeline à partir duquel reprendre l'exécution (ex: 5 pour Index Data)")
    args = parser.parse_args()

    print(f"\n⏯️ Reprise des pipelines à partir de l'étape {args.start_from}: {PIPELINES[args.start_from]['name']}\n")

    for i in range(args.start_from, len(PIPELINES)):
        if not run_pipeline_with_progress(PIPELINES[i]):
            print(f"❌ Pipeline stoppé à l'étape {i}: {PIPELINES[i]['name']}")
            break
