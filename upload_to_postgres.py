import os
import json
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# Fichiers à uploader
TARGETS = [
    "data/df_final_merged.csv",
    "data/fear_greed.json",
    "data/vix.json",
    "data/news.json",
    "data/sector_heatmap.json",
    "data/sector_performance.json",
    "data/sector_volatility.json",
    "data/headline_summary.json",
    "data/news_summaries_full.json",
    "output/df_sentiment_full.csv"
]

# Connexion à PostgreSQL avec SQLAlchemy
def connect_to_db():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL manquant dans le fichier .env")
    print("🔗 Connexion via SQLAlchemy")
    engine = create_engine(db_url)
    return engine

# Upload CSV
def upload_csv(path, engine):
    table_name = Path(path).stem.lower()
    try:
        df = pd.read_csv(path)
        print(f" Upload CSV → {table_name}")
        df.to_sql(table_name, engine, if_exists='replace', index=False, method='multi')
        print(f" CSV uploadé : {table_name}")
    except Exception as e:
        print(f" Erreur CSV {table_name} : {e}")

# Upload JSON (stocké comme table clé/valeur)
def upload_json(path, engine):
    table_name = Path(path).stem.lower()
    try:
        with open(path, "r") as f:
            data = json.load(f)

        if isinstance(data, dict):
            # Convertir les valeurs dict en chaînes JSON si nécessaire
            rows = [{"key": k, "value": json.dumps(v) if isinstance(v, dict) else v} for k, v in data.items()]
            df = pd.DataFrame(rows)
        elif isinstance(data, list):
            df = pd.json_normalize(data)
        else:
            raise ValueError("Format JSON non reconnu")

        print(f" Upload JSON → {table_name}")
        df.to_sql(table_name, engine, if_exists='replace', index=False, method='multi')
        print(f" JSON uploadé : {table_name}")
    except Exception as e:
        print(f" Erreur JSON {table_name} : {e}")

# Lancement global
def upload_all():
    engine = connect_to_db()

    for path in TARGETS:
        if not os.path.exists(path):
            print(f" Fichier introuvable : {path}")
            continue

        if path.endswith(".csv"):
            upload_csv(path, engine)
        elif path.endswith(".json"):
            upload_json(path, engine)
        else:
            print(f" Format non supporté : {path}")

    engine.dispose()
    print("🔒 Connexion fermée.")

if __name__ == "__main__":
    upload_all()
