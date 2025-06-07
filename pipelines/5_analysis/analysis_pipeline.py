import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.manifold import TSNE
from sklearn.cluster import KMeans
import sqlite3
from datetime import datetime
import plotly.express as px
from openai import OpenAI
from dotenv import load_dotenv
from pathlib import Path

# Chargement sécurisé du .env
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

# Chargement des données

def load_data():
    df = pd.read_csv("data/df_final_merged.csv")
    df['ExtractionDate'] = datetime.today().strftime('%Y-%m-%d')
    df['Ticker'] = df['Ticker'].astype(str).str.upper().str.strip()
    df['CapType'] = df['IndexSource'].apply(lambda x: 'BigCap' if x in ['SP500', 'CAC40', 'Nikkei225'] else 'SmallCap')
    return df

df_all = load_data()

# Normalisation
scaler = MinMaxScaler()
df_all[['ValueScore', 'SignalScore']] = scaler.fit_transform(df_all[['ValueScore', 'SignalScore']])

# Filtrage progressif

def filtre_progressif(df, col, seuil_init, direction=">", step=0.01, min_remaining=5, max_iter=10):
    seuil = seuil_init
    for i in range(max_iter):
        if direction == ">":
            subset = df[df[col] > seuil]
        elif direction == "<":
            subset = df[df[col] < seuil]
        elif direction == "between":
            low, high = seuil
            subset = df[df[col].between(low, high)]
        else:
            raise ValueError("Direction invalide")

        if len(subset) >= min_remaining:
            print(f"✅ {col} seuil ajusté à {seuil} ({len(subset)} entreprises restantes)")
            return subset, seuil

        if direction == ">":
            seuil -= step
        elif direction == "<":
            seuil += step
        elif direction == "between":
            low, high = seuil
            seuil = (low - step, high + step)

    print(f"⚠️ {col} seuil final à {seuil} ({len(subset)} entreprises seulement)")
    return subset, seuil

# Pipelines

def pipeline_filtrage_progressif_short(df):
    print("\n⚙️ Filtrage progressif : SHORT candidates")
    df_filtered, _ = filtre_progressif(df, "RSI_14", 62, direction=">", step=1)
    df_filtered, _ = filtre_progressif(df_filtered, "Momentum_10", 16, direction=">", step=0.5)
    df_filtered, _ = filtre_progressif(df_filtered, "ValueScore", 0.76, direction=">", step=0.01)
    df_filtered, _ = filtre_progressif(df_filtered, "SignalScore", 0.44, direction="<", step=0.01)
    df_filtered, _ = filtre_progressif(df_filtered, "Volatility", 0.021, direction=">", step=0.001)
    df_filtered, _ = filtre_progressif(df_filtered, "Beta", 1.1, direction=">", step=0.05)
    df_filtered, _ = filtre_progressif(df_filtered, "PB", 3.5, direction=">", step=0.2)
    return df_filtered.sort_values(by=["Momentum_10", "RSI_14", "Volatility"], ascending=[False, False, False]).head(15)

def pipeline_filtrage_progressif_midterm(df):
    print("\n⚙️ Filtrage progressif : MIDTERM buys")
    df_filtered, _ = filtre_progressif(df, "ValueScore", 0.78, direction=">", step=0.01)
    df_filtered, _ = filtre_progressif(df_filtered, "QualityScore", 0.42, direction=">", step=0.01)
    df_filtered, _ = filtre_progressif(df_filtered, "PE", 30, direction="<", step=1)
    df_filtered, _ = filtre_progressif(df_filtered, "ROE", 0.19, direction=">", step=0.01)
    df_filtered, _ = filtre_progressif(df_filtered, "ProfitMargin", 0.14, direction=">", step=0.01)
    df_filtered, _ = filtre_progressif(df_filtered, "GrossMargin", 0.36, direction=">", step=0.01)
    df_filtered, _ = filtre_progressif(df_filtered, "Volatility", 0.023, direction="<", step=0.001)
    df_filtered, _ = filtre_progressif(df_filtered, "Beta", 1.02, direction="<", step=0.05)
    df_filtered, _ = filtre_progressif(df_filtered, "RSI_14", (45, 61), direction="between", step=1)
    return df_filtered.sort_values(by=["ValueScore", "QualityScore", "ROE"], ascending=[False, False, False]).head(15)

def pipeline_filtrage_progressif_shortterm(df):
    print("\n⚙️ Filtrage progressif : SHORT TERM opps")
    df_filtered, _ = filtre_progressif(df, "Momentum_10", 6.3, direction=">", step=0.3)
    df_filtered, _ = filtre_progressif(df_filtered, "MACD", -0.01, direction=">", step=0.05)
    df_filtered, _ = filtre_progressif(df_filtered, "SignalScore", 0.46, direction=">", step=0.01)
    df_filtered, _ = filtre_progressif(df_filtered, "PE", 29, direction="<", step=1)
    df_filtered, _ = filtre_progressif(df_filtered, "RSI_14", (45, 63), direction="between", step=1)
    df_filtered, _ = filtre_progressif(df_filtered, "BB_Percent", 0.72, direction=">", step=0.01)
    return df_filtered.sort_values(by=["SignalScore", "Momentum_10", "MACD"], ascending=[False, False, False]).head(15)

# Application aux deux sous-univers
for cap_type in ['BigCap', 'SmallCap']:
    df_sub = df_all[df_all['CapType'] == cap_type].copy()
    short = pipeline_filtrage_progressif_short(df_sub)
    mid = pipeline_filtrage_progressif_midterm(df_sub)
    shortterm = pipeline_filtrage_progressif_shortterm(df_sub)
    short.to_csv(f"data/list_short_{cap_type.lower()}.csv", index=False)
    mid.to_csv(f"data/list_midterm_{cap_type.lower()}.csv", index=False)
    shortterm.to_csv(f"data/list_shortterm_{cap_type.lower()}.csv", index=False)

# Clustering global
features = ['ValueScore', 'QualityScore', 'SignalScore', 'RSI_14', 'Momentum_10', 'MACD', 'Volatility', 'Beta']
df_clust = df_all.dropna(subset=features).copy()
X_scaled = StandardScaler().fit_transform(df_clust[features])
df_clust['Cluster_KMeans'] = KMeans(n_clusters=5, random_state=42).fit_predict(X_scaled)
tsne = TSNE(n_components=2, perplexity=30, random_state=42)
X_tsne = tsne.fit_transform(X_scaled)
df_clust['TSNE1'] = X_tsne[:, 0]
df_clust['TSNE2'] = X_tsne[:, 1]
df_kmeans = df_clust.copy()
df_kmeans['Cluster'] = df_kmeans['Cluster_KMeans']
df_kmeans.to_csv("data/df_kmeans_clusters.csv", index=False)

# Résumé GPT
try:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    cluster_summary = df_kmeans.groupby("Cluster")[features].mean().round(3)
    prompt = f"""
    Tu es un analyste quantitatif expert en finance.
    Voici les moyennes des principales métriques par cluster d'entreprises :
    {cluster_summary.to_string()}
    Merci de produire un résumé structuré et clair pour un dashboard d’analyse.
    """
    chat_completion = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "Tu es un expert en analyse financière."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.5,
        max_tokens=1000,
    )
    with open("output/cluster_descriptions.txt", "w", encoding="utf-8") as f:
        f.write(chat_completion.choices[0].message.content)
    print("✅ Résumé des clusters généré par OpenAI et sauvegardé dans 'output/cluster_descriptions.txt'.")
except Exception as e:
    print(f"⚠️ Erreur GPT : {e}")
