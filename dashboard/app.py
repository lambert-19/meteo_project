import streamlit as st
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dashboard.charts import (
    chart_temperatures,
    chart_precipitations,
    chart_amplitude_thermique,
    chart_comparaison_moyennes,
)


st.set_page_config(
    page_title="Météo Dashboard",
    page_icon="🌤️",
    layout="wide",
)


@st.cache_data
def load_data():
    """Charge les CSV générés par le pipeline Dagster."""
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    fichiers = [
        os.path.join(data_dir, f)
        for f in os.listdir(data_dir)
        if f.endswith(".csv") and f.startswith("weather_data")
    ]

    if not fichiers:
        return None

    dfs = []
    for f in fichiers:
        try:
            dfs.append(pd.read_csv(f))
        except Exception:
            continue

    if not dfs:
        return None

    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(subset=["city", "date"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    rename_map = {
        "temperature": "temp_avg",
        "temperature_2m_max": "temp_max",
        "temperature_2m_min": "temp_min",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    for col in ["temp_max", "temp_min", "precipitation_sum"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def donnees_fictives():
    """Génère des données fictives si aucun CSV n'est disponible."""
    import numpy as np
    import datetime

    villes = ["Paris", "Lyon", "Marseille"]
    dates = pd.date_range(end=datetime.date.today(), periods=30)
    records = []

    base_temps = {"Paris": (14, 8), "Lyon": (16, 9), "Marseille": (19, 12)}

    for ville in villes:
        moy_max, moy_min = base_temps[ville]
        for date in dates:
            temp_max = round(moy_max + np.random.uniform(-3, 3), 1)
            temp_min = round(moy_min + np.random.uniform(-2, 2), 1)
            records.append({
                "city": ville,
                "date": date,
                "temp_max": temp_max,
                "temp_min": temp_min,
                "precipitation_sum": round(max(0, np.random.uniform(-2, 10)), 1),
            })

    return pd.DataFrame(records)


st.title("🌤️ Dashboard Météo — Paris, Lyon, Marseille")
st.markdown("Pipeline ELT automatisée • Open-Meteo API • Dagster + DuckDB + dbt")
st.divider()

df = load_data()

if df is None:
    st.warning("⚠️ Aucun fichier CSV trouvé dans `/data`. Affichage avec données fictives.")
    df = donnees_fictives()
    st.info("💡 Lance le pipeline Dagster pour voir les vraies données : `dagster dev`")

# ─── Filtres sidebar ─────────────────────────────────────────

with st.sidebar:
    st.header("🔧 Filtres")

    villes_dispo = sorted(df["city"].unique().tolist())
    villes_select = st.multiselect(
        "Villes",
        options=villes_dispo,
        default=villes_dispo,
    )

    date_min = df["date"].min().date()
    date_max = df["date"].max().date()
    periode = st.date_input(
        "Période",
        value=(date_min, date_max),
        min_value=date_min,
        max_value=date_max,
    )

    st.divider()
    st.markdown("**Projet :** Pipeline ELT Météo")
    st.markdown("**API :** Open-Meteo Historical")
    st.markdown("**Stack :** Dagster · DuckDB · dbt")


df_filtered = df[df["city"].isin(villes_select)]
if len(periode) == 2:
    df_filtered = df_filtered[
        (df_filtered["date"].dt.date >= periode[0]) &
        (df_filtered["date"].dt.date <= periode[1])
    ]


col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("🌡️ Temp Max Moy", f"{df_filtered['temp_max'].mean():.1f}°C")
with col2:
    st.metric("🌡️ Temp Min Moy", f"{df_filtered['temp_min'].mean():.1f}°C")
with col3:
    st.metric("🌧️ Précip Totale", f"{df_filtered['precipitation_sum'].sum():.1f} mm")
with col4:
    nb_jours = df_filtered["date"].nunique()
    st.metric("📅 Jours analysés", nb_jours)

st.divider()

st.plotly_chart(chart_temperatures(df_filtered), use_container_width=True)

col_left, col_right = st.columns(2)
with col_left:
    st.plotly_chart(chart_precipitations(df_filtered), use_container_width=True)
with col_right:
    st.plotly_chart(chart_amplitude_thermique(df_filtered), use_container_width=True)

st.plotly_chart(chart_comparaison_moyennes(df_filtered), use_container_width=True)

with st.expander("🔍 Voir les données brutes"):
    st.dataframe(df_filtered.sort_values(["city", "date"]), use_container_width=True)