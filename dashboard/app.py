import streamlit as st
import pandas as pd
import duckdb
import os
import sys

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
    """Charge les données depuis DuckDB (modèles dbt)."""
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "meteo.duckdb")

    if not os.path.exists(db_path):
        return None

    try:
        conn = duckdb.connect(db_path, read_only=True)
        df = conn.execute("""
            SELECT
                city_name   AS city,
                observed_at AS date,
                temperature_max  AS temp_max,
                temperature_min  AS temp_min,
                precipitation_mm AS precipitation_sum
            FROM stg_weather_history
            WHERE city_name IS NOT NULL
            ORDER BY observed_at
        """).df()
        conn.close()

        if df.empty:
            return None

        df["date"] = pd.to_datetime(df["date"])
        df = df.drop_duplicates(subset=["city", "date"])
        df = df.sort_values("date")

        for col in ["temp_max", "temp_min", "precipitation_sum"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    except Exception as e:
        st.error(f"Erreur DuckDB : {e}")
        return None


def donnees_fictives():
    """Génère des données fictives si DuckDB n'est pas disponible."""
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


# ─── Interface ───────────────────────────────────────────────

st.title("🌤️ Dashboard Météo — Paris, Lyon, Marseille")
st.markdown("Pipeline ELT automatisée • Open-Meteo API • Dagster + DuckDB + dbt")
st.divider()

df = load_data()

if df is None:
    st.warning("⚠️ Aucune donnée dans DuckDB. Affichage avec données fictives.")
    df = donnees_fictives()
    st.info("💡 Lance le pipeline Dagster pour voir les vraies données : `dagster dev`")
else:
    st.success(f"✅ Données chargées depuis DuckDB — {len(df)} enregistrements")

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

# ─── Filtrage ────────────────────────────────────────────────

df_filtered = df[df["city"].isin(villes_select)]
if len(periode) == 2:
    df_filtered = df_filtered[
        (df_filtered["date"].dt.date >= periode[0]) &
        (df_filtered["date"].dt.date <= periode[1])
    ]

# ─── KPIs ────────────────────────────────────────────────────

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

# ─── Graphiques ──────────────────────────────────────────────

st.plotly_chart(chart_temperatures(df_filtered), use_container_width=True)

col_left, col_right = st.columns(2)
with col_left:
    st.plotly_chart(chart_precipitations(df_filtered), use_container_width=True)
with col_right:
    st.plotly_chart(chart_amplitude_thermique(df_filtered), use_container_width=True)

st.plotly_chart(chart_comparaison_moyennes(df_filtered), use_container_width=True)

with st.expander("🔍 Voir les données brutes"):
    st.dataframe(df_filtered.sort_values(["city", "date"]), use_container_width=True)