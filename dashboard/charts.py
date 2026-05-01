import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


COULEURS_VILLES = {
    "Paris": "#636EFA",
    "Lyon": "#EF553B",
    "Marseille": "#00CC96",
}


def chart_temperatures(df: pd.DataFrame) -> go.Figure:
    """Courbe des températures max et min par ville sur 30 jours."""
    fig = go.Figure()

    for ville in df["city"].unique():
        df_ville = df[df["city"] == ville].sort_values("date")
        couleur = COULEURS_VILLES.get(ville, "#888")

        fig.add_trace(go.Scatter(
            x=df_ville["date"],
            y=df_ville["temp_max"],
            name=f"{ville} - Max",
            line=dict(color=couleur, width=2),
            mode="lines+markers",
        ))
        fig.add_trace(go.Scatter(
            x=df_ville["date"],
            y=df_ville["temp_min"],
            name=f"{ville} - Min",
            line=dict(color=couleur, width=1, dash="dot"),
            mode="lines+markers",
            opacity=0.6,
        ))

    fig.update_layout(
        title="🌡️ Températures Max / Min par ville (30 derniers jours)",
        xaxis_title="Date",
        yaxis_title="Température (°C)",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        template="plotly_dark",
    )
    return fig


def chart_precipitations(df: pd.DataFrame) -> go.Figure:
    """Histogramme des précipitations cumulées par ville."""
    fig = px.bar(
        df.sort_values("date"),
        x="date",
        y="precipitation_sum",
        color="city",
        color_discrete_map=COULEURS_VILLES,
        barmode="group",
        title="🌧️ Précipitations journalières par ville (mm)",
        labels={"precipitation_sum": "Précipitations (mm)", "date": "Date", "city": "Ville"},
        template="plotly_dark",
    )
    return fig


def chart_amplitude_thermique(df: pd.DataFrame) -> go.Figure:
    """Amplitude thermique (temp_max - temp_min) par ville."""
    df = df.copy()
    df["amplitude"] = df["temp_max"] - df["temp_min"]

    fig = px.line(
        df.sort_values("date"),
        x="date",
        y="amplitude",
        color="city",
        color_discrete_map=COULEURS_VILLES,
        title="📊 Amplitude thermique journalière par ville (°C)",
        labels={"amplitude": "Amplitude (°C)", "date": "Date", "city": "Ville"},
        markers=True,
        template="plotly_dark",
    )
    fig.add_hline(
        y=15,
        line_dash="dash",
        line_color="orange",
        annotation_text="Seuil anomalie (15°C)",
    )
    return fig


def chart_comparaison_moyennes(df: pd.DataFrame) -> go.Figure:
    """Tableau comparatif des moyennes par ville."""
    stats = df.groupby("city").agg(
        temp_max_moy=("temp_max", "mean"),
        temp_min_moy=("temp_min", "mean"),
        precipitation_totale=("precipitation_sum", "sum"),
    ).reset_index().round(1)

    fig = go.Figure(data=[go.Table(
        header=dict(
            values=["🏙️ Ville", "🌡️ Temp Max Moy (°C)", "🌡️ Temp Min Moy (°C)", "🌧️ Précip Totale (mm)"],
            fill_color="#1f2937",
            font=dict(color="white", size=13),
            align="center",
        ),
        cells=dict(
            values=[
                stats["city"],
                stats["temp_max_moy"],
                stats["temp_min_moy"],
                stats["precipitation_totale"],
            ],
            fill_color="#111827",
            font=dict(color="white", size=12),
            align="center",
        )
    )])
    fig.update_layout(title="📋 Résumé statistique par ville", template="plotly_dark")
    return fig