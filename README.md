# 🌤️ Meteo Project — Pipeline ELT Météo Automatisée

> Projet Data Engineering — Groupe Lyon
> **Membres :** Saer DIENE, Ibrahima NDAW, Mame Astou MBACKE

---

## 📋 Vue d'ensemble

Pipeline de données ELT complète et automatisée pour comparer les températures et précipitations de **Paris, Lyon et Marseille** sur les 30 derniers jours.

```
🌍 Open-Meteo API
       ↓  Extract
🐍 Python (WeatherExtractor + Pydantic)
       ↓  Load
🦆 DuckDB local (Schéma en étoile)
       ↓  Transform
📦 dbt (Staging → Marts)
       ↓  Orchestrate
⚙️  Dagster (Assets, Jobs, Schedule, Sensors, Partitions)
       ↓  Visualise
📊 Streamlit Dashboard
```

---

## 🏗️ Architecture du projet

```
meteo_project/
├── 📁 extract/                  # Extraction API Open-Meteo
│   ├── weather_extractor.py    # Classe WeatherExtractor
│   └── __init__.py
├── 📁 data/                     # Données brutes générées
│   └── weather_data_*.csv
├── 📁 dagster_weather/          # Orchestration Dagster
│   └── dagster_weather/
│       ├── assets.py           # Assets : raw → load → dbt
│       ├── definitions.py      # Définitions Dagster
│       ├── config_jobs.py      # Jobs, Schedules
│       ├── sensors.py          # Sensors CSV + failure
│       └── __init__.py
├── 📁 dbt_meteo/                # Transformations dbt
│   ├── models/
│   │   ├── staging/            # stg_weather_history
│   │   └── marts/              # 5 modèles analytiques
│   ├── snapshots/              # snp_cities (SCD Type 2)
│   └── profiles.yml
├── 📁 validation/               # Validation Pydantic
│   ├── schemas.py              # WeatherRecord, WeatherBatch
│   └── validator.py
├── 📁 tests/                    # Tests pytest
│   ├── test_weather_extractor.py
│   ├── test_data_quality.py
│   └── test_validation.py
├── 📁 dashboard/                # Dashboard Streamlit
│   ├── app.py
│   └── charts.py
├── 📁 docker/
│   └── Dockerfile
├── conftest.py
├── docker-compose.yml
└── README.md
```

---

## 🚀 Installation et lancement

### 1. Cloner et installer

```bash
git clone https://github.com/lambert-19/meteo_project.git
cd meteo_project

python3 -m venv venv
source venv/bin/activate

pip install dagster dagster-webserver dagster-dbt dbt-duckdb duckdb==1.0.0 \
    pandas requests python-dotenv pydantic streamlit plotly pytest
```

### 2. Lancer Dagster (pipeline ELT)

```bash
dagster dev -m dagster_weather.dagster_weather
```

Ouvrir **http://localhost:3000** puis lancer le job `weather_pipeline_job`.

### 3. Lancer le Dashboard

```bash
streamlit run dashboard/app.py
```

Ouvrir **http://localhost:8501**

### 4. Lancer les tests

```bash
pytest tests/ -v
```

### 5. Lancer avec Docker (bonus)

```bash
docker-compose up --build
```

Dagster accessible sur **http://localhost:3000**

---

## ⚙️ Pipeline Dagster — Assets

| Asset | Rôle |
|---|---|
| `raw_weather_data` | Extraction API Open-Meteo → CSV |
| `load_weather_to_duckdb` | Chargement CSV → DuckDB local |
| `dbt_meteo_assets` | Transformations dbt (staging + marts) |

- **Schedule** : Exécution automatique chaque jour à 9h00
- **Sensors** : Détection de fichiers CSV + alertes en cas d'échec du pipeline
- **Partitions** : Quotidiennes depuis 2024-01-01 — permet le Backfill ciblé par date

---

## 🦆 Modèle de données DuckDB

### Schéma en étoile

```
dim_cities ──┐
             ├── fct_weather_history
dim_calendar ┘
```

| Table | Type | Contenu |
|---|---|---|
| `dim_cities` | Dimension | Ville, latitude, longitude, altitude, région |
| `dim_calendar` | Dimension | Date, jour, mois, année |
| `fct_weather_history` | Faits | temp_max, temp_min, precipitation_sum |

---

## 📦 Modèles dbt

| Couche | Modèle | Rôle |
|---|---|---|
| Staging | `stg_weather_history` | Renommage, typage, nettoyage des colonnes |
| Mart | `mart_city_frost_days` | Jours de gel par ville (temp_min < 0°C) |
| Mart | `mart_city_heatwaves` | Épisodes de vagues de chaleur |
| Mart | `mart_city_thermal_amplitude` | Amplitude thermique journalière |
| Mart | `mart_city_weather_records` | Records de température par ville |
| Mart | `mart_regional_precipitation_comparison` | Comparaison des précipitations par région |
| Snapshot | `snp_cities` | Historisation SCD Type 2 des villes |

---

## 🧪 Tests et Qualité des données

```bash
pytest tests/ -v
# Résultat : 23 passed, 1 xfailed
```

| Fichier | Ce qui est testé |
|---|---|
| `test_weather_extractor.py` | temp_max > temp_min, bornes -30/50°C, structure, format date YYYY-MM-DD |
| `test_data_quality.py` | Détection anomalies, doublons, volume batch 90 enregistrements |
| `test_validation.py` | Schémas Pydantic : rejets aberrants, batch vide, doublons city+date |

### Validation Pydantic

Chaque enregistrement JSON de l'API est validé avant ingestion :
- Température dans les bornes -30°C / 60°C
- temp_max toujours supérieure à temp_min
- Précipitations non négatives et inférieures à 300mm
- Coordonnées GPS valides
- Aucun champ null

---

## 📊 Dashboard Streamlit

- Températures Max/Min par ville sur 30 jours
- Précipitations journalières comparées entre villes
- Amplitude thermique avec seuil d'anomalie à 15°C
- Tableau récapitulatif des moyennes et totaux
- Filtres dynamiques par ville et période

---

## 🐳 Conteneurisation Docker

```bash
docker-compose up --build
```

Le conteneur embarque Dagster, dbt, DuckDB et toutes les dépendances. Les données sont persistées via un volume Docker monté sur `./data`.

> Note : DuckDB n'acceptant qu'un seul writer simultané, des conflits peuvent apparaître si plusieurs runs se lancent en parallèle. En production, PostgreSQL ou MotherDuck seraient préférés.

---

## ⚠️ Risques et atténuations

| Risque | Atténuation |
|---|---|
| Valeurs nulles ou aberrantes API | Tests dbt `not_null` + validation Pydantic avant ingestion |
| Décalage UTC / heure locale | Normalisation des timestamps dans la couche staging |
| Breaking change API Open-Meteo | Schéma Pydantic bloque l'ingestion si structure JSON change |
| Concurrence DuckDB | Un seul writer à la fois — PostgreSQL recommandé en production |
| Volume snapshot trop grand | Limité aux 30 derniers jours glissants |

---

## 🔁 Plan B

- **Coupure API** : Fichiers CSV de backup dans `/data/backup` pour valider les transformations dbt
- **Snapshot complexe** : Historisation simple par date (insert overwrite) pour garantir la livraison

---

## 📈 Métriques du pipeline

- **Temps d'extraction** : ~1 seconde pour 3 villes
- **Temps de chargement** : ~80ms dans DuckDB
- **Temps dbt** : ~300ms pour tous les modèles
- **Tests** : 23 passed, 1 xfailed en ~1 seconde
- **Taux de succès** : 100% sur les runs locaux

---


**🌤️ Meteo Project — Pipeline ELT opérationnelle et prête pour la production !**


 Repository github: https://github.com/lambert-19/meteo_project.git