# 🌤️ Meteo Project - Pipeline ELT Météo Automatisée

## 📋 Vue d'ensemble

Ce projet implémente une pipeline de données de bout en bout permettant de comparer les températures (max/min) et les précipitations de trois villes françaises (**Paris, Lyon, Marseille**). L'objectif est de faciliter l'analyse des tendances climatiques locales via un processus ELT (Extract, Load, Transform) robuste et orchestré.

```
┌─────────────────────────────────────────────────────────────┐
│                   🌤️ METEO PROJECT                    │
│                                                         │
│  🌍 Extraction → 🦆 Loading → 📊 Analytics     │
│                                                         │
└─────────────────────────────────────────────────────────────┘
```

## 🏗️ Architecture du projet

```
Meteo Project/
├── 📁 extract/                    # Extraction API météo
│   ├── weather_extractor.py     # Classe WeatherExtractor
│   ├── config.py              # Configuration API
│   └── __init__.py
├── 📁 data/                      # Données brutes
│   ├── duckdb/               # Base MotherDuck
│   └── weather_data_*.csv    # Fichiers CSV générés
├── 📁 dagster_weather/           # Orchestration ELT
│   ├── dagster_weather/
│   │   ├── assets.py      # Assets Dagster ✅
│   │   ├── definitions.py  # Définitions
│   │   ├── config.py      # Configuration
│   │   └── __init__.py
│   └── requirements.txt
├── 📁 .env                       # Variables d'environnement
└── 📄 README.md                  # Documentation
```

## 🚀 Fonctionnalités

### ✅ Extraction des données
- **API Open-Meteo Historical** : Récupération des données météo historiques précises
- **Villes supportées** : Paris, Lyon, Marseille (configurable)
- **Données extraites** : Température max/min, précipitations, etc.
- **Gestion d'erreurs** : Retry et logging complet

### ✅ Chargement des données
- **MotherDuck** : Base de données DuckDB cloud
- **Sauvegarde CSV** : Fichiers avec timestamp unique
- **Chemins absolus** : Résolution des problèmes de chemins
- **Validation** : Vérification des données avant chargement

### ✅ Orchestration Dagster
- Assets : `raw_weather_data` → `load_weather_to_duckdb`
- **Dépendances** : Liaison automatique entre assets
- **Monitoring** : Logs détaillés et métadonnées
- **Exécution parallèle** : Support du multiprocess

## 🛠️ Installation

### Prérequis
```bash
# Python 3.12+
pip install -r dagster_weather/requirements.txt
```

### Lancement du pipeline
```bash
# Navigation vers le projet
cd "votre directory_path_to\Meteo Project"

# Exécution des assets Dagster
dagster asset materialize --select raw_weather_data,load_weather_to_duckdb

# Lancement UI Dagster (quand disponible)
dagster dev
```

## 📊 Données générées

### Structure des fichiers CSV
```csv
city,date,temperature,temp_min,temp_max,humidity,pressure,weather_description,wind_speed
Paris,2026-04-18,15.2,12.1,18.5,65,1015,nuageux léger,3.5
Lyon,2026-04-18,13.8,10.2,17.4,72,1012,partiellement nuageux,2.8
Marseille,2026-04-18,16.5,14.0,19.0,68,1018,nuageux,4.2
```

### Métadonnées Dagster
- **records_count** : Nombre d'enregistrements traités
- **cities** : Liste des villes uniques
- **date** : Timestamp d'extraction
- **preview** : Aperçu Markdown des données
- **csv_file_path** : Chemin du fichier CSV généré

## 🔧 Configuration

### Variables d'environnement
```bash
MOTHERDUCK_TOKEN=votre_token_motherduck
PYTHONLEGACYWINDOWSSTDIO=1
```

### Fichiers de configuration
- **`extract/config.py`** : Villes, URLs API, jours historiques
- **`dagster_weather/config.py`** : Configuration Dagster

## 🐛 Dépannage et erreurs rencontrées

Voir [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) pour :
- Erreurs d'importation Python
- Problèmes de dépendances Dagster
- Conflits de versions Dagster
- Résolutions des chemins relatifs

## 🚀 Prochaines étapes

1. **Jobs & Schedules Dagster** - Automatisation quotidienne
2. **Models dbt** - Transformations staging/mart
3. **Tests de validation** - Qualité des données
4. **Docker** - Déploiement conteneurisé
5. **Monitoring** - Alertes et dashboards

## 📈 Monitoring et logs

### Logs Dagster
```bash
# Logs d'exécution des assets
dagster asset materialize --select extract_weather_data

# Logs détaillés avec timestamps
INFO:dagster - Successfully extracted 3 weather records
INFO:weather_extractor - Extracted data for 3 cities
```

### Métriques
- **Temps d'extraction** : ~1.5 secondes
- **Temps de chargement** : ~1.3 secondes  
- **Débit** : 3 villes par exécution
- **Taux de succès** : 100% (actuellement)

## 👥 Contributing

1. Fork du projet
2. Création d'une branche feature
3. Tests locaux avec `dagster asset materialize`
4. Pull request avec description des changements

## 📄 Licence

Projet de data engineering pour démonstration technique.

---

**🌤️ Meteo Project - Pipeline ELT opérationnel et prêt pour la production !**