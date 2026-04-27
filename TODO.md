# 📋 État d'Avancement et Roadmap du Projet Météo

Ce document récapitule les étapes accomplies et les tâches restantes pour finaliser la pipeline ELT automatisée.

## ✅ Étapes Accomplies

### 1. Conception et Infrastructure
- [x] Choix de l'API : **Open-Meteo Historical** (Gratuit, stable, pas de clé).
- [x] Stack Technique : Dagster, DuckDB, Pandas, Docker.
- [x] Structure du Repo : Architecture modulaire (`extract/`, `data/`, `dagster_weather/`).

### 2. Extraction (Extract)
- [x] Développement de `WeatherExtractor` compatible avec Open-Meteo.
- [x] Gestion des coordonnées GPS, altitude et régions pour Paris, Lyon et Marseille.
- [x] Extraction basée sur les **Partitions Dagster** (permet de cibler une date précise).

### 3. Stockage (Load)
- [x] Scripts d'initialisation de la base DuckDB (`setup_duckdb.py`) avec Schéma en Étoile.
- [x] Création des dimensions `dim_cities` (enrichie) et `dim_calendar`.
- [x] Logique de chargement incrémental des CSV vers la table de faits `fct_weather_history`.

### 4. Orchestration (Dagster)
- [x] Définition des Assets (`raw_weather_data`, `load_weather_to_duckdb`).
- [x] Mise en place des Jobs (`weather_pipeline_job`).
- [x] Planification (Schedules) : Exécution quotidienne à 9h00.
- [x] Capteurs (Sensors) : Détection de fichiers CSV et trigger manuel via `.trigger_weather_pipeline`.
- [x] Conteneurisation : Docker et Docker Compose configurés.

---

## ⏳ Ce qu'il reste à faire (Tâches Prioritaires)

### 1. Transformations avec dbt ✅
- [x] **Staging Layer** : Modèle `stg_weather_history` (typage, renommage).
- [x] **Mart Layer** : Records par ville, vagues de chaleur, amplitude thermique.
- [x] **Macros** : Logique Gaps and Islands pour les événements consécutifs.
- [x] **Orchestration** : Intégration de dbt dans Dagster via `dagster-dbt`.

### 2. Tests et Qualité (Validation)
- [ ] **Tests Unitaires (pytest)** : Créer des tests pour valider `weather_extractor.py` (vérifier que `temp_max > temp_min`).
- [x] **Tests dbt** : Validation de la jointure snapshot et contraintes de base (`not_null`, `unique`).
- [ ] **Validation Pydantic** : Renforcer la validation du JSON reçu de l'API.

### 3. Visualisation et Aide à la Décision
- [ ] **Choix de l'outil** : Sélectionner un outil (Evidence.dev, Streamlit, ou un simple notebook intégré).
- [ ] **Dashboard** : Créer un graphique comparatif des 3 villes sur les 30 derniers jours.
- [ ] **Analyse** : Identifier visuellement les anomalies par rapport aux normales saisonnières.

### 4. Finalisation de la Soutenance
- [ ] **README.md** : Mettre à jour avec les captures d'écran du graphe Dagster (Lineage).
- [ ] **Documentation technique** : Expliquer le choix du partitionnement quotidien.
- [ ] **Démo** : Préparer un scénario de "Backfill" (rejouer les données du mois dernier) devant les encadrants.

---

## 🚀 Prochaine action immédiate
**Lancer le `setup_duckdb.py`** pour mettre à jour les tables avec les nouveaux champs (Régions, Altitude) puis effectuer un **Backfill** de 30 jours via l'interface Dagster pour peupler la base de données.
