FROM python:3.10-slim

# Empêche Python de générer des fichiers .pyc et force l'affichage des logs en temps réel
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Dossier requis pour la configuration de Dagster
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

WORKDIR /app

# Installation des dépendances système nécessaires pour DuckDB et dbt
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Mise à jour de pip et installation des dépendances clés
# Note: On installe manuellement les versions stables car le requirements.txt actuel contient des erreurs de version
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    dagster==1.9.2 \
    dagster-webserver==1.9.2 \
    dagster-dbt==0.25.2 \
    dbt-duckdb==1.8.3 \
    pandas==2.2.2 \
    requests==2.32.3

# Copie de l'ensemble du projet
COPY . .

# Installation du projet local en mode éditable (basé sur ton pyproject.toml)
RUN pip install -e .

# Port par défaut pour l'interface Dagster
EXPOSE 3000

# Commande de lancement pour le développement
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]