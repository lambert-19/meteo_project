FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/opt/dagster/dagster_home

RUN mkdir -p $DAGSTER_HOME

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    dagster==1.13.3 \
    dagster-webserver==1.13.3 \
    dagster-dbt==0.29.3 \
    dbt-duckdb==1.8.3 \
    duckdb==1.0.0 \
    pandas==2.2.2 \
    requests==2.32.3 \
    python-dotenv \
    pydantic \
    streamlit \
    plotly

COPY . .

RUN pip install -e dagster_weather/

WORKDIR /app/dagster_weather

EXPOSE 3000

CMD ["dagster", "dev", "-m", "dagster_weather", "-h", "0.0.0.0", "-p", "3000"]
