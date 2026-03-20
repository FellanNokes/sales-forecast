# Weather & Sales Pipeline

This project builds a data pipeline that combines **historical sales data** with **weather API data**, joins them, and makes the enriched dataset available for analysis. Events flow through Kafka topics, get transformed and persisted to Supabase (PostgreSQL), and are visualized in Evidence dashboards.

The entire stack runs in Docker containers for easy local setup and reproducible deployments.

---

## MVP

|     | Tasks                                  |
| --- | -------------------------------------- |
| 1   | Import sales data                      |
| 2   | Load weather API                       |
| 3   | Join historical weather and sales data |
| 4   | Analyze sales data                     |
| 5   | Analyze sales with weather data        |
| 6   | Setup project                          |
| 7   | Load data with Kafka                   |

---

## Built With

### Core Pipeline

[![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)](https://docs.python.org/3/)
[![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/docs/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)](https://kafka.apache.org/documentation/)
[![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docs.docker.com/)

### Storage & Database

[![Supabase](https://img.shields.io/badge/Supabase-3ECF8E?style=for-the-badge&logo=supabase&logoColor=white)](https://supabase.com/docs)
[![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/docs/)
[![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=databricks&logoColor=white)](https://docs.databricks.com/en/sql/index.html)

### Visualisation & BI

[![Evidence](https://img.shields.io/badge/Evidence-black?style=for-the-badge)](https://docs.evidence.dev/)

### Data & Dev Tools

[![Kaggle](https://img.shields.io/badge/Kaggle-035a7d?style=for-the-badge&logo=kaggle&logoColor=white)](https://www.kaggle.com/docs)
[![uv](https://img.shields.io/badge/uv-%23DE5FE9.svg?style=for-the-badge&logo=uv&logoColor=white)](https://docs.astral.sh/uv/)
[![NPM](https://img.shields.io/badge/NPM-%23CB3837.svg?style=for-the-badge&logo=npm&logoColor=white)](https://docs.npmjs.com/)
[![Markdown](https://img.shields.io/badge/markdown-%23000000.svg?style=for-the-badge&logo=markdown&logoColor=white)](https://www.markdownguide.org/)

### IDE & Version Control

[![VS Code](https://img.shields.io/badge/Visual%20Studio%20Code-0078d7.svg?style=for-the-badge&logo=visual-studio-code&logoColor=white)](https://code.visualstudio.com/docs)
[![PyCharm](https://img.shields.io/badge/pycharm-143?style=for-the-badge&logo=pycharm&logoColor=black&color=black&labelColor=green)](https://www.jetbrains.com/help/pycharm/)
[![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white)](https://docs.github.com/)

---

## Architecture

```
Open-Meteo API ──→ weather-producer ──→ Kafka (weather-data) ──→ weather-consumer ──→ Supabase
                                                                                          ↑
Fake Sales ────→ sales-producer ────→ Kafka (sales-data) ────→ sales-consumer ───────────┘
                                                                                          ↓
                                                                                      Evidence
```

---

## Project Structure

```
project/
├── .env                          # Secret keys (never commit this)
├── .env.example                  # Template — copy to .env and fill in values
├── Dockerfile                    # Evidence container
├── docker-compose.yml            # Starts all services
├── package.json                  # Evidence dependencies
│
├── app/
│   ├── weather/
│   │   ├── coordinates.py        # Geocodes store locations via Nominatim/geopy
│   │   ├── fetch_weather.py      # Fetches historical weather per store from Open-Meteo
│   │   ├── clean_weather_data.py # Cleans and rejects invalid weather rows
│   │   ├── upload_weather.py     # Uploads cleaned data to Supabase (historic_weather)
│   │   ├── weather_features.py   # Engineers temp_category and weather_condition labels
│   │   └── upload_weather_features.py  # Uploads weather features to Supabase
│   ├── analysis/
│   │   └── correlation_weather_sales.py  # Computes weather-sales correlations
│   └── utility/
│       └── supabase_functions.py # Shared Supabase client, fetch_table, upload_dataframe
│
├── data/
│   ├── raw/                      # Raw fetched data (e.g. weather_raw.csv)
│   ├── cleaned/                  # Validated data ready for upload
│   └── rejected/                 # Rows that failed validation
│
├── pages/                        # Evidence dashboard pages
│   ├── index.md
│   ├── Products.md
│   ├── Revenue.md
│   └── sales_and_weather.md
│
├── sources/
│   └── supabase/                 # SQL queries for Evidence
│       ├── connection.yaml
│       ├── sales_weather_joined.sql
│       ├── weather_correlation_results.sql
│       ├── weather_sales_summary.sql
│       └── ...
│
├── kafka-producer/               # Fetches weather from Open-Meteo → Kafka
│   ├── Dockerfile
│   ├── requirements.txt
│   └── weather_producer.py
│
├── kafka-consumer/               # Kafka → Supabase (weather_forecast)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── weather_consumer.py
│
├── kafka-sales-producer/         # Generates fake sales data → Kafka
│   ├── Dockerfile
│   ├── requirements.txt
│   └── sales_producer.py
│
└── kafka-sales-consumer/         # Kafka → Supabase (sales_forecast)
    ├── Dockerfile
    ├── requirements.txt
    └── sales_consumer.py
```

---

## Data Pipeline

### Sales Data

Raw sales data is fetched from Supabase, then passed through a two-step validation pipeline:

- `clean_dataframe()` — normalizes fields such as dates, currency formatting, and null checks
- `validate_dataframe()` — filters out incomplete or malformed records and returns both a validated dataset and a rejected dataset for traceability

### Weather Data

Historical daily weather data (Jan–Jun 2023) is fetched from the Open-Meteo Archive API for each store location. The pipeline:

1. Geocodes store locations using Nominatim/geopy (`coordinates.py`)
2. Fetches raw weather per store (`fetch_weather.py`)
3. Cleans and validates the data, rejecting rows with missing values, out-of-range temperatures, negative precipitation, or duplicates (`clean_weather_data.py`)
4. Uploads cleaned data to Supabase in batches via upsert (`upload_weather.py`)
5. Exports both cleaned and rejected rows to CSV for traceability

### Weather Feature Engineering

Raw weather data is enriched with categorical features:

- `temp_category` — classifies temperature into freezing / cold / mild / warm
- `weather_condition` — human-readable labels derived from WMO weather codes

Results are stored in the `weather_features` Supabase table.

### Correlation Analysis

`correlation_weather_sales.py` fetches the `sales_weather_joined` table, aggregates daily revenue per store, weather condition, and temperature category, then computes correlations. Results are uploaded to `weather_sales_summary` and `weather_correlation_results`.

| Metric                             | Value                                |
| ---------------------------------- | ------------------------------------ |
| Temperature vs revenue correlation | 0.67 (moderate-strong positive)      |
| Rain vs revenue correlation        | -0.16 (weak negative)                |
| Warm vs freezing revenue           | ~2x higher on warm days              |
| Best weather condition             | Clear weather outperforms all others |

### Shared Supabase Utility (`app/utility/supabase_functions.py`)

A reusable module used across the entire project for consistent database access:

- **Cached client** — creates the Supabase client once and reuses it, with credentials loaded from `.env`
- **`fetch_table(table_name)`** — retrieves all rows using pagination (1000 rows per request), returns a pandas DataFrame
- **`upload_dataframe(df, table_name)`** — uploads data in batches of 500 rows via upsert with per-batch error handling

```python
from app.utility import supabase_functions as sf

df = sf.fetch_table("sales_weather_joined")
sf.upload_dataframe(df, "analytics_table")
```

---

## Analytics Tables

The sales analytics pipeline computes and stores the following tables in Supabase:

| Table                           | Contents                                                 |
| ------------------------------- | -------------------------------------------------------- |
| `analytics_revenue_by_store`    | Daily revenue per store                                  |
| `analytics_top5_products`       | Top 5 products overall per store                         |
| `analytics_top5_products_month` | Top 5 products per month and store                       |
| `analytics_revenue_per_month`   | Monthly revenue per store                                |
| `analytics_top_day_per_month`   | Best revenue day per month and store                     |
| `analytics_least5_popular`      | 5 least popular products per store                       |
| `weather_features`              | Temperature categories and weather condition labels      |
| `weather_sales_summary`         | Aggregated revenue per weather condition and temperature |
| `weather_correlation_results`   | Correlation coefficients between weather and revenue     |

---

## Supabase Tables

Run the following in **Supabase Dashboard → SQL Editor** before starting:

```sql
CREATE TABLE IF NOT EXISTS weather_forecast (
    id                        BIGSERIAL PRIMARY KEY,
    location_name             TEXT NOT NULL,
    latitude                  DOUBLE PRECISION,
    longitude                 DOUBLE PRECISION,
    forecast_time             TIMESTAMPTZ NOT NULL,
    temperature_2m            DOUBLE PRECISION,
    apparent_temperature      DOUBLE PRECISION,
    precipitation             DOUBLE PRECISION,
    precipitation_probability INTEGER,
    wind_speed_10m            DOUBLE PRECISION,
    wind_direction_10m        INTEGER,
    weather_code              INTEGER,
    cloud_cover               INTEGER,
    relative_humidity_2m      INTEGER,
    is_day                    BOOLEAN,
    fetched_at                TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_location_time UNIQUE (location_name, forecast_time)
);

CREATE TABLE IF NOT EXISTS sales_forecast (
    id              BIGSERIAL PRIMARY KEY,
    city            TEXT NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    transactions    INTEGER,
    revenue_sek     DOUBLE PRECISION,
    avg_order_sek   DOUBLE PRECISION,
    CONSTRAINT uq_sales_city_time UNIQUE (city, timestamp)
);

-- Required primary key for analytics upsert to work correctly
ALTER TABLE analytics_revenue_by_store
    ADD PRIMARY KEY (store_id, store_location, transaction_date);
```

---

## Installation & Setup

### 1. Clone the repository

```bash
git clone https://github.com/your-org/your-repo.git
cd your-repo
```

### 2. Set up environment variables

```bash
cp .env.example .env
```

> **Never commit `.env` to version control.** Make sure it is listed in `.gitignore`.

Open `.env` and fill in your values. See the [Environment Variables](#environment-variables) section below.

### 3. Start the stack

```bash
docker compose up --build
```

### 4. Verify everything is running

```bash
docker compose ps
```

All services should show status `running`.

### 5. Open dashboards

| Service  | URL                   | Description                       |
| -------- | --------------------- | --------------------------------- |
| Evidence | http://localhost:3000 | Weather and sales dashboard       |
| Kafka UI | http://localhost:8080 | Monitor Kafka topics and messages |

### 6. Stop the stack

```bash
docker compose down
```

To also remove volumes:

```bash
docker compose down -v
```

---

## Running the Analytics Pipelines

These scripts run outside Docker and require `uv` and a valid `.env`.

```bash
# 1. Fetch and clean historical weather data
uv run app/weather/fetch_weather.py
uv run app/weather/clean_weather_data.py
uv run app/weather/upload_weather.py

# 2. Engineer weather features
uv run app/weather/weather_features.py
uv run app/weather/upload_weather_features.py

# 3. Run sales analytics
python analytics.py

# 4. Run weather-sales correlation analysis
python app/analysis/correlation_weather_sales.py
```

Output files are written to `data/cleaned/` and `data/rejected/` for traceability.

---

## Environment Variables

| Variable                  | Description                           | Example                            |
| ------------------------- | ------------------------------------- | ---------------------------------- |
| `SUPABASE_URL`            | URL to your Supabase project          | `https://your-project.supabase.co` |
| `SUPABASE_KEY`            | Supabase anon/public API key          | `your-anon-key`                    |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address                  | `kafka:9092`                       |
| `KAFKA_CLUSTER_ID`        | Unique ID for the Kafka KRaft cluster | `MkU3OEVBNTcwNTJENDM2Qk`           |
| `PRODUCTS_TOPIC`          | Kafka topic for product events        | `products.created`                 |
| `RAW_EVENTS_GROUP_ID`     | Kafka consumer group ID               | `raw-events-writer`                |
| `DB_USER`                 | PostgreSQL username                   | `postgres`                         |
| `DB_PASSWORD`             | PostgreSQL password                   | `some-password-123`                |
| `DB_HOST`                 | Database host                         | `postgres`                         |
| `DB_PORT`                 | Database port                         | `5432`                             |
| `DB_NAME`                 | Database name                         | `some-name`                        |

---

## Useful Commands

```bash
# Show status of all services
docker compose ps

# Follow logs for a specific service
docker compose logs -f weather-producer
docker compose logs -f kafka-sales-consumer

# Restart a single service
docker compose restart weather-producer

# Rebuild a single service after a code change
docker compose build weather-producer
docker compose up -d weather-producer
```

---

## Troubleshooting

**Kafka won't start**

```bash
docker compose logs kafka
# Wait 30 seconds after starting — KRaft needs time to initialise
```

**Producer/consumer can't connect to Kafka**

Producers and consumers automatically retry for up to 100 seconds while waiting for Kafka to become ready. If the issue persists, check the Kafka logs.

**Data not appearing in Supabase**

Verify that `SUPABASE_URL` and `SUPABASE_KEY` in `.env` are correct and that all required tables exist.

**Evidence shows no data**

Check that `sources/supabase/connection.yaml` points to the correct Supabase project and that the SQL queries reference the correct table names.

**Analytics upsert overwrites wrong rows**

Make sure the primary key is defined on `analytics_revenue_by_store` as described in the Supabase Tables section. Without it, upserts will overwrite rows across store locations incorrectly.

---
