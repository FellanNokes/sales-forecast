# Weather & Sales Pipeline

This project builds a data pipeline that combines historical sales data with weather API data, joins them, and makes the enriched dataset available for analysis. Results are visualized in Evidence dashboards.

The project has two distinct pipelines. The historical pipeline processes a static sales CSV and fetches archived weather data from Open-Meteo, cleans and validates both datasets, and uploads them to Supabase for analysis.

The live pipeline streams 14-day weather forecasts and synthetic sales transactions through Kafka into Supabase, keeping the dashboard continuously updated.

The live pipeline runs in Docker containers. The historical pipeline is run manually with uv and uploads its results to the same Supabase database, making everything available in the Evidence dashboard.

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

This starts all services: Kafka (KRaft), Kafka UI, the four Kafka producer/consumer containers, and the Evidence dashboard. Allow ~30 seconds for Kafka to initialise before producers begin publishing.

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
| 8   | Run with Docker                        |

---

## Architecture

```
Historical Pipeline (Local Execution - Run Once)
--------------------------------------------------
Kaggle CSV ──────────> process_sales_data.py ───> load_sales_data.py ───┐
                                                                        │
Open-Meteo Archive ──> fetch_weather.py ───────> clean_weather_data.py  │
                          │      └─────────────> upload_weather.py ─────┤
                          └────────────────────> weather_features.py ───┤
                                                       └─> upload.py ───┤
                                                                        │
                                                                        │
Docker Compose Runtime (Real-time Stream)                            │
--------------------------------------------                            │
 [ Kafka UI ] <───> [ Kafka Broker ]                                    │
  (port 8080)             ▲    ▲                                        │
                          │    │                                        │
    ┌───────────┬─────────┘    └──────────┬────────────┐                │
    │           │                         │            │                │
 [producer]  [sales-producer]       [consumer]  [sales-consumer]        │
    │           │                         │            │                │
    ▼           ▼                         ▼            ▼                │
 Open-Meteo   Reads Forecasts       ┌──────────────────────────┐        │
    API             │               │      supabase-db         │<───────┘
                    └──────────────>│     (Postgres 15)        │ (Historic Data)
                                    └─────────────┬────────────┘
                                                  │
                                                  ▼
                                            [  evidence  ]
                                             (port 3000)

```

---

## Kafka Pipeline

The real-time streaming layer consists of four Docker services communicating via two Kafka topics.

### Topics

| Topic              | Producer         | Consumer         | Destination table  |
| ------------------ | ---------------- | ---------------- | ------------------ |
| `weather-forecast` | `producer`       | `consumer`       | `weather_forecast` |
| `sales-forecast`   | `sales-producer` | `sales-consumer` | `sales_forecast`   |

### Weather flow

1. `kafka/weather-forecast/producer/weather_producer.py` fetches **14-day daily forecast** data from the Open-Meteo API for each configured store location (Lower Manhattan, Hell's Kitchen, Astoria) and publishes one JSON message per store per day to the `weather-forecast` topic. It polls every **5 minutes**.

2. `kafka/weather-forecast/consumer/weather_consumer.py` subscribes to `weather-forecast`, deserialises each message, and writes it to the `weather_forecast` table via a direct PostgreSQL connection (psycopg). The upsert uses `ON CONFLICT (store_location, date) DO UPDATE SET` — meaning forecasts are always refreshed with the latest values.

### Sales flow

1. `kafka/sales-forecast/producer/sales_producer.py` reads upcoming weather from the `weather_forecast` table, then generates realistic synthetic transactions per store per day — adjusting product mix, quantities, and transaction count based on temperature and weather condition. Messages are published to the `sales-forecast` topic. It runs once every **24 hours**.

2. `kafka/sales-forecast/consumer/sales_consumer.py` subscribes to `sales-forecast` and writes individual transaction rows to `sales_forecast` via a direct PostgreSQL connection. Records are batched in groups of **100** before each insert, using `ON CONFLICT DO NOTHING` to avoid duplicates.

### Startup & retry behaviour

All producers and consumers retry up to **10 times** with a **5-second delay** between attempts while waiting for the Kafka broker to become ready. This means services can start in any order — they will not crash on boot if Kafka hasn't initialised yet.

Kafka runs in **KRaft mode** (no ZooKeeper). The cluster ID is set via `KAFKA_CLUSTER_ID` in `.env`. Allow ~30 seconds after `docker compose up` before producers begin publishing.

---

## Project Structure

```
project/
├── .env                          # Secret keys (Supabase URL, API keys, etc.)
├── .env.example                  # Template for environment variables
├── docker-compose.yml            # Orchestrates Kafka, DB, Producers, Consumers, and Evidence
├── init.sql                      # DDL: Creates all tables and schemas on first start
│
├── app/                          # HISTORICAL PIPELINE (Local/Manual execution)
│   ├── weather/
│   │   ├── fetch_weather.py      # Archive API -> data/raw/
│   │   ├── clean_weather.py      # Data validation -> data/cleaned/
│   │   ├── upload_weather.py     # data/cleaned/ -> Supabase (historic_weather)
│   │   └── weather_features.py   # Feature engineering & labels -> upload_weather_features.py
│   ├── sales/
│   │   ├── process_sales.py      # Kaggle CSV -> data/cleaned/
│   │   ├── load_sales_data.py    # data/cleaned/ -> Supabase (historic_sales)
│   │   └── analyse_sales.py      # SQL analytics directly on DB
│   └── utility/
│       └── supabase_functions.py # Shared Supabase client and DB utility functions
│
├── data/                         # Local storage for historical data processing
│   ├── raw/                      # Raw fetched data (e.g., weather_raw.csv)
│   ├── cleaned/                  # Validated data ready for upload
│   └── rejected/                 # Log files for rows failing validation
│
├── kafka/                        # REAL-TIME PIPELINE (Dockerized Services)
│   ├── weather-forecast/
│   │   ├── producer/             # Fetches weather forecast -> Kafka Topic
│   │   └── consumer/             # Kafka Topic -> Supabase (weather_forecast table)
│   └── sales-forecast/
│       ├── producer/             # Reads weather_forecast -> Generates synthetic sales -> Kafka
│       └── consumer/             # Kafka Topic -> Supabase (sales_forecast table)
│
├── dashboard/                    # EVIDENCE (BI & Visualization)
│   ├── Dockerfile                # Builds the Evidence container
│   ├── package.json              # Evidence dependencies and plugins
│   ├── evidence.config.yaml      # Configuration for the Evidence workspace
│   ├── sources/                  # SQL queries for database connection
│   │   └── postgres/
│   │       ├── connection.yaml    # Database connection settings
│   │       └── sales_analysis.sql # Analytic queries for reporting
│   └── pages/                     # Markdown files for UI/Graphs
│       ├── index.md
│       └── sales_and_weather.md
└──
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

| Metric                             | Value                           |
| ---------------------------------- | ------------------------------- |
| Temperature vs revenue correlation | 0.67 (moderate-strong positive) |
| Rain vs revenue correlation        | -0.16 (weak negative)           |

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

Tables are created automatically when the `supabase-db` container first starts, via `init.sql`. The definitions are shown below for reference.

```sql
CREATE TABLE IF NOT EXISTS weather_forecast (
    id                  BIGSERIAL PRIMARY KEY,
    store_location      TEXT NOT NULL,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    date                DATE NOT NULL,
    weather_code        INTEGER,
    temperature_mean    DOUBLE PRECISION,
    temperature_max     DOUBLE PRECISION,
    temperature_min     DOUBLE PRECISION,
    rain_sum            DOUBLE PRECISION,
    snowfall_sum        DOUBLE PRECISION,
    wind_speed_10m_max  DOUBLE PRECISION,
    fetched_at          TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_location_date UNIQUE (store_location, date)
);

CREATE TABLE IF NOT EXISTS sales_forecast (
    id                  BIGSERIAL PRIMARY KEY,
    transaction_date    DATE NOT NULL,
    transaction_time    TIME NOT NULL,
    transaction_qty     INTEGER,
    store_id            INTEGER,
    store_location      TEXT NOT NULL,
    product_id          INTEGER,
    unit_price          DOUBLE PRECISION,
    product_category    TEXT,
    product_type        TEXT,
    product_detail      TEXT,
    is_synthetic        BOOLEAN DEFAULT TRUE,
    generated_at        TIMESTAMPTZ,
    CONSTRAINT uq_sales_transaction UNIQUE (store_location, transaction_date, transaction_time, store_id)
);

-- Required primary key for analytics upsert to work correctly
ALTER TABLE analytics_revenue_by_store
    ADD PRIMARY KEY (store_id, store_location, transaction_date);

CREATE TABLE IF NOT EXISTS historic_weather (
    id                  BIGSERIAL PRIMARY KEY,
    store_location      TEXT NOT NULL,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    date                DATE NOT NULL,
    weather_code        INTEGER,
    temperature_mean    DOUBLE PRECISION,
    temperature_max     DOUBLE PRECISION,
    temperature_min     DOUBLE PRECISION,
    rain_sum            DOUBLE PRECISION,
    snowfall_sum        DOUBLE PRECISION,
    wind_speed_max      DOUBLE PRECISION,
    CONSTRAINT uq_historic_weather UNIQUE (store_location, date)
);

CREATE TABLE IF NOT EXISTS weather_features (
    id                  BIGSERIAL PRIMARY KEY,
    store_location      TEXT NOT NULL,
    date                DATE NOT NULL,
    weather_code        INTEGER,
    weather_condition   TEXT,
    temperature_mean    DOUBLE PRECISION,
    temp_category       TEXT,
    rain_sum            DOUBLE PRECISION,
    snowfall_sum        DOUBLE PRECISION,
    CONSTRAINT uq_weather_features UNIQUE (store_location, date)
);
```

---

## Running the Analytics Pipelines

These scripts run outside Docker and require `uv` and a valid `.env`.

### Historical sales data

```bash
# 1. Clean and validate the raw sales CSV
uv run app/sales/process_sales_data.py
# Output: data/cleaned/coffee-shop-sales-revenue.csv
#         data/rejected/coffee-shop-sales-revenue.csv

# 2. Upload cleaned sales data to Supabase (historic_sales)
uv run app/sales/load_sales_data.py

# 3. Run sales analytics and upload results to Supabase
uv run app/sales/analyse_sales_data.py
```

### Historical weather data

```bash
# 4. Fetch historical weather from Open-Meteo Archive API (Jan–Jun 2023)
uv run app/weather/fetch_weather.py
# Output: data/raw/weather_raw.csv

# 5. Clean and validate the raw weather data
uv run app/weather/clean_weather_data.py
# Output: data/cleaned/weather_clean.csv
#         data/rejected/weather_rejected.csv

# 6. Upload cleaned weather data to Supabase (historic_weather)
uv run app/weather/upload_weather.py

# 7. Engineer weather features and upload to Supabase (weather_features)
uv run app/weather/weather_features.py
uv run app/weather/upload_weather_features.py
```

### Analysis

```bash
# 8. Run weather-sales correlation analysis
uv run app/analysis/correlation_weather_sales.py
```

All intermediate files are written to `data/cleaned/` and `data/rejected/` for traceability.

---

## Environment Variables

| Variable                  | Description                                                 | Example                                       |
| ------------------------- | ----------------------------------------------------------- | --------------------------------------------- |
| `SUPABASE_URL`            | URL to your Supabase project                                | `https://your-project.supabase.co`            |
| `SUPABASE_KEY`            | Supabase anon/public API key                                | `your-anon-key`                               |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address                                        | `kafka:9092`                                  |
| `KAFKA_CLUSTER_ID`        | Unique ID for the Kafka KRaft cluster                       | `MkU3OEVBNTcwNTJENDM2Qk`                      |
| `DB_URL`                  | Full PostgreSQL connection string (used by Kafka consumers) | `postgresql://user:password@host:5432/dbname` |
| `DB_USER`                 | PostgreSQL username                                         | `postgres`                                    |
| `DB_PASSWORD`             | PostgreSQL password                                         | `some-password-123`                           |
| `DB_HOST`                 | Database host                                               | `postgres`                                    |
| `DB_PORT`                 | Database port                                               | `5432`                                        |
| `DB_NAME`                 | Database name                                               | `some-name`                                   |

---

## Useful Commands

```bash
# Show status of all services
docker compose ps

# Follow logs for a specific service
docker compose logs -f producer
docker compose logs -f consumer
docker compose logs -f sales-producer
docker compose logs -f sales-consumer

# Restart a single service
docker compose restart producer

# Rebuild a single service after a code change
docker compose build producer
docker compose up -d producer
```

---

## Troubleshooting

**Kafka won't start**

```bash
docker compose logs kafka
# Wait 30 seconds after starting — KRaft needs time to initialise
```

**Producer/consumer can't connect to Kafka**

Producers and consumers automatically retry up to 10 times with a 5-second delay while waiting for Kafka to become ready. If the issue persists, check the Kafka logs and verify that `KAFKA_BOOTSTRAP_SERVERS` is set to `kafka:9092` (not `localhost:9092`) inside Docker.

**Messages not appearing in Kafka UI**

Open http://localhost:8080 and check that the `weather-forecast` and `sales-forecast` topics exist and have a rising message count. If topics are missing, the producers have not yet connected — check their logs with `docker compose logs -f producer`.

**Data not appearing in Supabase**

Verify that `SUPABASE_URL` and `SUPABASE_KEY` in `.env` are correct and that all required tables exist. Check consumer logs for upsert errors.

**Evidence shows no data**

Check that `sources/supabase/connection.yaml` points to the correct Supabase project and that the SQL queries reference the correct table names.

**Analytics upsert overwrites wrong rows**

Make sure the primary key is defined on `analytics_revenue_by_store` as described in the Supabase Tables section. Without it, upserts will overwrite rows across store locations incorrectly.

---

## Documentation & Resources

### Data Sources (Open-Meteo)

- [Open-Meteo Forecast API](https://open-meteo.com/en/docs) - Documentation for real-time weather forecasting.
- [Open-Meteo Historical Archive](https://open-meteo.com/en/docs/historical-weather-api) - Documentation for the historical weather database.

### Messaging & Infrastructure

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/) - Official guide to Kafka brokers and topics.
- [Docker Compose File Reference](https://docs.docker.com/compose/compose-file/) - Manual for configuring services in `docker-compose.yml`.

### Database & Visualization

- [Evidence.dev Official Docs](https://docs.evidence.dev/) - Complete guide to building dashboards with SQL and Markdown.
- [Supabase Python Reference](https://supabase.com/docs/reference/python/introduction) - How to interact with the Supabase client.
- [Supabase Documentation](https://supabase.com/docs) - Main guide for database management, Auth, and APIs.

---
