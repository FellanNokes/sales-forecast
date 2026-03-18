## Project Description

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

## Installation & SetUp

### 1. Clone the repository

```bash
git clone https://github.com/your-org/your-repo.git
cd your-repo
```

### 2. Set up environment variables

Create a `.env` file in the project root by copying the example file:

```bash
cp .env.example .env
```

> Never commit...

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

### 5. Stop the stack

```bash
docker compose down
```

To also remove volumes:

```bash
docker compose down -v
```

## Environment Variables

Create a `.env` file in the project root by copying the example file:

```bash
cp .env.example .env
```

> **Never commit `.env` to version control.** Make sure it is listed in `.gitignore`.

| Variable                  | Description                    | Example                             |
| ------------------------- | ------------------------------ | ----------------------------------- |
| `DB_USER`                 | PostgreSQL username            | `postgres`                          |
| `DB_PASSWORD`             | PostgreSQL password            | `some-password-123`                 |
| `DB_HOST`                 | Database host                  | `postgres`                          |
| `DB_PORT`                 | Database port                  | `5432`                              |
| `DB_NAME`                 | Database name                  | `some-name`                         |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address           | `kafka:9092`                        |
| `PRODUCTS_TOPIC`          | Kafka topic for product events | `products.created`                  |
| `RAW_EVENTS_GROUP_ID`     | Kafka consumer group ID        | `raw-events-writer`                 |
| `SUPABASE_URL`            | URL to your Supabase project   | `https://your-project.supabase.co/` |
| `SUPABASE_KEY`            | Supabase anon/public API key   | `your-anon-key`                     |

---
