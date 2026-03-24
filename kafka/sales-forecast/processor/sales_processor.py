"""
sales_processor.py

Consumes trigger messages from Kafka topic "sales-process".
On each trigger:
  1. Fetches unprocessed rows from staging.raw_sales
  2. Runs process_sales_data.py cleaning + validation logic
  3. Inserts accepted rows into curated.sales
  4. Inserts rejected rows into staging.rejected_sales
"""

import json
import os
import time
from datetime import datetime

import pandas as pd
import psycopg
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BROKER  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC         = "sales-process"
GROUP_ID      = "sales-process-consumer"
DB_URL        = os.getenv("DB_URL")

import sys
sys.path.insert(0, "/app")
from app.sales.process_sales_data import process_sales


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

def create_consumer(retries=10, delay=5):
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
            )
            print("Processor ansluten till Kafka!")
            return consumer
        except Exception as e:
            print(f"Försök {attempt + 1}/{retries} misslyckades: {e}")
            time.sleep(delay)
    raise Exception("Kunde inte ansluta till Kafka")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db_connection():
    return psycopg.connect(DB_URL)


def fetch_unprocessed(conn) -> pd.DataFrame:
    """Fetch rows from staging.raw_sales that haven't been processed yet."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM staging.raw_sales
            WHERE processed_at IS NULL
            """
        )
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def mark_as_processed(conn, ids: list):
    """Mark rows in staging.raw_sales as processed."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE staging.raw_sales SET processed_at = %s WHERE id = ANY(%s)",
            (datetime.utcnow().isoformat(), ids)
        )
    conn.commit()


def insert_accepted(conn, df: pd.DataFrame):
    """Insert cleaned rows into curated.sales."""
    if df.empty:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO curated.sales (
                transaction_id, transaction_date, transaction_time,
                transaction_qty, store_id, store_location,
                product_id, unit_price, product_category,
                product_type, product_detail, is_synthetic, processed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
            """,
            [(
                row.get("transaction_id"),
                row.get("transaction_date"),
                row.get("transaction_time"),
                row.get("transaction_qty"),
                row.get("store_id"),
                row.get("store_location"),
                row.get("product_id"),
                row.get("unit_price"),
                row.get("product_category"),
                row.get("product_type"),
                row.get("product_detail"),
                row.get("is_synthetic"),
                datetime.utcnow().isoformat(),
            ) for row in df.to_dict(orient="records")]
        )
    conn.commit()
    print(f"Inserted {len(df)} rows into curated.sales")


def insert_rejected(conn, df: pd.DataFrame):
    """Insert rejected rows into staging.rejected_sales."""
    if df.empty:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO staging.rejected_sales (
                transaction_id, transaction_date, transaction_time,
                transaction_qty, store_id, store_location,
                product_id, unit_price, product_category,
                product_type, product_detail, is_synthetic,
                reason, rejected_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [(
                row.get("transaction_id"),
                row.get("transaction_date"),
                row.get("transaction_time"),
                row.get("transaction_qty"),
                row.get("store_id"),
                row.get("store_location"),
                row.get("product_id"),
                row.get("unit_price"),
                row.get("product_category"),
                row.get("product_type"),
                row.get("product_detail"),
                row.get("is_synthetic"),
                row.get("reason"),
                datetime.utcnow().isoformat(),
            ) for row in df.to_dict(orient="records")]
        )
    conn.commit()
    print(f"Inserted {len(df)} rows into staging.rejected_sales")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"Ansluter till Kafka på {KAFKA_BROKER}...")
    consumer = create_consumer()

    print("Ansluter till databasen...")
    conn = get_db_connection()
    print("Allt klart! Lyssnar på 'sales-process'...")

    for message in consumer:
        print(f"\n[{datetime.now()}] Trigger mottagen — bearbetar rådata...")
        try:
            raw_df = fetch_unprocessed(conn)

            if raw_df.empty:
                print("Ingen obearbetad data hittades.")
                continue

            accepted_df, rejected_df = process_sales(raw_df)

            insert_accepted(conn, accepted_df)
            insert_rejected(conn, rejected_df)
            mark_as_processed(conn, raw_df["id"].tolist())

            print(f"Klart! {len(accepted_df)} accepterade, {len(rejected_df)} avvisade.")

        except Exception as e:
            print(f"Fel vid bearbetning: {e}")
            conn.rollback()


if __name__ == "__main__":
    main()