"""
sales_consumer.py

Consumes raw sales messages from Kafka topic "sales-raw".
- Batch-inserts into staging.raw_sales in Supabase
- For each batch, publishes a trigger message to "sales-process" topic
  so sales_processor.py knows new data is ready to clean
"""

import json
import os
import time
from datetime import datetime

import psycopg
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

KAFKA_BROKER   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUME_TOPIC  = "sales-raw"
PRODUCE_TOPIC  = "sales-process"
GROUP_ID       = "sales-raw-consumer"
DB_URL         = os.getenv("DB_URL")
BATCH_SIZE     = 100


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

def create_consumer(retries=10, delay=5):
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                CONSUME_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
            )
            print("Consumer ansluten till Kafka!")
            return consumer
        except Exception as e:
            print(f"Försök {attempt + 1}/{retries} misslyckades: {e}")
            time.sleep(delay)
    raise Exception("Kunde inte ansluta till Kafka")


def create_producer(retries=10, delay=5):
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("Producer ansluten till Kafka!")
            return producer
        except Exception as e:
            print(f"Försök {attempt + 1}/{retries} misslyckades: {e}")
            time.sleep(delay)
    raise Exception("Kunde inte ansluta till Kafka")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db_connection():
    return psycopg.connect(DB_URL)


def insert_raw_batch(conn, batch: list[dict]):
    """Insert a batch of raw rows into staging.raw_sales."""
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO staging.raw_sales (
                transaction_id, transaction_date, transaction_time,
                transaction_qty, store_id, store_location,
                product_id, unit_price, product_category,
                product_type, product_detail, is_synthetic, received_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            [(
                d.get("transaction_id"),
                d.get("transaction_date"),
                d.get("transaction_time"),
                d.get("transaction_qty"),
                d.get("store_id"),
                d.get("store_location"),
                d.get("product_id"),
                d.get("unit_price"),
                d.get("product_category"),
                d.get("product_type"),
                d.get("product_detail"),
                d.get("is_synthetic"),
                datetime.utcnow().isoformat(),
            ) for d in batch]
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"Ansluter till Kafka på {KAFKA_BROKER}...")
    consumer = create_consumer()
    producer = create_producer()

    print("Ansluter till databasen...")
    conn = get_db_connection()
    print("Allt klart! Lyssnar på 'sales-raw'...")

    batch = []

    for message in consumer:
        batch.append(message.value)

        if len(batch) >= BATCH_SIZE:
            try:
                insert_raw_batch(conn, batch)
                print(f"[{datetime.now()}] Sparade {len(batch)} rådata-rader till staging.raw_sales")

                # Trigger processor
                producer.send(PRODUCE_TOPIC, value={"batch_size": len(batch), "triggered_at": datetime.utcnow().isoformat()})
                producer.flush()
                print(f"Skickade trigger till '{PRODUCE_TOPIC}'")

                batch = []
            except Exception as e:
                print(f"Fel vid sparande: {e}")
                conn.rollback()
                batch = []


if __name__ == "__main__":
    main()
