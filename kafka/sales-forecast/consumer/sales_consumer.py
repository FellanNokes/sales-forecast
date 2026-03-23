import json
import os
import time
from datetime import datetime

import psycopg
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "sales-forecast"
GROUP_ID = "sales-forecast-consumer"
DB_URL = os.getenv("DB_URL")


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
            print("Ansluten till Kafka!")
            return consumer
        except Exception as e:
            print(f"Försök {attempt + 1}/{retries} misslyckades: {e}")
            print(f"Väntar {delay}s...")
            time.sleep(delay)
    raise Exception("Kunde inte ansluta till Kafka efter flera försök")


def get_db_connection():
    return psycopg.connect(DB_URL)


def insert_transaction(conn, data: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sales_forecast (
                transaction_date, transaction_time, transaction_qty,
                store_id, store_location, product_id, unit_price,
                product_category, product_type, product_detail,
                is_synthetic, generated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (store_location, transaction_date, transaction_time, store_id) 
            DO NOTHING
            """,
            (
                data["transaction_date"],
                data["transaction_time"],
                data["transaction_qty"],
                data["store_id"],
                data["store_location"],
                data["product_id"],
                data["unit_price"],
                data["product_category"],
                data["product_type"],
                data["product_detail"],
                data["is_synthetic"],
                data["generated_at"],
            ),
        )
    conn.commit()

def main():
    print(f"Ansluter till Kafka på {KAFKA_BROKER}...")
    consumer = create_consumer()

    print("Ansluter till databasen...")
    conn = get_db_connection()
    print("Allt klart! Lyssnar på Kafka-meddelanden...")

    batch = []
    BATCH_SIZE = 100

    for message in consumer:
        batch.append(message.value)

        if len(batch) >= BATCH_SIZE:
            try:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO sales_forecast (
                            transaction_date, transaction_time, transaction_qty,
                            store_id, store_location, product_id, unit_price,
                            product_category, product_type, product_detail,
                            is_synthetic, generated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (store_location, transaction_date, transaction_time, store_id)
                        DO NOTHING
                        """,
                        [(
                            d["transaction_date"], d["transaction_time"],
                            d["transaction_qty"], d["store_id"], d["store_location"],
                            d["product_id"], d["unit_price"], d["product_category"],
                            d["product_type"], d["product_detail"],
                            d["is_synthetic"], d["generated_at"],
                        ) for d in batch]
                    )
                conn.commit()
                print(f"[{datetime.now()}] Sparade {len(batch)} rader")
                batch = []
            except Exception as e:
                print(f"Fel vid sparande: {e}")
                conn.rollback()
                batch = []

if __name__ == "__main__":
    main()
