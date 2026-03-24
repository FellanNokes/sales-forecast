import json
import os
import time
from datetime import datetime, timezone
 
import psycopg
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
 
load_dotenv()
 
KAFKA_BROKER  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUME_TOPIC = "sales-raw"
PRODUCE_TOPIC = "sales-process"
GROUP_ID      = "sales-raw-consumer"
DB_URL        = os.getenv("DB_URL")
BATCH_SIZE    = 100
 
BIGINT_MIN = -9223372036854775808
BIGINT_MAX =  9223372036854775807
 
 
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
 
 
def get_db_connection():
    return psycopg.connect(DB_URL, autocommit=False)
 
 
def _safe_transaction_id(tid):
    """Returns tid if valid bigint, else None (will be caught by process_sales as missing)."""
    if tid is None:
        return None
    try:
        tid = int(tid)
        if BIGINT_MIN <= tid <= BIGINT_MAX:
            return tid
        print(f"transaction_id utanför bigint-intervall, sätts till None: {tid}")
        return None
    except (TypeError, ValueError):
        return None
 
 
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
                _safe_transaction_id(d.get("transaction_id")),
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
                datetime.now(timezone.utc).isoformat(),
            ) for d in batch]
        )
    conn.commit()
 
 
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
 
                producer.send(PRODUCE_TOPIC, value={
                    "batch_size": len(batch),
                    "triggered_at": datetime.now(timezone.utc).isoformat(),
                })
                producer.flush()
                print(f"Skickade trigger till '{PRODUCE_TOPIC}'")
 
            except Exception as e:
                print(f"Fel vid sparande: {e}")
                conn.rollback()
                conn = get_db_connection()  # återskapa anslutningen vid fel
 
            batch = []
 
 
if __name__ == "__main__":
    main()