"""
sales_producer.py

Fetches weather forecast from Supabase, generates messy fake sales data
via generate_fake_sales(), and publishes each transaction to the
Kafka topic "sales-raw".

Runs once on startup, then waits 24 hours before running again.
"""

import json
import os
import time
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC        = "sales-raw"
FORECAST_DAYS = 7

# Import generate_fake_sales — works when run via Docker (context: project root)
import sys
sys.path.insert(0, "/app")
from app.sales.generate_fake_sales import generate_fake_sales


# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------

def create_producer(retries=10, delay=5):
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
            print(f"Ansluten till Kafka på {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"Försök {attempt + 1}/{retries} misslyckades: {e}")
            print(f"Väntar {delay}s innan nästa försök...")
            time.sleep(delay)
    raise Exception("Kunde inte ansluta till Kafka efter flera försök")


def main():
    producer = create_producer()
    print(f"Skickar till topic: {TOPIC}")

    while True:
        print(f"\n[{datetime.now()}] Genererar försäljningsdata...")
        try:
            df = generate_fake_sales(days=FORECAST_DAYS, random_seed=None)
            records = df.to_dict(orient="records")

            for record in records:
                producer.send(TOPIC, value=record)

            producer.flush()
            print(f"Skickade {len(records)} meddelanden till Kafka topic '{TOPIC}'!")

        except Exception as e:
            print(f"Fel: {e}")

        print("Väntar 24 timmar innan nästa körning...")
        time.sleep(86400)


if __name__ == "__main__":
    main()
