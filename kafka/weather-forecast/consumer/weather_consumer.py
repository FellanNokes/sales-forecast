import json
import os
import time
from datetime import datetime

import psycopg
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "weather-forecast"
GROUP_ID = "weather-forecast-consumer"
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


def insert_forecast(conn, data: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO weather_forecast (
                store_location, latitude, longitude, date, weather_code,
                temperature_mean, temperature_max, temperature_min,
                rain_sum, snowfall_sum, wind_speed_10m_max, fetched_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (store_location, date) DO UPDATE SET
                weather_code = EXCLUDED.weather_code,
                temperature_mean = EXCLUDED.temperature_mean,
                temperature_max = EXCLUDED.temperature_max,
                temperature_min = EXCLUDED.temperature_min,
                rain_sum = EXCLUDED.rain_sum,
                snowfall_sum = EXCLUDED.snowfall_sum,
                wind_speed_10m_max = EXCLUDED.wind_speed_10m_max,
                fetched_at = EXCLUDED.fetched_at
            """,
            (
                data["store_location"], data["latitude"], data["longitude"],
                data["date"], data["weather_code"], data["temperature_mean"],
                data["temperature_max"], data["temperature_min"],
                data["rain_sum"], data["snowfall_sum"],
                data["wind_speed_10m_max"], data["fetched_at"],
            ),
        )
    conn.commit()


def main():
    print(f"Ansluter till Kafka på {KAFKA_BROKER}...")
    consumer = create_consumer()

    print("Ansluter till databasen...")
    conn = get_db_connection()
    print("Allt klart! Lyssnar på Kafka-meddelanden...")

    for message in consumer:
        data = message.value
        try:
            with get_db_connection() as conn:
                insert_forecast(conn, data)
                print(
                    f"[{datetime.now()}] Sparade: {data['store_location']} - {data['date']}")
        except Exception as e:
            print(f"Fel vid sparande: {e}")


if __name__ == "__main__":
    main()
