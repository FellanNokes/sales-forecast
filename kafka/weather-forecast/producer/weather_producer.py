"""
weather_producer.py
Hämtar 14-dagars forecast från Open-Meteo varje timme
och publicerar varje timrad som ett eget Kafka-meddelande.

Använder confluent-kafka (Apache Kafka officiell Python-klient).
"""

import os
import time
import json
import logging
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Konfiguration från miljövariabler ─────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "weather-forecast")
FETCH_INTERVAL          = int(os.getenv("FETCH_INTERVAL_SECONDS", 3600))
LOCATIONS_RAW           = os.getenv("LOCATIONS", "59.3293,18.0686,Stockholm")

# Format: "lat,lon,Namn;lat,lon,Namn;..."
LOCATIONS = []
for loc in LOCATIONS_RAW.split(";"):
    parts = loc.strip().split(",")
    if len(parts) == 3:
        LOCATIONS.append({
            "lat":  float(parts[0]),
            "lon":  float(parts[1]),
            "name": parts[2].strip()
        })

# Open-Meteo variabler vi hämtar
HOURLY_VARS = ",".join([
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "precipitation_probability",
    "rain",
    "snowfall",
    "weather_code",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "is_day",
])

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def delivery_report(err, msg):
    """Callback som anropas när ett meddelande bekräftats av Kafka."""
    if err is not None:
        log.error("Leveransfel för %s: %s", msg.key(), err)


def ensure_topic(bootstrap_servers: str, topic: str) -> None:
    """Skapar topic om det inte redan finns."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        new_topic = NewTopic(topic, num_partitions=3, replication_factor=1)
        fs = admin.create_topics([new_topic])
        for t, f in fs.items():
            try:
                f.result()
                log.info("Topic '%s' skapades.", t)
            except Exception as e:
                log.warning("Topic '%s' kunde inte skapas (kanske finns redan): %s", t, e)


def fetch_forecast(location: dict) -> list[dict]:
    """Hämtar forecast för en plats, returnerar lista av timrader."""
    params = {
        "latitude":      location["lat"],
        "longitude":     location["lon"],
        "hourly":        HOURLY_VARS,
        "forecast_days": 14,
        "timezone":      "auto",
    }

    resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    hourly = data["hourly"]
    times  = hourly["time"]

    rows = []
    for i in range(len(times)):
        rows.append({
            "location_name":             location["name"],
            "latitude":                  location["lat"],
            "longitude":                 location["lon"],
            "forecast_time":             times[i],
            "fetched_at":                datetime.now(timezone.utc).isoformat(),
            "temperature_2m":            hourly["temperature_2m"][i],
            "apparent_temperature":      hourly["apparent_temperature"][i],
            "relative_humidity_2m":      hourly["relative_humidity_2m"][i],
            "precipitation":             hourly["precipitation"][i],
            "precipitation_probability": hourly["precipitation_probability"][i],
            "rain":                      hourly["rain"][i],
            "snowfall":                  hourly["snowfall"][i],
            "weather_code":              hourly["weather_code"][i],
            "cloud_cover":               hourly["cloud_cover"][i],
            "wind_speed_10m":            hourly["wind_speed_10m"][i],
            "wind_direction_10m":        hourly["wind_direction_10m"][i],
            "wind_gusts_10m":            hourly["wind_gusts_10m"][i],
            "is_day":                    bool(hourly["is_day"][i]),
        })
    return rows


def create_producer() -> Producer:
    """Skapar en Apache Kafka Producer (confluent-kafka)."""
    conf = {
        "bootstrap.servers":       KAFKA_BOOTSTRAP_SERVERS,
        "acks":                    "all",          # vänta på leader + repliker
        "retries":                 5,
        "retry.backoff.ms":        500,
        "linger.ms":               100,            # batcha meddelanden i 100ms
        "batch.size":              65536,          # 64 KB batch
        "compression.type":        "lz4",          # komprimera
        "socket.keepalive.enable": True,
    }

    # Vänta tills Kafka är redo
    for attempt in range(1, 11):
        try:
            p = Producer(conf)
            # Testa anslutningen
            p.list_topics(timeout=5)
            log.info("Ansluten till Apache Kafka på %s", KAFKA_BOOTSTRAP_SERVERS)
            return p
        except Exception as e:
            log.warning("Kafka inte redo (försök %d/10): %s", attempt, e)
            time.sleep(10)
    raise RuntimeError("Kunde inte ansluta till Kafka efter 10 försök.")


def publish_forecast(producer: Producer) -> None:
    """Hämtar och publicerar forecast för alla platser."""
    for loc in LOCATIONS:
        log.info("Hämtar forecast för %s...", loc["name"])
        try:
            rows = fetch_forecast(loc)
            for row in rows:
                key = f"{loc['name']}_{row['forecast_time']}"
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=key.encode("utf-8"),
                    value=json.dumps(row).encode("utf-8"),
                    callback=delivery_report,
                )
                # Töm köen regelbundet så vi inte håller för mycket i minnet
                producer.poll(0)

            producer.flush()
            log.info("  ✓ %d timrader publicerade för %s", len(rows), loc["name"])
        except Exception as exc:
            log.error("  ✗ Fel vid hämtning av %s: %s", loc["name"], exc)


def main():
    log.info("Weather Producer startar (Apache Kafka KRaft).")
    log.info("Platser: %s", [l["name"] for l in LOCATIONS])

    producer = create_producer()
    ensure_topic(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    while True:
        log.info("═══ Ny hämtningscykel kl. %s ═══", datetime.now().strftime("%H:%M:%S"))
        publish_forecast(producer)
        log.info("Nästa hämtning om %d sekunder.", FETCH_INTERVAL)
        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    main()
