import json
import time
from datetime import datetime

import requests
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "weather-forecast"

STORE_COORDINATES = {
    "Lower Manhattan, New York, United States": (40.7135, -74.0054),
    "Hell's Kitchen, New York, United States": (40.7638, -73.9918),
    "Astoria, New York, United States": (40.7721, -73.9302),
}

DAILY_WEATHER = [
    "weather_code",
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_max",
]


def fetch_forecast() -> list[dict]:
    locations = list(STORE_COORDINATES.keys())
    latitudes = [STORE_COORDINATES[loc][0] for loc in locations]
    longitudes = [STORE_COORDINATES[loc][1] for loc in locations]

    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": latitudes,
            "longitude": longitudes,
            "daily": DAILY_WEATHER,
            "timezone": "America/New_York",
            "forecast_days": 7,
        },
    )
    response.raise_for_status()
    results = response.json()

    if isinstance(results, dict):
        results = [results]

    messages = []
    for location, lat, lon, data in zip(locations, latitudes, longitudes, results):
        print(f"Hämtar forecast för {location}...")
        daily = data["daily"]
        for i, date in enumerate(daily["time"]):
            message = {
                "store_location": location,
                "latitude": lat,
                "longitude": lon,
                "date": date,
                "weather_code": daily["weather_code"][i],
                "temperature_mean": daily["temperature_2m_mean"][i],
                "temperature_max": daily["temperature_2m_max"][i],
                "temperature_min": daily["temperature_2m_min"][i],
                "rain_sum": daily["rain_sum"][i],
                "snowfall_sum": daily["snowfall_sum"][i],
                "wind_speed_10m_max": daily["wind_speed_10m_max"][i],
                "fetched_at": datetime.utcnow().isoformat(),
            }
            messages.append(message)

    return messages


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Ansluten till Kafka på {KAFKA_BROKER}")
    print(f"Skickar till topic: {TOPIC}")

    while True:
        print(f"\n[{datetime.now()}] Hämtar forecast...")
        messages = fetch_forecast()

        for msg in messages:
            producer.send(TOPIC, value=msg)
            print(f"  Skickade: {msg['store_location']} - {msg['date']}")

        producer.flush()
        print(f"Skickade {len(messages)} meddelanden till Kafka!")
        print("Väntar 1 timme innan nästa hämtning...")
        time.sleep(3600)


if __name__ == "__main__":
    main()
