import json
import sys
from confluent_kafka import Consumer
from database import insert_weather_data

# so the print statement is not buffered
sys.stdout.reconfigure(line_buffering=True)

TOPIC = "weather_sensor_data"

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "weather_fetcher",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])


def transform_weather_data(weather_data, site_code):
    return [
        {
            "site_code": site_code,
            "date": date,
            "temperature_2m": temperature,
            "relative_humidity_2m": humidity,
            "shortwave_radiation": radiation,
        }
        for date, temperature, humidity, radiation in zip(
            weather_data["date"],
            weather_data["temperature_2m"],
            weather_data["relative_humidity_2m"],
            weather_data["shortwave_radiation"],
        )
    ]


def process_message(msg):
    site_code = msg.key().decode("utf-8")

    weather_data = json.loads(msg.value().decode("utf-8"))
    rows = transform_weather_data(weather_data, site_code)

    insert_weather_data(rows)
    consumer.commit(msg)
    print(f"[OK] {site_code} — {len(rows)} rows inserted")


def run():
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                process_message(msg)
            except Exception as e:
                # Without this, one bad message kills the entire consumer process, not just that message.
                print(f"[ERROR] offset={msg.offset()} site={msg.key()}: {e}")
                consumer.commit(msg)

    finally:
        consumer.close()


if __name__ == "__main__":
    run()
