import json
import sys
from confluent_kafka import Consumer
from data_pipeline import fetch_site_weather, insert_weather_data
import database as db

sys.stdout.reconfigure(line_buffering=True)

TOPIC = "site_requests"

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "weather_fetcher",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])


def process_message(msg):
    record = json.loads(msg.value().decode("utf-8"))
    site_code = record["site_code"]

    result_df, outcome = fetch_site_weather(
        site_code, record["latitude"], record["longitude"]
    )

    if result_df is not None:
        insert_weather_data(result_df)
        print(f"[OK] {site_code} — {len(result_df)} rows inserted")
    else:
        print(f"[{outcome.upper()}] {site_code} — no data returned")

    db.update_tracking_status([site_code], status=outcome)
    consumer.commit(msg)


def run():
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            process_message(msg)

    finally:
        consumer.close()


if __name__ == "__main__":
    run()
