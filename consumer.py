import json
from confluent_kafka import Consumer
from data_pipeline import fetch_site_weather, insert_weather_data
import database as db

TOPIC = "site-requests"

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "weather-fetchers",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])


def process_message(msg):
    record = json.loads(msg.value().decode("utf-8"))
    site_code = record["site_code"]

    result_df = fetch_site_weather(site_code, record["latitude"], record["longitude"])

    if result_df is not None:
        insert_weather_data(result_df)
        db.update_tracking_status([site_code], status=True)
    else:
        db.update_tracking_status([site_code], status=False)

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
