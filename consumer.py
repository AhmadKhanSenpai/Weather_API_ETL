import json
import sys
from confluent_kafka import Consumer
from data_pipeline import insert_weather_data

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


def process_message(msg):
    site_code = msg.key().decode("utf-8")
    weather_data = json.loads(msg.value().decode("utf-8"))

    insert_weather_data(weather_data)

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
