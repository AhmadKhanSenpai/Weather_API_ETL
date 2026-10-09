import json
import sys
import pandas as pd
from confluent_kafka import Consumer, TopicPartition
from database import insert_daily_aggregate

sys.stdout.reconfigure(line_buffering=True)

TOPIC = "weather_sensor_data"

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "analytics-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
consumer = Consumer(conf)
consumer.subscribe([TOPIC])


def daily_stats(weather_data):
    df = pd.DataFrame(weather_data)
    df["date"] = pd.to_datetime(df["date"])

    # aggregating the data by day (dt.date will remove time and zone)
    df["date"] = df["date"].dt.date
    daily_stats = (
        df.groupby(["date", "site_code"])
        .agg(
            {
                "temperature_2m": "mean",
                "relative_humidity_2m": "mean",
                "shortwave_radiation": "mean",
            }
        )
        .reset_index()
    )

    return daily_stats


def process_message(msg):
    weather_data = json.loads(msg.value().decode("utf-8"))
    aggregate_data = daily_stats(weather_data)
    insert_daily_aggregate(aggregate_data)
    print(f"""
    Data inserted for: site={msg.key().decode('utf-8')}
    offset={msg.offset()}
    """)
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

            try:
                process_message(msg)

            except Exception as e:
                site_code = msg.key().decode("utf-8")
                print(f"[SKIPPED] offset={msg.offset()} site={site_code}: {e}")
                consumer.seek(
                    TopicPartition(msg.topic(), msg.partition(), msg.offset())
                )  # seek to the same offset
                consumer.commit(msg)

    except KeyboardInterrupt:
        print("Analytics consumer stopped")
        consumer.close()

    finally:
        print("Analytics consumer closed all messages consumed")
        consumer.close()


if __name__ == "__main__":
    run()
