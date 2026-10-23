import json
import sys
import time
import pandas as pd
import database as db
from confluent_kafka import Consumer, TopicPartition

sys.stdout.reconfigure(line_buffering=True)

TOPIC = "weather_sensor_data"
SUN_LIMIT = 200
WIND_LIMIT = 3

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "analytics-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
consumer = Consumer(conf)
consumer.subscribe([TOPIC])


def power_mix(weather_data):
    df = pd.DataFrame(weather_data)
    df["date"] = pd.to_datetime(df["date"])

    df = df[["site_code", "date", "wind_speed_80m", "shortwave_radiation"]].copy()
    df["sun_ok"] = df["shortwave_radiation"] > SUN_LIMIT

    df["wind_ok"] = df["wind_speed_80m"] > WIND_LIMIT

    df["DG_ok"] = ~(df["sun_ok"] | df["wind_ok"])

    df["date"] = pd.to_datetime(df["date"]).dt.date

    return (
        df.groupby(["site_code", "date"])
        .agg(
            {
                "wind_speed_80m": "mean",
                "shortwave_radiation": "mean",
                "sun_ok": "sum",
                "wind_ok": "sum",
                "DG_ok": "sum",
            }
        )
        .reset_index()
    )


def process_message(msg):
    weather_data = json.loads(msg.value().decode("utf-8"))
    aggregate_data = power_mix(weather_data)
    db.insert_power_mix(aggregate_data)
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
                print(f"Retrying site={site_code}: {e}, offset={msg.offset()}")
                consumer.seek(
                    TopicPartition(msg.topic(), msg.partition(), msg.offset())
                )  # seek to the same offset, loop will keep retrying that message
                time.sleep(5)

    except KeyboardInterrupt:
        print("Analytics consumer stopped")

    finally:
        print("Analytics consumer closed all messages consumed")
        consumer.close()


if __name__ == "__main__":
    run()
