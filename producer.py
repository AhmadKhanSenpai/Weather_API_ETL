import json
import sys
from confluent_kafka import Producer
import pandas as pd
import database as db
from data_pipeline import fetch_site_weather

sys.stdout.reconfigure(line_buffering=True)

TOPIC = "weather_sensor_data"

conf = {"bootstrap.servers": "localhost:9092"}
producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}")


def produce_weather_data(path):
    df = pd.read_csv(path)
    if df.empty:
        print("No sites to process. Please produce metadata first.")
        return

    # skipping sites that are already done
    tracker = db.read_parsed_sites()
    mask = ~df["site_code"].isin(tracker["site_code"])
    df = df.loc[mask]  # skip already-done sites

    for row in df.itertuples(index=False):
        site_code = row.site_code
        lat = row.latitude
        lon = row.longitude
        result_dict = fetch_site_weather(
            site_code=site_code, latitude=lat, longitude=lon
        )

        if result_dict is not None:
            producer.produce(
                topic=TOPIC,
                key=row.site_code.encode("utf-8"),
                value=json.dumps(result_dict).encode("utf-8"),
                callback=delivery_report,
            )
            db.update_tracking_status(row.site_code, status=True)
            print(
                f"[Site_Successful] {row.site_code} — {len(result_dict)} rows inserted"
            )

        else:
            db.update_tracking_status(row.site_code, status=False)
            print(f"[Site_Failed] {row.site_code} — no data returned")
        producer.poll(0)  # drains delivery receipts so the buffer doesn't fill up

    producer.flush()  # blocks until every message is confirmed delivered


if __name__ == "__main__":
    produce_weather_data("meta_data.csv")
