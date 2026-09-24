import json
import sys
from confluent_kafka import Producer
import pandas as pd
import database as db
from data_pipeline import fetch_site_weather

# so that print statement is not buffered
sys.stdout.reconfigure(line_buffering=True)

TOPIC = "weather_sensor_data"

conf = {"bootstrap.servers": "localhost:9092"}
producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key().decode('utf-8')}: {err}")

    else:
        print(f"""
        Delivered key={msg.key()} 
        to topic={msg.topic()} partition={msg.partition()}
        at offset={msg.offset()}
        """)


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
        result_json = fetch_site_weather(
            site_code=site_code, latitude=lat, longitude=lon
        )

        if result_json is not None:
            db.update_tracking_status(site_code, status=True)

            producer.produce(
                topic=TOPIC,
                key=row.site_code.encode("utf-8"),
                value=json.dumps(result_json).encode("utf-8"),
                callback=delivery_report,
            )

        else:
            db.update_tracking_status(row.site_code, status=False)
            print(f"[Site_Failed] {row.site_code} — no data returned")

        producer.poll(0)

    producer.flush()  # blocks until every message is confirmed delivered


if __name__ == "__main__":
    db.create_tracking_table()
    db.create_weather_table()
    produce_weather_data("meta_data.csv")
