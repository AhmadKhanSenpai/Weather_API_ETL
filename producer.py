import json
import sys
from confluent_kafka import Producer
import pandas as pd
import database as db

sys.stdout.reconfigure(line_buffering=True)

TOPIC = "site_requests"

conf = {"bootstrap.servers": "localhost:9092"}
producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}")


def produce_site_requests(path):
    df = pd.read_csv(path)
    db.insert_meta_data(df)  # metadata still goes straight to Postgres, unchanged

    tracker = db.read_parsed_sites()
    mask = ~df["site_code"].isin(tracker["site_code"])
    df = df.loc[mask]  # same filtering as before — skip already-done sites

    for row in df.itertuples(index=False):
        payload = {
            "site_code": row.site_code,
            "latitude": row.latitude,
            "longitude": row.longitude,
        }

        producer.produce(
            topic=TOPIC,
            key=row.site_code.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)  # drains delivery receipts so the buffer doesn't fill up

    producer.flush()  # blocks until every message is confirmed delivered


if __name__ == "__main__":
    produce_site_requests("meta_data.csv")
