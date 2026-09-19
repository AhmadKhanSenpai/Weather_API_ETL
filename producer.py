from confluent_kafka import Producer
import json
import database as db
import pandas as pd

TOPIC = "site_requests"
config = {"bootstrap.servers": "localhost:9092"}
producer = Producer(config)


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_site_requests(path):
    # inserting the meta data in database
    df = pd.read_csv(path)
    db.insert_meta_data(df)

    # filtering all the sites that have already been parsed
    tracker = db.read_parsed_sites()
    mask = ~df["site_code"].isin(tracker["site_code"])
    df = df.loc[mask]

    for row in df.itertuples(index=False):
        params = {
            "site_code": row.site_code,
            "latitude": row.latitude,
            "longitude": row.longitude,
        }

        producer.produce(
            topic=TOPIC,
            key=row.site_code.encode("utf-8"),
            value=json.dumps(params).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
        producer.flush()


if __name__ == "__main__":
    produce_site_requests("meta_data.csv")
