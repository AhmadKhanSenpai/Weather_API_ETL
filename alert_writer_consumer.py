import json
import pandas as pd
import sys
import database as db
import time
from confluent_kafka import Consumer, TopicPartition

sys.stdout.reconfigure(line_buffering=True)

TOPIC = "site_danger_alerts"

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "alert-writer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])


def process_message(msg):
    alerts = pd.DataFrame(json.loads(msg.value().decode("utf-8")))
    db.insert_danger_alerts(alerts)
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
        print("Alert consumer stopped")

    finally:
        print("Alert consumer closed all messages consumed")
        consumer.close()


if __name__ == "__main__":
    run()
