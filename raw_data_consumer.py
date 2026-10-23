import json
import sys
import pandas as pd
from confluent_kafka import Consumer, TopicPartition
from database import insert_weather_data

sys.stdout.reconfigure(line_buffering=True)

TOPIC = "weather_sensor_data"

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "raw-data-writer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
consumer = Consumer(conf)
consumer.subscribe([TOPIC])


def process_msg(msg):
    weather_data = json.loads(msg.value().decode("utf-8"))
    weather_data_df = pd.DataFrame(weather_data)
    insert_weather_data(weather_data_df.to_dict(orient="records"))
    print(f"""
    Data inserted for: site={msg.key().decode('utf-8')}
    offset={msg.offset()}
    """)
    consumer.commit(msg)


try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            process_msg(msg)

        except Exception as e:
            site_code = msg.key().decode("utf-8")
            print(f"Retrying site={site_code}: {e}, offset={msg.offset()}")
            consumer.seek(
                TopicPartition(msg.topic(), msg.partition(), msg.offset())
            )  # seek to the same offset, loop will keep retrying that message
            consumer.commit(msg)

except KeyboardInterrupt:
    print("Analytics consumer stopped")
    consumer.close()

finally:
    print("Analytics consumer closed all messages consumed")
    consumer.close()
