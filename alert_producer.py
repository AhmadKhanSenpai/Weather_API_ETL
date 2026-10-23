import sys
import json
import time
import pandas as pd
import database as db
from confluent_kafka import Consumer, Producer, TopicPartition

sys.stdout.reconfigure(line_buffering=True)


HEAT_LIMIT = 40
STORM_LIMIT = 15
HEAVY_RAIN_LIMIT = 7.5
HUMIDITY_LIMIT = 90
DEW_POINT_LIMIT = 2
PRODUCER_TOPIC = "site_danger_alerts"
CONSUMER_TOPIC = "weather_sensor_data"

conf_consumer = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "alert-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
conf_producer = {
    "bootstrap.servers": "localhost:9092",
}

consumer = Consumer(conf_consumer)
producer = Producer(conf_producer)

consumer.subscribe([CONSUMER_TOPIC])


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key().decode('utf-8')}: {err}")

    else:
        print(f"""
        Delivered key={msg.key()} 
        to topic={msg.topic()} partition={msg.partition()}
        at offset={msg.offset()}
        """)


def danger_alert(weather_data):
    df = pd.DataFrame(weather_data)

    # filtering data based on alert types
    heat = df[df["temperature_2m"] > HEAT_LIMIT]
    heat["alert_type"] = "heat"
    heat["value"] = heat["temperature_2m"]

    storm = df[df["wind_gusts_10m"] > STORM_LIMIT]
    storm["alert_type"] = "storm"
    storm["value"] = storm["wind_gusts_10m"]

    heavy_rain = df[df["precipitation"] > HEAVY_RAIN_LIMIT]
    heavy_rain["alert_type"] = "heavy_rain"
    heavy_rain["value"] = heavy_rain["precipitation"]

    condensation = df[
        (df["relative_humidity_2m"] > HUMIDITY_LIMIT)
        & ((df["temperature_2m"] - df["dew_point_2m"]) < DEW_POINT_LIMIT)
    ]
    condensation["alert_type"] = "condensation"
    condensation["value"] = condensation["relative_humidity_2m"]

    # ignoring index because 2 alerts from same index can be in Dataframe
    alerts = pd.concat([heat, storm, heavy_rain, condensation], ignore_index=True)

    return alerts[["site_code", "date", "alert_type", "value"]]


def process_msg(msg):
    weather_data = json.loads(msg.value().decode("utf-8"))
    # converting to dict and back to json because we need to send it as bytes
    alerts = danger_alert(weather_data).to_dict(orient="records")

    if alerts:
        producer.produce(
            topic=PRODUCER_TOPIC,
            key=msg.key(),
            value=json.dumps(alerts).encode("utf-8"),
            callback=delivery_report,
        )  # we are going to produce alerts to a different topic

        producer.flush()

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
                process_msg(msg)

            except Exception as e:
                site_code = msg.key().decode("utf-8")
                print(f"Retrying site={site_code}: {e}, offset={msg.offset()}")
                consumer.seek(
                    TopicPartition(msg.topic(), msg.partition(), msg.offset())
                )  # seek to the same offset, loop will keep retrying that message
                time.sleep(5)

    except KeyboardInterrupt:
        print("Alert Producer / Weather data consumer stopped")

    finally:
        print("Alert Producer / Weather data consumer closed all messages consumed")
        consumer.close()


if __name__ == "__main__":
    db.create_danger_alerts_table()
    run()
