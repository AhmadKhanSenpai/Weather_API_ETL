import json
import pandas as pd
import database as db
from producer import producer, delivery_report, TOPIC
from data_pipeline import fetch_site_weather


def retry_failed_sites(path):
    failed = db.read_failed_sites()

    if failed.empty:
        print("There are no failed sites to retry.")
        return

    # filter out the failed sites
    df = pd.read_csv(path)
    mask = df["site_code"].isin(failed["site_code"])
    df_failed = df.loc[mask]

    for row in df_failed.itertuples(index=False):
        site_code = row.site_code
        lat = row.latitude
        lon = row.longitude

        result_dict = fetch_site_weather(
            site_code=site_code, latitude=lat, longitude=lon
        )

        if result_dict is not None:
            db.update_tracking_status(site_code=site_code, status=True)
            print(f"[OK] {site_code} — {len(result_dict)} rows inserted")

        else:
            db.update_tracking_status(site_code=site_code, status=False)
            print(f"[FAIL] {site_code}")

        producer.produce(
            TOPIC,
            key=site_code.encode("utf-8"),
            value=json.dumps(result_dict).encode("utf-8"),
            on_delivery=delivery_report,
        )

        producer.poll(0)

    producer.flush()
    print(f"Retrying {len(df_failed)} failed sites.")


if __name__ == "__main__":
    retry_failed_sites("meta_data.csv")
