import json
import pandas as pd
import database as db
from producer import producer, delivery_report, TOPIC


def retry_failed_sites(path):
    failed = (
        db.read_failed_sites()
    )  # status = 'retryable' only — 'permanent' never comes back here

    if failed.empty:
        print("No retryable sites to requeue.")
        return

    df = pd.read_csv(path)
    mask = df["site_code"].isin(failed["site_code"])
    df_failed = df.loc[mask]

    for row in df_failed.itertuples(index=False):
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
        producer.poll(0)

    producer.flush()
    print(f"Re-queued {len(df_failed)} retryable site(s).")


if __name__ == "__main__":
    retry_failed_sites("meta_data.csv")
