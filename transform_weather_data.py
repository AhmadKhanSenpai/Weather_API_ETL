from datetime import datetime as dt
from datetime import timedelta as td
import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry


# I used the list of dicts first but then i remember why flood memory with a list in case of large data we would need
# a generator
def parameter_builder(file_path):
    df = pd.read_csv(file_path)

    # calcualting date based on current date
    end_date = dt.now().strftime("%Y-%m-%d")
    diff = dt.now() - td(days=7)
    start_date = diff.strftime("%Y-%m-%d")

    for _, row in df.iterrows():
        params = {
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "hourly": ["temperature_2m", "relative_humidity_2m", "shortwave_radiation"],
            "timezone": "auto",
            "start_date": start_date,
            "end_date": end_date,
        }

        yield row["site_code"], params


# testing
if __name__ == "__main__":
    result = parameter_builder("meta_data.csv")
    for i, param in enumerate(result):
        if i > 0:
            break
        print(i, param)
