from datetime import datetime as dt
from datetime import timedelta as td
from retry_requests import retry
from sqlalchemy import engine

import openmeteo_requests
import pandas as pd
import requests_cache

# retries and backoff factors can handle errors
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

url = "https://api.open-meteo.com/v1/forecast"


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
            "hourly": [
                "temperature_2m",
                "relative_humidity_2m",
                "global_tilted_irradiance_instant",
            ],
            "timezone": "auto",
            "start_date": start_date,
            "end_date": end_date,
        }

        yield row["site_code"], params


def fetch_weather_data(params):
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_global_tilted_irradiance_instant = hourly.Variables(2).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
    }

    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["global_tilted_irradiance_instant"] = (
        hourly_global_tilted_irradiance_instant
    )

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    print("\nHourly data\n", hourly_dataframe)


# testing
if __name__ == "__main__":
    result = parameter_builder("meta_data.csv")
    for i, param in enumerate(result):
        if i > 0:
            break
        fetch_weather_data(params=param[1])
