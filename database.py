from dotenv import load_dotenv
from sqlalchemy import create_engine, text

import os
import pandas as pd

load_dotenv()

# fetching environment variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


# creating engine for context manager
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


def create_weather_table():
    query = """
    CREATE TABLE IF NOT EXISTS weather_hourly (
        site_code VARCHAR(50) NOT NULL,
        date TIMESTAMPTZ NOT NULL,
        temperature_2m DOUBLE PRECISION,
        relative_humidity_2m DOUBLE PRECISION,
        shortwave_radiation DOUBLE PRECISION,
        cloud_cover DOUBLE PRECISION, 
        wind_speed_10m DOUBLE PRECISION, 
        wind_speed_80m DOUBLE PRECISION, 
        wind_gusts_10m DOUBLE PRECISION,
        wind_direction_80m DOUBLE PRECISION, 
        precipitation DOUBLE PRECISION, 
        dew_point_2m DOUBLE PRECISION,

        FOREIGN KEY (site_code)
        REFERENCES tracker(site_code),

        UNIQUE(site_code, date)
    );
    """

    with engine.begin() as conn:
        conn.execute(text(query))

    print("weather_hourly table created successfully.")


def create_tracking_table():
    query = """
    CREATE TABLE IF NOT EXISTS tracker(
    site_code VARCHAR(50) NOT NULL,
    status BOOLEAN NOT NULL,
    PRIMARY KEY (site_code)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(query))

    print("tracker table created successfully")


def create_power_mix_table():
    query = """
    CREATE TABLE IF NOT EXISTS sites_power_mix(
        site_code VARCHAR(50) NOT NULL,
        date DATE NOT NULL,
        avg_shortwave_radiation DOUBLE PRECISION,
        avg_wind_speed_80m DOUBLE PRECISION,
        sun_ok_hours INT,
        wind_ok_hours INT,
        DG_ok_hours INT,

        PRIMARY KEY (site_code, date)

    );"""

    with engine.begin() as conn:
        conn.execute(text(query))

    print("sites_power_mix table created successfully")


def create_danger_alerts_table():
    query = """
    CREATE TABLE IF NOT EXISTS site_danger_alerts(
        site_code VARCHAR(50) NOT NULL,
        date TIMESTAMPTZ NOT NULL,
        alert_type VARCHAR(50) NOT NULL,
        value DOUBLE PRECISION NOT NULL,

        PRIMARY KEY (site_code, date, alert_type)
    );
    """

    with engine.begin() as conn:
        conn.execute(text(query))

    print("site_danger_alerts table created successfully")


def update_tracking_status(site_code, status):
    query = """
    INSERT INTO tracker (site_code, status)
    VALUES (:site_code, :status)

    ON CONFLICT (site_code)
    DO UPDATE SET status = EXCLUDED.status;
    """

    data = {"site_code": site_code, "status": status}

    with engine.begin() as conn:
        conn.execute(text(query), data)


def read_failed_sites():
    query = """
    SELECT * 
    FROM tracker 
    WHERE status = false
    """
    return pd.read_sql_query(query, engine)


def read_parsed_sites():
    query = """
    SELECT *
    FROM tracker
    WHERE status = true
    """
    return pd.read_sql_query(query, engine)


def insert_weather_data(weather_data):

    query = """
    INSERT INTO weather_hourly (
        site_code,
        date,
        temperature_2m,
        relative_humidity_2m,
        shortwave_radiation,
        cloud_cover, 
        wind_speed_10m, 
        wind_speed_80m, 
        wind_gusts_10m,
        wind_direction_80m, 
        precipitation, 
        dew_point_2m
    )
    VALUES (
        :site_code,
        :date,
        :temperature_2m,
        :relative_humidity_2m,
        :shortwave_radiation,
        :cloud_cover, 
        :wind_speed_10m, 
        :wind_speed_80m, 
        :wind_gusts_10m,
        :wind_direction_80m, 
        :precipitation, 
        :dew_point_2m
    )

    ON CONFLICT (site_code, date)
    DO NOTHING;
    """

    with engine.begin() as conn:
        conn.execute(text(query), weather_data)


def insert_power_mix(aggregate_data):
    if aggregate_data.empty:
        return

    query = """
    INSERT INTO sites_power_mix(
        site_code,
        date,
        avg_shortwave_radiation,
        avg_wind_speed_80m,
        sun_ok_hours,
        wind_ok_hours,
        DG_ok_hours
    )
    VALUES (
        :site_code,
        :date,
        :shortwave_radiation,
        :wind_speed_80m,
        :sun_ok,
        :wind_ok,
        :DG_ok
    )
    ON CONFLICT (site_code, date)
    DO NOTHING;
    """

    with engine.begin() as conn:
        conn.execute(text(query), aggregate_data.to_dict(orient="records"))


def insert_danger_alerts(alerts):
    if alerts.empty:
        return

    query = """
    INSERT INTO site_danger_alerts(
        site_code,
        date,
        alert_type,
        value
    )
    VALUES (
        :site_code,
        :date,
        :alert_type,
        :value
    )
    ON CONFLICT (site_code, date, alert_type)
    DO NOTHING;
    """
    with engine.begin() as conn:
        conn.execute(text(query), alerts.to_dict(orient="records"))
