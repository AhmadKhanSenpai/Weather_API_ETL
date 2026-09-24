from dotenv import load_dotenv
from sqlalchemy import create_engine, text

import os
import pandas as pd

load_dotenv()

# using dot_env so i dont need to hard code my sensitive info
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
        global_tilted_irradiance_instant DOUBLE PRECISION,

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
        global_tilted_irradiance_instant
    )
    VALUES (
        :site_code,
        :date,
        :temperature_2m,
        :relative_humidity_2m,
        :shortwave_radiation
    )

    ON CONFLICT (site_code, date)
    DO NOTHING;
    """

    with engine.begin() as conn:
        conn.execute(text(query), weather_data)


if __name__ == "__main__":
    create_tracking_table()
    create_weather_table()
