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


def create_sites_table():
    query = """
    CREATE TABLE IF NOT EXISTS sites (
        site_code VARCHAR(50) PRIMARY KEY,
        latitude DOUBLE PRECISION NOT NULL,
        longitude DOUBLE PRECISION NOT NULL
    );
    """

    with engine.begin() as conn:
        conn.execute(text(query))

    print("sites table created successfully.")


def create_weather_table():
    query = """
    CREATE TABLE IF NOT EXISTS weather_hourly (
        site_code VARCHAR(50) NOT NULL,
        date TIMESTAMPTZ NOT NULL,
        temperature_2m DOUBLE PRECISION,
        relative_humidity_2m DOUBLE PRECISION,
        global_tilted_irradiance_instant DOUBLE PRECISION,

        FOREIGN KEY (site_code)
        REFERENCES sites(site_code),

        UNIQUE(site_code, date)
    );
    """

    with engine.begin() as conn:
        conn.execute(text(query))

    print("weather_hourly table created successfully.")


def create_tracking_table():
    query = """
    CREATE TABLE IF NOT EXISTS tracker(
    site_code VARCHAR(50) REFERENCES sites(site_code),
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
    with engine.begin() as conn:
        conn.execute(text(query), {"site_code": site_code, "status": status})


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


def insert_meta_data(df):
    """This funtion will deal with the duplicate values if inserted"""

    query = """
    INSERT INTO sites (site_code, latitude, longitude)
    VALUES (:site_code, :latitude, :longitude)

    ON CONFLICT (site_code)
    DO NOTHING;
    """

    data = df.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(text(query), data)

    print("Metadata inserted successfully.")


def insert_weather_data(df):

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
        :global_tilted_irradiance_instant
    )

    ON CONFLICT (site_code, date)
    DO NOTHING;
    """

    data = df.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(text(query), data)


if __name__ == "__main__":
    create_sites_table()
    create_weather_table()
