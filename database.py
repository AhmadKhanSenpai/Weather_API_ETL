import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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

    print("Weather data inserted successfully.")


if __name__ == "__main__":
    create_sites_table()
    create_weather_table()
