import os
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# using dot_env so i dont need to hard code my sensitive info
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


# create a table in database if it does not exist
def create_weather_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_hourly (
        site_code VARCHAR(50) NOT NULL,
        date TIMESTAMPTZ NOT NULL,
        temperature_2m DOUBLE PRECISION,
        relative_humidity_2m DOUBLE PRECISION,
        global_tilted_irradiance_instant DOUBLE PRECISION,

        PRIMARY KEY (site_code, date)
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_query))

    print("weather_hourly table is ready.")


def insert_weather_data(df):
    df.to_sql(name="weather_hourly", con=engine, if_exists="append", index=False)


if __name__ == "__main__":
    create_weather_table()
