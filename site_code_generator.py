import random
import string

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# Change this to whatever number of sites you want
NUM_SITES = 100_000

OUTPUT_FILE = "meta_data.csv"

# Use a fixed seed if you want the same random coordinates
# every time you run the script.
random.seed(42)


SOUTH_ASIA = [
    "Afghanistan",
    "Bangladesh",
    "Bhutan",
    "India",
    "Maldives",
    "Nepal",
    "Pakistan",
    "Sri Lanka",
]


def generate_site_code(number):
    """
    Generate a unique site code.

    Examples:
        0       -> AA000
        1       -> AA001
        999     -> AA999
        1000    -> AB000
        1999    -> AB999
        26000   -> BA000
    """

    numeric_part = number % 1000
    letter_number = number // 1000

    first_letter = string.ascii_uppercase[(letter_number // 26) % 26]

    second_letter = string.ascii_uppercase[letter_number % 26]

    return f"{first_letter}{second_letter}{numeric_part:03d}"


def generate_land_point(land):
    """Generate a random point that falls inside South Asian land."""

    minx, miny, maxx, maxy = land.total_bounds

    while True:
        longitude = random.uniform(minx, maxx)
        latitude = random.uniform(miny, maxy)

        point = Point(longitude, latitude)

        if land.contains(point).any():
            return latitude, longitude


# --------------------------------------------------
# Load country boundaries
# --------------------------------------------------

countries = gpd.read_file(
    "https://naturalearth.s3.amazonaws.com/110m_cultural/"
    "ne_110m_admin_0_countries.zip"
)


# --------------------------------------------------
# Select South Asian countries
# --------------------------------------------------

south_asia = countries[countries["NAME"].isin(SOUTH_ASIA)]

land = south_asia[["NAME", "geometry"]]


# --------------------------------------------------
# Generate sites
# --------------------------------------------------

sites = []

for i in range(NUM_SITES):

    site_code = generate_site_code(i)

    latitude, longitude = generate_land_point(land)

    sites.append(
        {
            "site_code": site_code,
            "latitude": latitude,
            "longitude": longitude,
        }
    )


# --------------------------------------------------
# Save CSV
# --------------------------------------------------

df = pd.DataFrame(sites)

df.to_csv(OUTPUT_FILE, index=False)

print(f"{NUM_SITES:,} sites generated successfully.")
print(f"Saved to: {OUTPUT_FILE}")
