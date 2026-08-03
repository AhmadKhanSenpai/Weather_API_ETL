import random
import string
import pandas as pd

# -----------------------------
# Configuration
# -----------------------------
NUM_SITES = 10_000

LAT_MIN = 23.5
LAT_MAX = 37.0

LON_MIN = 60.5
LON_MAX = 77.5


def generate_site_code(existing_codes):
    """Generate a unique 5-character alphanumeric site code."""
    chars = string.ascii_uppercase + string.digits

    while True:
        code = "".join(random.choices(chars, k=5))

        if code not in existing_codes:
            existing_codes.add(code)
            return code


# Keep track of uniqueness
used_codes = set()
used_coordinates = set()

sites = []

while len(sites) < NUM_SITES:

    latitude = round(random.uniform(LAT_MIN, LAT_MAX), 6)
    longitude = round(random.uniform(LON_MIN, LON_MAX), 6)

    # Skip duplicate coordinate pairs
    if (latitude, longitude) in used_coordinates:
        continue

    used_coordinates.add((latitude, longitude))

    sites.append(
        {
            "site_code": generate_site_code(used_codes),
            "latitude": latitude,
            "longitude": longitude,
        }
    )

# Create DataFrame
df = pd.DataFrame(sites)

# Save to CSV
df.to_csv("meta_data.csv", index=False)

print(f"Successfully generated {len(df):,} unique sites.")
