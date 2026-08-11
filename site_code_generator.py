import random
import string
import pandas as pd

# -----------------------------
# Configuration
# -----------------------------
NUM_SITES = 10_000

# Pakistan bounding box
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


used_codes = set()

sites = []

for _ in range(NUM_SITES):

    sites.append(
        {
            "site_code": generate_site_code(used_codes),
            "latitude": round(random.uniform(LAT_MIN, LAT_MAX), 6),
            "longitude": round(random.uniform(LON_MIN, LON_MAX), 6),
            "status": pd.NA,
        }
    )

df = pd.DataFrame(sites)

df.to_csv("meta_data.csv", index=False)

print(f"Successfully generated {len(df):,} sites.")
