import pandas as pd
import random
import string


def generate_site_code(existing):
    chars = string.ascii_uppercase + string.digits
    while True:
        code = "".join(random.choices(chars, k=5))
        if code not in existing:
            existing.add(code)
            return code


# Approximate bounding box of Pakistan
# Latitude: ~23.5 to 37.0
# Longitude: ~60.5 to 77.5
sites = []
codes = set()

while len(sites) < 10000:
    sites.append(
        {
            "site_code": generate_site_code(codes),
            "latitude": round(random.uniform(23.5, 37.0), 6),
            "longitude": round(random.uniform(60.5, 77.5), 6),
        }
    )

df = pd.DataFrame(sites)

file_path = "meta_data.csv"
df.to_csv(file_path, index=False)
