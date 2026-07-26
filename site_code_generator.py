"""
generate_sites.py

Generates N random site codes with latitude/longitude coordinates
scattered within a given radius range around Islamabad, Pakistan.

Output: sites.csv with columns -> site_code, latitude, longitude
"""

import random
import math
import csv

# Islamabad approximate center coordinates
CENTER_LAT = 33.6844
CENTER_LON = 73.0479

MIN_RADIUS_KM = 25
MAX_RADIUS_KM = 100

NUM_SITES = 100
OUTPUT_FILE = "sites.csv"

random.seed(42)  # fixed seed -> reproducible results every run


def random_point(center_lat, center_lon, min_km, max_km):
    """
    Returns a random (lat, lon) point located between min_km and max_km
    away from the given center, in a random direction.
    """
    r_km = random.uniform(min_km, max_km)
    angle = random.uniform(0, 2 * math.pi)

    # ~111 km per degree of latitude
    dlat = (r_km / 111.0) * math.cos(angle)
    # longitude degrees shrink as you move away from the equator,
    # so we adjust using cos(latitude)
    dlon = (r_km / (111.0 * math.cos(math.radians(center_lat)))) * math.sin(angle)

    return round(center_lat + dlat, 5), round(center_lon + dlon, 5)


def random_site_code():
    """Generates a random 5-character alphanumeric code (uppercase)."""
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(random.choice(chars) for _ in range(5))


def generate_sites(num_sites):
    sites = []
    for _ in range(num_sites):
        code = random_site_code()
        lat, lon = random_point(CENTER_LAT, CENTER_LON, MIN_RADIUS_KM, MAX_RADIUS_KM)
        sites.append((code, lat, lon))
    return sites


def save_to_csv(sites, filename):
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["site_code", "latitude", "longitude"])
        writer.writerows(sites)


if __name__ == "__main__":
    sites = generate_sites(NUM_SITES)
    save_to_csv(sites, OUTPUT_FILE)
    print(f"Generated {len(sites)} sites -> {OUTPUT_FILE}")
