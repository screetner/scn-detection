import os
import math
import matplotlib.pyplot as plt

from src.tloc_decoder import read_location_binary


def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points on the Earth using the Haversine formula.
    Returns distance in kilometers.
    """
    R = 6371  # Earth's radius in kilometers

    # Convert degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Differences
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def location_distance_histogram():
    location_file_path = os.path.join(os.path.dirname(__file__), '../../tloc/1731657012941.tloc')
    locations = read_location_binary(location_file_path)

    distances = []

    for i in range(len(locations) - 1):
        loc1 = locations[i]
        loc2 = locations[i + 1]

        # Convert haversine distance to meters
        distance_km = haversine(loc1['latitude'], loc1['longitude'], loc2['latitude'], loc2['longitude'])
        distance_m = distance_km * 1000  # Convert km to meters
        distances.append(distance_m)

    # Plot the histogram
    plt.hist(distances, bins=20, edgecolor='black', alpha=0.7)
    plt.title('Histogram of Distances Between Consecutive Locations')
    plt.xlabel('Distance (m)')  # Update label to meters
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.show()


if __name__ == "__main__":
    location_distance_histogram()
