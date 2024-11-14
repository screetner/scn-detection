import struct

def read_location_binary(path):
    locations = []

    with open(path, 'rb') as f:
        header = f.read(4)
        num_locations = struct.unpack('i', header)[0]

        for _ in range(num_locations):
            location_data = f.read(24)

            if not location_data or len(location_data) < 24:
                break

            timestamp, lat, lng = struct.unpack('qdd', location_data)

            locations.append({
                'timestamp': timestamp,
                'latitude': lat,
                'longitude': lng,
            })
        
    return locations