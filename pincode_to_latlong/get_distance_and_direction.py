import pandas as pd
from geopy.distance import geodesic
from math import atan2, degrees, sin, cos


def calculate_bearing(origin, destination):
    lat1, lon1 = origin
    lat2, lon2 = destination

    # Calculate the difference between longitudes
    delta_lon = lon2 - lon1

    # Calculate bearing
    y = sin(delta_lon) * cos(lat2)
    x = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(delta_lon)
    bearing = atan2(y, x)

    # Convert bearing from radians to degrees
    bearing = degrees(bearing)
    bearing = (bearing + 360) % 360  # Normalize to 0-360 degrees

    return bearing


def bearing_to_direction(bearing):
    directions = ['North', 'North East', 'East', 'South East', 'South', 'South West', 'West', 'North West', 'North']
    index = round(bearing / 45) % 8
    return directions[index]


# Read the Excel file
excel_file = "Varanasi.xlsx"
df = pd.read_excel(excel_file)

# Origin coordinates (constant)
origin = (25.31403323656833, 82.96241494381286)  # Replace with actual origin latitude and longitude

# Iterate over rows and calculate distance, bearing, and direction
distances = []
bearings = []
directions = []
for index, row in df.iterrows():
    destination = (row['Latitude'], row['Longitude'])
    distance = geodesic(origin, destination).kilometers
    bearing = calculate_bearing(origin, destination)
    direction = bearing_to_direction(bearing)
    distances.append(distance)
    bearings.append(bearing)
    directions.append(direction)

# Update the DataFrame with distances, bearings, and directions
df['Distance (km)'] = distances
df['Bearing (degrees)'] = bearings
df['Direction'] = directions

# Write the updated DataFrame back to the Excel file
df.to_excel(excel_file, index=False)
