import pandas as pd
from geopy.distance import geodesic

# Load the data
file_path = 'vehicle_lat_long_1_Jan_2025_Pune.csv'
data = pd.read_csv(file_path)

# Target coordinates
target_coords = (18.5301577, 73.9212209)

# Calculate distance to target and add as a new column
data['distance_to_target'] = data.apply(
    lambda row: geodesic((row['latitude'], row['longitude']), target_coords).meters, axis=1
)

# Save the updated DataFrame back to the same file
data.to_csv(file_path, index=False)

print(f"Distances calculated and saved to {file_path}")
