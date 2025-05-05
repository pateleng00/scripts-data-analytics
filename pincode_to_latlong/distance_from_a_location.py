import pandas as pd
from geopy.distance import geodesic

# Read the Excel file for each sheet
sheet1_df = pd.read_excel('pin_codes_with_lat_long.xlsx', sheet_name='Sheet1')

# Define your specific latitude and longitude
specific_lat = 19.12935667423027  # Example latitude
specific_long = 72.93100299426884  # Example longitude


# Function to calculate distance between two points
def calculate_distance(row):
    point = (row['Latitude'], row['Longitude'])
    return geodesic((specific_lat, specific_long), point).kilometers


# Calculate distance for each sheet
sheet1_df['Distance'] = sheet1_df.apply(calculate_distance, axis=1)

# Write back to the Excel file
with pd.ExcelWriter('switch_Mumbai.xlsx') as writer:
    sheet1_df.to_excel(writer, sheet_name='Sheet1', index=False)

