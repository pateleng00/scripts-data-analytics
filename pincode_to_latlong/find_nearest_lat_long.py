import pandas as pd
from haversine import haversine
from mysql.connector import MySQLConnection

from python_mysql_dbconfig import read_db_config

dbconfig = read_db_config()
conn = MySQLConnection(**dbconfig)
cursor = conn.cursor()
# Load pin codes and corresponding latitude and longitude from Excel file into a pandas DataFrame
df_excel = pd.read_excel('pin_codes_with_lat_long.xlsx')


# Query to fetch latitude, longitude, and hub information from the database
query = """
SELECT FLD_HubId, FLD_HubName, FLD_latitude, FLD_longitude
FROM db.TBL_HUBMASTER
WHERE FLD_IsThirdParty = 0 AND FLD_HubStatus = 1;
"""

cursor.execute(query)
rows = cursor.fetchall()
# Assuming you have latitude and longitude data from your database as lists
db_hubs = [(row[0], row[1], row[2], row[3]) for row in rows]


# Function to find nearest hub
def find_nearest_hub(lat, lon):
    min_distance = float('inf')
    nearest_hub = None
    nearest_hub_lat = None
    nearest_hub_lon = None

    for hub_id, hub_name, hub_lat, hub_lon in db_hubs:
        # Convert latitude and longitude to float
        hub_lat = float(hub_lat)
        hub_lon = float(hub_lon)

        distance = haversine((float(lat), float(lon)), (hub_lat, hub_lon))
        if distance < min_distance:
            min_distance = distance
            nearest_hub = (hub_id, hub_name)
            nearest_hub_lat = hub_lat
            nearest_hub_lon = hub_lon

    return nearest_hub, min_distance, nearest_hub_lat, nearest_hub_lon


# Apply the function to each row in the Excel DataFrame
nearest_hub_data = []

for index, row in df_excel.iterrows():
    lat, lon = row['Latitude'], row['Longitude']
    nearest_hub, nearest_hub_distance, nearest_hub_lat, nearest_hub_lon = find_nearest_hub(lat, lon)
    if nearest_hub:
        nearest_hub_data.append(
            (nearest_hub[0], nearest_hub[1], nearest_hub_distance, nearest_hub_lat, nearest_hub_lon))
    else:
        nearest_hub_data.append(('N/A', 'N/A', 'N/A', 'N/A', 'N/A'))  # or any other default value

# Add the nearest hub information to the DataFrame
df_excel['Nearest_Hub_ID'] = [hub[0] for hub in nearest_hub_data]
df_excel['Nearest_Hub_Name'] = [hub[1] for hub in nearest_hub_data]
df_excel['Nearest_Hub_Distance'] = [hub[2] for hub in nearest_hub_data]
df_excel['Nearest_Hub_Latitude'] = [hub[3] for hub in nearest_hub_data]
df_excel['Nearest_Hub_Longitude'] = [hub[4] for hub in nearest_hub_data]

# Save the DataFrame with the additional information to a new Excel file
df_excel.to_excel('excel_data_with_nearest_hub_info.xlsx', index=False)

if __name__ == '__main__':
    print("")
