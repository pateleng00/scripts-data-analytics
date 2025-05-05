import pandas as pd
import requests


# Function to get latitude and longitude for a pin code
def get_lat_long(pincode):
    try:
        # API endpoint
        url = f"https://maps.googleapis.com/maps/api/geocode/json?address={pincode}&key=abcd"

        # Send request
        response = requests.get(url)
        data = response.json()
        # print(data)
        # Extract latitude and longitude
        lat = data['results'][0]['geometry']['location']['lat']
        lng = data['results'][0]['geometry']['location']['lng']
        return lat, lng
    except Exception as e:
        print(f"Error geocoding pin code {pincode}: {e}")
        return None, None


# Load pin codes from Excel file into a pandas DataFrame
df_pins = pd.read_excel('pin_codes.xlsx')

# Get latitude and longitude for each pin code
latitudes = []
longitudes = []

for pincode in df_pins['Pincode']:
    lat, lng = get_lat_long(pincode)
    latitudes.append(lat)
    longitudes.append(lng)

# Add latitude and longitude columns to DataFrame
df_pins['Latitude'] = latitudes
df_pins['Longitude'] = longitudes

# Save DataFrame back to Excel file with latitude and longitude
df_pins.to_excel('pin_codes_with_lat_long.xlsx', index=False)

if __name__ == '__main__':
    print("")
