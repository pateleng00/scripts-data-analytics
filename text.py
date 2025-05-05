import requests
import openpyxl

# API endpoint and headers
url = 'https://windblade.moeving.com/paris/tracking/dashboard-view'
headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJha2FzaC5rdW1hckBtb2V2aW5nLmNvbSIsImV4cCI6MTczMjUzMDk3MH0.TfMCy4l85YWAlM30fc2RA5rKKHwUQ5ZJtr8ujBPe13M',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'origin': 'https://moeving.app',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://moeving.app/',
    'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
}

# Make the API request
response = requests.post(url, headers=headers, json={})

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    if data.get('success') and 'data' in data:
        vehicle_data = data['data']

        # Create an Excel workbook and sheet
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.title = "Vehicle Data"

        # Write headers to the Excel sheet
        headers = ["Vehicle No", "Latitude", "Longitude", "Vehicle Type", "GPS Updated Time"]
        sheet.append(headers)

        # Write data to the Excel sheet
        for vehicle in vehicle_data:
            row = [
                vehicle.get("vehicleNo"),
                vehicle.get("latitude"),
                vehicle.get("longitude"),
                vehicle.get("vehicleType"),
                vehicle.get("gpsUpdatedTime")
            ]
            sheet.append(row)

        # Save the Excel file
        workbook.save("vehicle_data.xlsx")
        print("Data has been written to vehicle_data.xlsx successfully.")
    else:
        print("No data found in the response.")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
