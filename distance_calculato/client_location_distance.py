import json
import time
from datetime import datetime, timedelta

import mysql
import requests
from geopy.distance import geodesic
from httplib2 import Http
from mysql.connector import MySQLConnection

conn = mysql.connector.connect(host="localhost:3306", user="live_user", password="1PC^&uO$5",
                               database="mydb")
cursor = conn.cursor()

query = """select tcm.FLD_NAME , tcrl.FLD_REPORTING_LOCATION_SHORT_NAME, tdfdd.FLD_DRIVER_CHECKIN_TIME, tvd.FLD_RC_NUMBER, 
tdfdd.FLD_DRIVER_USER_ID, tcrl.FLD_LAT, tcrl.FLD_LON, tdfdd.FLD_DRIVER_CHECKIN_LAT, tdfdd.FLD_DRIVER_CHECKIN_LON, 
tld.FLD_LOCATION_ID, tdd.FLD_MOEVING_HUB_ID, tcrl.FLD_COMPANY_REPORTING_LOCATION_ID, tcm.FLD_COMPANY_ID, tdfdd.FLD_DEMAND_FULFILMENT_DRIVER_ID from TBL_DEMAND_FULFILMENT_DRIVER_DETAILS tdfdd 
inner join TBL_DEMAND_TEMPLATE tdt on tdt.FLD_DEMAND_TEMPLATE_ID  = tdfdd.FLD_DEMAND_TEMPLATE_ID 
inner join TBL_VEHICLE_DETAILS tvd on tvd.FLD_VEHICLE_ID = tdfdd.FLD_VEHICLE_ID 
inner join TBL_DRIVER_DETAILS tdd on tdd.FLD_DRIVER_USER_ID = tdfdd.FLD_DRIVER_USER_ID 
inner join TBL_USER_DETAILS tud on tud.FLD_USER_ID  = tdd.FLD_DRIVER_USER_ID
inner join TBL_COMPANY_REPORTING_LOCATION tcrl  on tdt.FLD_REPORTING_LOCATION_ID  = tcrl.FLD_COMPANY_REPORTING_LOCATION_ID 
inner join TBL_LOCATION_DETAILS tld on tld.FLD_LOCATION_ID  = tdt.FLD_REPORTING_CITY 
inner JOIN TBL_COMPANY_MASTER tcm on tcm.FLD_COMPANY_ID  = tdt.FLD_CLIENT_COMPANY_ID 
where date(FLD_DRIVER_CHECKIN_TIME) = CURRENT_DATE() - interval 1 day;"""
cursor.execute(query)
results = cursor.fetchall()


def calculate_distance(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).kilometers


def get_distance_between_client_and_vehicle_location(vehicle_number, start_time, end_time):
    api_url = 'https://windblade.moeving.com/paris/tracking/time-range'
    bearer_token = ('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9'
                    '.eyJzdWIiOiJha2FzaC5rdW1hckBtb2V2aW5nLmNvbSIsImV4cCI6MTMyMTg5ODQwODh9'
                    '.rIBeWuS9mnAcwHVgmSHSe0OyCl2Po8YNcY1X7Hb0TEg')

    params = {
        "vehicleNo": vehicle_number,
        "startTime": start_time,
        "endTime": end_time
    }

    # Convert payload to JSON string

    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'content_type': 'application/json'
    }
    response = requests.post(api_url, headers=headers, json=params)
    return response


for result in results:
    client = result[0]
    reporting_location = result[1]
    checkin_time = result[2]
    vehicle_no = result[3]
    driver_id = result[4]
    reporting_location_lat = result[5]
    reporting_location_lon = result[6]
    checkin_lat = result[7]
    checkin_lon = result[8]
    city_id = result[9]
    driver_hub_id = result[10]
    client_id = result[12]
    client_reporting_location_id = result[11]
    demand_fullfillment_driver_id = result[13]
    initial_timestamp = datetime.strptime(str(checkin_time), '%Y-%m-%d %H:%M:%S')
    start_time = initial_timestamp - timedelta(minutes=5)
    end_time = initial_timestamp + timedelta(minutes=5)
    vehicle_location_object = get_distance_between_client_and_vehicle_location(vehicle_no, str(start_time),
                                                                               str(end_time))
    vehicle_location_data = vehicle_location_object.json()

    if vehicle_location_object.status_code == 200:
        data = vehicle_location_data.get('data')
        coordinates_map = data.get('coordinatesMap')
        if coordinates_map:
            nearest_timestamp = None
            min_diff = None
            for timestamp_str, coordinates in coordinates_map.items():
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                diff = abs((checkin_time - timestamp).total_seconds())
                if min_diff is None or diff < min_diff:
                    min_diff = diff
                    nearest_timestamp = timestamp_str
            if nearest_timestamp:
                nearest_coordinates = coordinates_map[nearest_timestamp]
                vehicle_lat = nearest_coordinates.get('latitude')
                vehicle_lon = nearest_coordinates.get('longitude')
                vehicle_distance = calculate_distance(vehicle_lat, vehicle_lon, reporting_location_lat,
                                                      reporting_location_lon)
                driver_distance = calculate_distance(checkin_lat, checkin_lon, reporting_location_lat,
                                                     reporting_location_lon)
                vehicle_distance = round(vehicle_distance, 2)
                driver_distance = round(driver_distance, 2)
                print("--------------------------------+++++++-------------------------------")
                insert_query = """INSERT INTO moevingdb.checkin_time_distance_client_vehicle_location (
                demand_fullfilment_driver_id, demand_city_id, driver_hub_id, driver_id, vehicle_no, client_id,
                client_reporting_location_id, reporting_location_lat, reporting_location_lon, checkin_time, checkin_lat, checkin_lon,
                vehicle_distance, driver_distance, nearest_timestamp, vehicle_nearest_lat, vehicle_nearest_lon) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                cursor.execute(insert_query, (
                    demand_fullfillment_driver_id, city_id, driver_hub_id, driver_id, vehicle_no, client_id,
                    client_reporting_location_id, reporting_location_lat, reporting_location_lon, checkin_time,
                    checkin_lat, checkin_lon, vehicle_distance, driver_distance, nearest_timestamp,
                    vehicle_lat, vehicle_lon))
                conn.commit()
                time.sleep(2)
        else:
            insert_query = """INSERT INTO moevingdb.checkin_time_distance_client_vehicle_location (
            demand_fullfilment_driver_id, demand_city_id, driver_hub_id, driver_id, vehicle_no, client_id,
            client_reporting_location_id, reporting_location_lat, reporting_location_lon, checkin_time, checkin_lat,
            checkin_lon) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            cursor.execute(insert_query, (
                demand_fullfillment_driver_id, city_id, driver_hub_id, driver_id, vehicle_no, client_id,
                client_reporting_location_id, reporting_location_lat, reporting_location_lon, checkin_time,
                checkin_lat, checkin_lon,))
            conn.commit()
    elif vehicle_location_object.status_code != 200:
        error = vehicle_location_data['message']
        error = error['text']
        insert_query = """INSERT INTO moevingdb.checkin_time_distance_client_vehicle_location (
        demand_fullfilment_driver_id, demand_city_id, driver_hub_id, driver_id, vehicle_no, client_id,
        client_reporting_location_id, reporting_location_lat, reporting_location_lon, checkin_time, checkin_lat,
        checkin_lon, error) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (
            demand_fullfillment_driver_id, city_id, driver_hub_id, driver_id, vehicle_no, client_id,
            client_reporting_location_id, reporting_location_lat, reporting_location_lon, checkin_time,
            checkin_lat, checkin_lon, error))
        conn.commit()

today = datetime.today().date()
yesterday = today - timedelta(days=1)
msg = f"Distance Calculator At The Time Of Check-In Job Ran For:- {yesterday}."

bot_message = {
    'text': f"```{msg}```"}
message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
# # Prod
url = ('https://chat.googleapis.com/v1/spaces/AAAAc5Yb5K4/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI'
       '&token=cDQjg_fqvYjQCmTGrdHHgQeeAgNFfzQUkX92zbkvTm0')

# Test
# url = ('https://chat.googleapis.com/v1/spaces/AAAAQqFHSzs/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token'
#        '=ObHS4RWRvTHdYPm7JR_o6pkfayQD_SGcAHzDcbj0Rtk')
http_obj = Http()
response = http_obj.request(
    uri=url,
    method='POST',
    headers=message_headers,
    body=json.dumps(bot_message),
)
if __name__ == '__main__':
    print("")
