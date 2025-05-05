import datetime
import json

import requests
import time
from mysql.connector import MySQLConnection

from python_mysql_dbconfig import read_db_config


class BirthdayWishes:
    def get_drivers_detail(self):
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor()
        cursor.execute("SELECT tud.FLD_USER_ID, tpd.FLD_DATE_OF_BIRTH, tpd.FLD_NAME, tus.FLD_FCM_TOKEN, tld.FLD_LOCATION_ID FROM "
                       "TBL_PROFILE_DETAILS tpd INNER JOIN TBL_USER_DETAILS tud ON tud.FLD_USER_ID = tpd.FLD_USER_ID "
                       "inner join TBL_USER_SESSION tus on tus.FLD_USER_ID  = tpd.FLD_USER_ID  inner join "
                       "TBL_DRIVER_DETAILS tdd on tdd.FLD_DRIVER_USER_ID = tud.FLD_USER_ID  WHERE month( "
                       "tpd.FLD_DATE_OF_BIRTH) = month(CURRENT_DATE()) and  DAY(tpd.FLD_DATE_OF_BIRTH) = DAY( "
                       "CURRENT_DATE())  and tdd.FLD_OPERATING_CITY_ID not in (46,47) and tud.FLD_STATUS = 3 and "
                       "tdd.FLD_STATUS = 3; ")

        user_ids = cursor.fetchall()
        print(user_ids)

        api_url = 'https://abcd.abcd.com/fleet/notification/push'
        bearer_token = '..-'

        headers = {
            'Authorization': f'Bearer {bearer_token}',
            'Content-Type': 'application/json'
        }
        print(f"Sent Time Of Push Notification: {datetime.datetime.now()}")
        for user_id in user_ids:
            driver_name = user_id[2]
            user_id = user_id[0]
            payload = {
                "notification_type": "push-notification",
                "user_id": user_id,
                "mobile_number": '',
                "message": f'Dear {driver_name}, wishing you a day as fantastic as your drives! May your day be '
                           f'filled with safe and smooth rides with joyful moments!',
                "title": "Happy Birthday from MoEVing!",
                "notification_source": "scheduler-services"
            }

            # Convert payload to JSON string
            json_payload = json.dumps(payload)

            # Make the API call with the correct data parameter
            response = requests.post(api_url, headers=headers, data=json_payload)
            if response.status_code == 200:
                print(f"User ID: {user_id} - Response: {response.json()}")
            else:
                print(f"User ID: {user_id} - Request failed with status code: {response.status_code}")

            time.sleep(1)
