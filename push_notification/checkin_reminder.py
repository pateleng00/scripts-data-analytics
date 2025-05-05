import datetime
import json

import requests
import time
from mysql.connector import MySQLConnection

from python_mysql_dbconfig import read_db_config


class PushNotificationCheckIn:
    def get_attendance_details(self):
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor()
        cursor.execute("SELECT  a.FLD_USER_ID , tus.FLD_FCM_TOKEN FROM attendance a  inner join TBL_USER_SESSION "
                       "tus on tus.FLD_USER_ID  = a.FLD_USER_ID inner join TBL_DEMAND_FULFILMENT_DRIVER_DETAILS tdfdd "
                       "on tdfdd.FLD_DRIVER_USER_ID = a.FLD_USER_ID where TIMESTAMPDIFF(MINUTE, a.FLD_CLOCK_IN_TIME , "
                       "CURRENT_TIMESTAMP()) > 30 and tdfdd.FLD_DRIVER_CHECKIN_TIME is null and tdfdd.FLD_VEHICLE_ID "
                       "is not null and date(tdfdd.FLD_MODIFIED_DATE) = date(a.FLD_CLOCK_IN_TIME) and date("
                       "a.FLD_CLOCK_IN_TIME) = CURRENT_DATE()  ;")

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
            user_id = user_id[0]
            payload = {
                "notification_type": "push-notification",
                "user_id": user_id,
                "mobile_number": 0,
                "message": "Please do not forget to Check-in - Open MoEVing Driver app to Check-in.",
                "title": "Check-in Reminder",
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
