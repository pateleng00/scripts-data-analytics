import datetime
import json

import requests
import time
from mysql.connector import MySQLConnection

from python_mysql_dbconfig import read_db_config


class PushNotificationTrainingCompleted:
    def get_supervisor_details(self):
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor()
        cursor.execute("select tud2.FLD_USER_ID from TBL_PROFILE_DETAILS tpd inner join "
                       "TBL_USER_DETAILS tud on tud.FLD_USER_ID = tpd.FLD_USER_ID inner join TBL_DRIVER_DETAILS tdd "
                       "on tdd.FLD_DRIVER_USER_ID = tpd.FLD_USER_ID inner join TBL_ENTREPRENEUR_DETAILS ted on "
                       "ted.FLD_MOEVING_HUB_ID = tdd.FLD_MOEVING_HUB_ID inner join TBL_USER_DETAILS tud2 on "
                       "tud2.FLD_USER_ID = ted.FLD_ENTREPRENEUR_USER_ID inner join TBL_USER_SESSION tus on "
                       "tud2.FLD_USER_ID = tus.FLD_USER_ID where FLD_FROM_APP is not NULL and "
                       "tud.FLD_OPERATING_CITY_ID not in (46,47) and tud.FLD_STATUS <> 6 and ((DATEDIFF(CURRENT_DATE("
                       "), tud.FLD_CREATED_DATE)) >= tdd.FLD_TRAINING_DAYS) and tdd.FLD_DRIVING_TRAINING_STATUS != 16 "
                       "and tus.FLD_FCM_TOKEN  is not null and tus.FLD_FCM_TOKEN <> '';")

        user_ids = cursor.fetchall()
        print(user_ids)

        api_url = 'https://abcd.abcd.com/fleet/notification/push'
        bearer_token = ' . .- '

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
                "message": "New Driver has completed his training - Open MoEVing Driver app to share feedback and "
                           "mark his training as completed.",
                "title": "New Driver completed the training ",
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
