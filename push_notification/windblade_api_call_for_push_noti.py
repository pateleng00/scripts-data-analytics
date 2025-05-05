import datetime
import json
import requests
import time
from mysql.connector import MySQLConnection

from python_mysql_dbconfig import read_db_config


class PushNotification:
    def get_user_session(self):
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor()
        cursor.execute("select tus.FLD_USER_ID from attendance a left join TBL_USER_SESSION tus on "
                       "tus.FLD_USER_ID = a.FLD_USER_ID where a.app_id = 2 and a.mood_id is null and date("
                       "a.FLD_CLOCK_IN_TIME) = CURRENT_DATE() AND tus.FLD_USER_ID = 7387 ")
        user_ids = cursor.fetchall()
        print(user_ids)

        api_url = 'https://abcd.abcd.com/fleet/notification/push'
        bearer_token = '..7-74'

        headers = {
            'Authorization': f'Bearer {bearer_token}',
            'Content-Type': 'application/json'
        }
        print(f"Sent Time Of Push Notification: {datetime.datetime.now()}")
        for user_id in user_ids:
            user_id = user_id[0]
            payload = {
                "notification_type": "whatsapp",
                "user_id": user_id,
                "mobile_number": 000000000,
                "message": "मोइविंग परिवार मंगल कामना करता है कि आने वाला साल आपकी उत्तम ड्राइविंग की तरह ही आपके लिए अतिउत्तम साबित हो.मोइविंग के साथ जुड़े रहने के लिए धन्यवाद्।",
                "title": "मोइविंग (MoEVing ) की तरफ आपको जन्मदिन की हार्दिक शुभकामनाये",
                "notification_source": "whatsapp-sms-test"
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
