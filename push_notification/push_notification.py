import json

import boto3
from botocore.config import Config
from mysql.connector import MySQLConnection
from responses import logger

from python_mysql_dbconfig import read_db_config

sqs_client = boto3.client(
    'sqs',
    aws_access_key_id='keyid',
    aws_secret_access_key='kay',
    config=Config(signature_version='s3v4'),
    region_name='region'
)


def get_user_session():
    dbconfig = read_db_config()
    conn = MySQLConnection(**dbconfig)
    cursor = conn.cursor()
    cursor.execute("select tus.FLD_USER_ID , tus.FLD_FCM_TOKEN from attendance a left join TBL_USER_SESSION tus on "
                   "tus.FLD_USER_ID = a.FLD_USER_ID where a.app_id = 2 and a.mood_id is null and date("
                   "a.FLD_CLOCK_IN_TIME) = CURRENT_DATE() and a.FLD_USER_ID =5659 ")
    rows = cursor.fetchall()
    return rows


def send_push_notification():
    user_id = None
    fcm_token = None
    title: str = "Low Battery Alert"
    message_: str = "Current SOC for self-assigned vehicle is less than 30%. Please charge your vehicle immediately."
    session = get_user_session()
    for row in session:
        user_id = row[0]
        fcm_token = row[1]
        print(user_id)
    try:
        if not user_id or not fcm_token:
            logger.info(f"User session or fcm token not found for user_id: {user_id}")
            return False
        else:
            message = {"title": title, "message": message_, "fcm_token": fcm_token, "image": ""}
            response = sqs_client.send_message(
                QueueUrl='https://sqs.ap-south-1.amazonaws.com/00000000000/FCM-notification',
                MessageBody=json.dumps(message)
            )
            logger.info(f"Push Notification sent to user_id: {user_id}")
            return True
    except Exception as e:
        logger.error(f"Error while sending push notification to user_id: {user_id}")
        logger.error(e)
        return False


send_push_notification()
