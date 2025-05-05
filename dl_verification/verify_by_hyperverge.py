import base64
import datetime
import json
import re
import time
from random import random

import random
import string
import boto3
import requests
from httplib2 import Http
from mysql.connector import MySQLConnection
import mysql.connector

aws_s3_config = {
    'bucket_name': 'mybucket',
    'aws_access_key_id': 'abcdefgh',
    'aws_secret_access_key': 'abcdefgh',
    'folder_name': "profile",
    'file_folder_name': "driving_license_user_image"
}

razor_pay_credentials_dict = {
    'RAZOR_PAY_KEY_ID': 'abcd',
    'RAZOR_PAY_KEY_SECRET': 'abcd',
    'RAZOR_PAY_API_BASE_URL': 'https://api.razorpay.com/v1',
    'RAZOR_PAY_WEBHOOK_SECRET': 'abcd',
    'RAZOR_PAY_ACCOUNT_NUMBER': '1234', }

print(f"-Running DL Check for:-  {datetime.date.today()}-")

conn = mysql.connector.connect(host="localhost:3306", user="live_user", password="1PC^&uO$5",
                               database="mydb")
cursor = conn.cursor()

select_query = 'select user_id, dl_number  from driving_license_detail ;'
cursor.execute(select_query)
verified_user_details = cursor.fetchall()

query = ("""select tdd.FLD_DRIVER_USER_ID , tpd.FLD_DRIVING_LICENSE_NUMBER, DATE_FORMAT(tpd.FLD_DATE_OF_BIRTH, 
"%d-%m-%Y")   from TBL_DRIVER_DETAILS tdd inner join  TBL_PROFILE_DETAILS tpd on tpd.FLD_USER_ID = 
tdd.FLD_DRIVER_USER_ID inner join TBL_LOCATION_DETAILS tld on tld.FLD_LOCATION_ID = tdd.FLD_OPERATING_CITY_ID inner 
join TBL_USER_DETAILS tud on tud.FLD_USER_ID = tdd.FLD_DRIVER_USER_ID and tud.FLD_STATUS = 3 where tdd.FLD_STATUS in 
(15, 3) and tdd.FLD_OPERATING_CITY_ID not in (46,47) and tpd.FLD_DL_STATUS is null and date(tdd.FLD_CREATED_DATE) = 
CURRENT_DATE() - INTERVAL 1 day ; """)

cursor.execute(query)
user_details = cursor.fetchall()

hits = """select COUNT(id)  from driving_license_detail dld where (error is null or error != "Invalid driving license 
format") and month(created_at) = month(CURRENT_DATE())  and year(created_at) = YEAR(CURRENT_DATE()) ;"""
cursor.execute(hits)
count_total_api_hits = cursor.fetchall()

api_url = 'https://ind-verify.hyperverge.co/api/sbcd'


def generate_random_alphanumeric(length):
    alphanumeric_characters = string.ascii_letters + string.digits
    return ''.join(random.choice(alphanumeric_characters) for _ in range(length))


def upload_image_to_s3(base64_string, bucket_name, folder_name, file_name):
    print(" Into Upload Image To S3 ")

    # Decode base64 string
    image_data = base64.b64decode(base64_string)
    # Initialize S3 client
    s3 = boto3.client('s3', aws_access_key_id=aws_s3_config['aws_access_key_id'],
                      aws_secret_access_key=aws_s3_config['aws_secret_access_key'])

    s3_key = f"{folder_name}/{file_name}"
    # print(s3_key)
    # Upload image to S3 bucket
    s3.put_object(Body=image_data, Bucket=bucket_name, Key=s3_key)
    print(f"Image uploaded successfully by name:- {file_name}  ")


def save_to_database(data, user_id, image_path, dl_number, transaction_id):
    conn = mysql.connector.connect(host="localhost:3306", user="abcd", password="abcdefgh", database="mydb")
    cursor = conn.cursor()
    random_value_ = transaction_id
    # print(data)
    try:
        statusCode = int(data['statusCode'])
        # print("cov_2", cov_2)
        if statusCode == 200:
            issue_date = data['result']['issue_date']
            father_name = data['result']["father/husband"]
            if len(father_name) > 0:
                father_name = father_name
            else:
                father_name = None
            dl_number = data['result']['dl_number']
            # print("father_name", father_name)
            driver_name = data['result']['name']
            issue_date_formatted = datetime.datetime.strptime(issue_date, "%d-%m-%Y").strftime("%Y-%m-%d")
            if len(issue_date_formatted) > 0:
                issue_date_formatted = issue_date_formatted
            else:
                issue_date_formatted = None
            dob = data['result']['dob']
            dob_formatted = datetime.datetime.strptime(dob, "%d-%m-%Y").strftime("%Y-%m-%d")
            # print(dob_formatted)
            dl_validity_pass = data['result']['validity']['non-transport']
            # print(dl_validity_pass)
            dl_validity_com = data['result']['validity']['transport']
            if len(dl_validity_pass) == 10:
                non_transport_validity = datetime.datetime.strptime(dl_validity_pass, "%d-%m-%Y").strftime("%Y-%m-%d")
            else:
                validity_non_transport_start, validity_non_transport_end = dl_validity_pass.split(" to ")
                validity_non_transport_start_formatted = datetime.datetime.strptime(validity_non_transport_start,
                                                                                    "%d-%m-%Y").strftime("%Y-%m-%d")
                validity_non_transport_end_formatted = datetime.datetime.strptime(validity_non_transport_end,

                                                                                  "%d-%m-%Y").strftime("%Y-%m-%d")
                non_transport_validity = validity_non_transport_end_formatted
            address = data['result']['address']
            if len(address) > 0:
                address = address
            else:
                address = None
            cov_details = data['result']['cov_details']
            if len(cov_details) == 1:
                cov_1 = cov_details[0]
                cov_1_ = cov_1.get('cov')
                insert_query = (
                    "INSERT INTO moevingdb.driving_license_detail(user_id, dob, dl_number, validity_transport, "
                    "validity_non_transport, registered_address, issue_date, image_url, name, father_name, cov_1, "
                    "transaction_id, status_code )VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);")
                # print("non_transport_validity", non_transport_validity)
                cursor.execute(insert_query, (
                    user_id, dob_formatted, dl_number, dl_validity_com, non_transport_validity, address,
                    issue_date_formatted, image_path, driver_name, father_name, cov_1_, random_value_, statusCode,))
                print("---------------InsertedToProdWithSuccessResponse-----------------")
                conn.commit()
            elif len(cov_details) == 2:
                cov_1 = cov_details[0]
                cov_1_ = cov_1.get('cov')
                cov_2 = cov_details[1]
                cov_2_ = cov_2.get('cov')
                insert_query = (
                    "INSERT INTO moevingdb.driving_license_detail(user_id, dob, dl_number, validity_transport, "
                    "validity_non_transport, registered_address, issue_date, image_url, name, "
                    "father_name, cov_1, cov_2, transaction_id, status_code)VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);")
                cursor.execute(insert_query, (
                    user_id, dob_formatted, dl_number, dl_validity_com, non_transport_validity, address,
                    issue_date_formatted, image_path, driver_name, father_name, cov_1_, cov_2_, random_value_,
                    statusCode,))
                print("---------------InsertedToProdWithSuccessResponse-----------------")
                conn.commit()
            elif len(cov_details) == 3:
                cov_1 = cov_details[0]
                cov_1_ = cov_1.get('cov')
                cov_2 = cov_details[1]
                cov_2_ = cov_2.get('cov')
                cov_3 = cov_details[2]
                cov_3_ = cov_3.get('cov')
                insert_query = (
                    "INSERT INTO moevingdb.driving_license_detail(user_id, dob, dl_number, validity_transport, "
                    "validity_non_transport, registered_address, issue_date, image_url, name, "
                    "father_name, cov_1, cov_2, cov_3, transaction_id, status_code)VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);")
                cursor.execute(insert_query, (
                    user_id, dob_formatted, dl_number, dl_validity_com, non_transport_validity, address,
                    issue_date_formatted, image_path, driver_name, father_name, cov_1_, cov_2_, cov_3_, random_value_,
                    statusCode,))
                print("---------------InsertedToProdWithSuccessResponse-----------------")
                conn.commit()
        elif statusCode != 200:
            error = data['error']
            insert_query = "INSERT INTO moevingdb.driving_license_detail(user_id, error, dl_number, transaction_id, status_code)VALUES(%s, %s, %s, %s, %s);"
            cursor.execute(insert_query, (user_id, error, dl_number, random_value_, statusCode,))
            conn.commit()
            print("---------------CommittedToProdWithError-----------------")
    except KeyError as e:
        print(f"Error: Missing key {e} in the provided data.")
        return False
    except Exception as e:
        print(f"Error saving to database: {e}")
        return False
    finally:
        cursor.close()


success_counters = 0
failed_counters = 0
for user in user_details:
    random_value = generate_random_alphanumeric(8)
    headers = {
        'appId': 'oooo',
        'appKey': '000000',
        'transactionId': random_value,
        'Content-Type': 'application/json'
    }
    if len(user_details) > 50:
        msg = (
            f"Fetch driving license details api not called due to yesterdays onboarded driver count is exceeding daily "
            f"allowed limit 50 Current count is:- {len(user_details)}")

        bot_message = {
            'text': f"```{msg}```"}

        # print(bot_message['text'].replace('```', ''))

        message_headers = {'Content-Type': 'application/json; charset=UTF-8'}

        # Prod
        url = ('https://chat.googleapis.com/v1/spaces/AAAAc5Yb5K4/messages?key=AIzaSyDdI0hCZ')

        http_obj = Http()
        response = http_obj.request(
            uri=url,
            method='POST',
            headers=message_headers,
            body=json.dumps(bot_message),
        )
        break
    else:
        user_id = user[0]
        dl = user[1]
        dl = str(dl)
        dl = dl.replace("-", "").replace(" ", "")
        pattern = r'^[A-Z]{2}\d+$'
        match = re.match(pattern, dl)
        dob = user[2]
        payload = {
            "dlNumber": dl,
            "dob": str(dob),
            "returnState": "yes"
        }
        if match and len(dl) >= 9:
            success_counters = success_counters + 1
            print("Valid driving license format")
            # Convert payload to JSON string
            json_payload = json.dumps(payload)
            # Make the API call with the correct data parameter
            response = requests.post(api_url, headers=headers, data=json_payload)
            response = response.json()
            result = response.get('result')
            response_code = int(response['statusCode'])
            if response_code == 200:
                print("Into If Statement")
                dl_image = result['img']
                s3_bucket_name = aws_s3_config['bucket_name']
                s3_folder = f"{aws_s3_config['folder_name']}/{aws_s3_config['file_folder_name']}"
                file_name = f"{user_id}_dl_user_image_{dl}.jpg"
                image_path = f"{s3_folder}/{file_name}"
                print("image_path", image_path)
                upload_image_to_s3(dl_image, s3_bucket_name, s3_folder, file_name)
                save_to_database(response, user_id, image_path, dl_number=dl, transaction_id=random_value)
            elif response_code != 200:
                failed_counters = failed_counters + 1
                print(response)
                print("Into ElIf Statement")
                save_to_database(response, user_id, image_path=None, dl_number=dl, transaction_id=random_value)
        else:
            error_message = "Invalid driving license format"
            print(error_message)
            insert_query = ("INSERT INTO moevingdb.driving_license_detail(user_id, error, dl_number, "
                            "is_hyperverge)VALUES( %s, %s,%s, %s);")
            cursor.execute(insert_query, (user_id, error_message, dl, 0,))
            conn.commit()
            print("---------------InsertedToProdWithDLFormatError-----------------")
            time.sleep(3)

invalid_dl = """select count(id) as Count  from driving_license_detail dld WHERE date(created_at) = date(
CURRENT_DATE()) and error = "Invalid driving license format" ;"""
cursor.execute(invalid_dl)
count_total = cursor.fetchall()
print(count_total)


msg = (f"Driving license API hit for:-   {success_counters + failed_counters} user(s) \n"
       f"Successfully data fetched for:- {success_counters} user(s) \n"
       f"Found error in fetching for:-   {failed_counters} user(s) \n"
       f"Total invalid DL found today:-  {count_total[0][0]} user(s) on {datetime.date.today()}\n"
       f"Total API hit for month of {datetime.date.today().strftime('%B,%Y')}:- {count_total_api_hits[0][0]}")

bot_message = {
    'text': f"```{msg}```"}
message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
# # Prod
# url = ('https://chat.googleapis.com/v1/spaces/AAAAc5Yb5K4/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3')

# Test
url = 'https://chat.googleapis.com/v1/spaces/AAAAQqFHSzs/messages?key=AIzaSyDdI0hCZtE6vySjM'
http_obj = Http()
response = http_obj.request(
    uri=url,
    method='POST',
    headers=message_headers,
    body=json.dumps(bot_message),
)

if __name__ == '__main__':
    print("")
