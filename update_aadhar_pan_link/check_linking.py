import json

import mysql
from httplib2 import Http
from mysql.connector import MySQLConnection
import requests


conn = mysql.connector.connect(host="localhost:3306", user="abcd", password="abcd",
                               database="mydb")
cursor = conn.cursor()

query = """select pan, linked_aadhar_number, is_aadhar_linked, user_id, status_id, id from pan_detail pd where 
is_aadhar_linked =  1 and user_id is null and pd.id >= 534  ;"""
cursor.execute(query)
pan_data = cursor.fetchall()
print("pan data", pan_data)

profile_query = """select tpd.FLD_USER_ID, FLD_PAN_NUMBER, FLD_AADHAAR_NUMBER, tud.FLD_USER_NAME, pd.user_id from TBL_PROFILE_DETAILS tpd
inner join TBL_DRIVER_DETAILS tdd on tdd.FLD_DRIVER_USER_ID  = tpd.FLD_USER_ID
inner join TBL_USER_DETAILS tud on tud.FLD_USER_ID = tpd.FLD_USER_ID  
left join pan_detail pd on pd.user_id = tpd.FLD_USER_ID 
where tdd.FLD_OPERATING_CITY_ID not in (46,47) and date(tpd.FLD_CREATED_DATE) = CURRENT_DATE() - INTERVAL 1 day and pd.user_id is null;"""
cursor.execute(profile_query)
not_in_pan_details = cursor.fetchall()

bearer_token = (''
                '.'
                '.-4')

headers = {
    'Authorization': f'Bearer {bearer_token}',
    'Content-Type': 'application/json'
}

encrypt_pan_aadhar = "https://abcd.abcd.com/fleet/common/encrypt"
decrypt_pan = "https://abcd.abcd.com/fleet/common/decrypt"
check_pan_aadhar_linking = 'https://ind.thomas.hyperverge.co/v1/abcd'
pan_detailed_header = {
    'appId': '00000',
    'appKey': '0000000000000',
    'Content-Type': 'application/json'
}

update_pan_details = """update pan_detail set user_id = %s where id = %s ;"""
driverList = []
count = 0

for pan in pan_data:
    pan_number = pan[0]
    print(pan_number)
    aadhar_number = pan[1]
    pan_detail_id = pan[5]
    print(pan_detail_id)
    data = {
        "value": pan_number
    }
    json_payload = json.dumps(data)
    print(json_payload)
    decrypt_pan_response = requests.post(headers=headers, url=decrypt_pan, data=json_payload)
    decrypt_pan_response = decrypt_pan_response.json()
    print("Decrypt PanResponse", decrypt_pan_response)
    if 'data' in decrypt_pan_response:
        decrypt_pan_number = decrypt_pan_response['data']
        print(decrypt_pan_number)
        check_profile = """select tpd.FLD_USER_ID from TBL_PROFILE_DETAILS tpd 
                inner join TBL_DRIVER_DETAILS tdd on tpd.FLD_USER_ID = tdd.FLD_DRIVER_USER_ID 
                inner join TBL_USER_DETAILS tud on tud.FLD_USER_ID = tpd.FLD_USER_ID 
                where tpd.FLD_AADHAAR_NUMBER is not null and LOWER(tpd.FLD_PAN_NUMBER) = LOWER(%s) ;"""
        QUERY = cursor.execute(check_profile, (decrypt_pan_number,))
        print(QUERY)
        profile_details = cursor.fetchall()
        print("Profile data", profile_details)
        data = {
            "pan": decrypt_pan_number
        }
        json_payload = json.dumps(data)
        link_response = requests.post(check_pan_aadhar_linking, headers=pan_detailed_header, data=json_payload)
        aadhar_pan_link_response = link_response.json()
        print(link_response.json())
        aadhar_linking_status = aadhar_pan_link_response['result']['data']['panData']['aadhaarLinked']
        print(aadhar_linking_status)
        if aadhar_linking_status == True and profile_details:
            user_id = profile_details[0][0]
            print(user_id)
            print(pan_detail_id)
            cursor.execute(update_pan_details, (user_id, pan_detail_id,))
            conn.commit()
            count += 1
            driverList.append(user_id)
    else:
        continue
bot_message = {
    'text': f"Aadhar Pan Linking Status Updated For :- {count} Driver's. Driver List As Follows:- {driverList}"}

print(bot_message['text'].replace('```', ''))

message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
# Prod
url = 'https://chat.googleapis.com/v1/spaces/AAAAc5Yb5K4/messages?key=-WEfRq3CPzqKqqsHI&token='

# Test
# url = 'https://chat.googleapis.com/v1/spaces/AAAAQqFHSzs/messages?key=-&token=%3D'
http_obj = Http()
response = http_obj.request(
    uri=url,
    method='POST',
    headers=message_headers,
    body=json.dumps(bot_message),
)

conn.close()
