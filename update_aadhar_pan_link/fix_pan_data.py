import datetime
import json
import time

import mysql
from httplib2 import Http
from mysql.connector import MySQLConnection
import requests


conn = mysql.connector.connect(host="localhost:3306", user="abcd", password="abcd",
                               database="mydb")
cursor = conn.cursor()

profile_query = """select tpd.FLD_USER_ID, tpd.FLD_PAN_NUMBER, tpd.FLD_AADHAAR_NUMBER, pd.user_id, pd.pan, 
pd.linked_aadhar_number, pd.is_aadhar_linked from TBL_DRIVER_DETAILS tdd inner join TBL_PROFILE_DETAILS tpd on 
tpd.FLD_USER_ID = tdd.FLD_DRIVER_USER_ID inner join TBL_USER_DETAILS tud on tud.FLD_USER_ID = tpd.FLD_USER_ID left 
join pan_detail pd on pd.user_id = tdd.FLD_DRIVER_USER_ID where  tud.FLD_STATUS in (15, 3) and tdd.FLD_STATUS in (15, 3)
and tdd.FLD_OPERATING_CITY_ID not in (46,47) and pd.user_id is null and tpd.FLD_AADHAAR_NUMBER is not null 
and tpd.FLD_PAN_NUMBER is not null ;"""

check_pan = ("select pd.pan, pd.linked_aadhar_number, is_aadhar_linked from pan_detail pd  where "
             " pan in ('==','87vRmLj+==','DDeRe9/b2Vd+==',"
             "'==','+==');")

cursor.execute(check_pan)
data_from_pan = cursor.fetchall()
print(data_from_pan)

bearer_token = ('')

headers = {
    'Authorization': f'Bearer {bearer_token}',
    'Content-Type': 'application/json'
}

encrypt_pan_aadhar = "apiURL"
decrypt_pan = "apiURL"
pan_detailed_header = {
    'appId': '000',
    'appKey': '-99-090',
    'Content-Type': 'application/json'
}
count = 0
pan_details_not_found = []
aadhar_pan_linked = []
aadhar_pan_not_linked = []
for pan in data_from_pan:
    pan_number = pan[0]
    aadhaar_number = pan[1]

    data = {
        "pan": pan_number
    }
    json_payload = json.dumps(data)
    pan_decrypt = {
        "value": pan_number
    }
    pan_decrypt = json.dumps(pan_decrypt)
    aadhar_decrypt = {
        "value": aadhaar_number
    }
    aadhar_decrypt = json.dumps(aadhar_decrypt)
    decrypted_aadhar_response = requests.post(decrypt_pan, headers=headers, data=aadhar_decrypt)
    decrypted_aadhar_response = decrypted_aadhar_response.json()
    decrypted_aadhar_number = decrypted_aadhar_response.get('data')
    print("decrypted_aadhar_number", decrypted_aadhar_number)
    decrypted_pan_response = requests.post(decrypt_pan, headers=headers, data=pan_decrypt)
    decrypted_pan_response = decrypted_pan_response.json()
    decrypted_pan_number = decrypted_pan_response.get('data')
    decrypted_pan_number = [decrypted_pan_number]
    print("decrypted_pan_number", decrypted_pan_number)
    check_pan_from_profile = ("select tpd.FLD_USER_ID, tpd.FLD_PAN_NUMBER, tpd.FLD_AADHAAR_NUMBER from "
                              "TBL_PROFILE_DETAILS tpd where tpd.FLD_PAN_NUMBER = %s order by tpd.FLD_USER_ID desc "
                              "limit 1 ;")
    cursor.execute(check_pan_from_profile, decrypted_pan_number)
    pan_details = cursor.fetchall()
    print("pan_details", pan_details)
    for matched_pan in pan_details:
        user = matched_pan[0]
        m_pan = matched_pan[1]
        m_aadhar = matched_pan[2]
        if m_pan == decrypted_pan_number[0]:
            update_user = "update pan_detail set user_id = %s, modified_by = %s, modified_date = %s where pan =  %s"
            cursor.execute(update_user, (user, 8810, datetime.datetime.today(), pan_number))
            print("updating pan details for:- ", user)
            conn.commit()
            break

