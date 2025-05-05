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
and tdd.FLD_OPERATING_CITY_ID not in (46,47) and pd.user_id is  null and tpd.FLD_AADHAAR_NUMBER is not null and tpd.FLD_PAN_NUMBER != ''
and tpd.FLD_PAN_NUMBER is not null and date(tpd.FLD_CREATED_DATE) = CURRENT_DATE() - INTERVAL 1 DAY;"""

cursor.execute(profile_query)
not_in_pan_details = cursor.fetchall()

bearer_token = (''
                '.'
                '.')

headers = {
    'Authorization': f'Bearer {bearer_token}',
    'Content-Type': 'application/json'
}

encrypt_pan_aadhar = "apiURL"
decrypt_pan = "apiURL"
check_pan_aadhar_linking = 'apiURL'
pan_detailed_header = {
    'appId': '0000',
    'appKey': '--00',
    'Content-Type': 'application/json'
}
count = 0
pan_details_not_found =[]
aadhar_pan_linked = []
aadhar_pan_not_linked = []
for pan in not_in_pan_details:
    user_id = pan[0]
    print(user_id)
    pan_number = pan[1]
    aadhaar_number = pan[2]
    aadhaar_lst_4 = aadhaar_number[8:]
    data = {
        "pan": pan_number
    }
    json_payload = json.dumps(data)
    pan_encrypt = {
        "value": pan_number
    }
    aadhar_encrypt = {
        "value": aadhaar_lst_4
    }
    encrypted_aadhar_response = requests.get(encrypt_pan_aadhar, headers=headers, params=aadhar_encrypt)
    encrypted_aadhar_response = encrypted_aadhar_response.json()
    encrypted_aadhar_number = encrypted_aadhar_response.get('data')
    encrypted_pan_response = requests.get(encrypt_pan_aadhar, headers=headers, params=pan_encrypt)
    encrypted_pan_response = encrypted_pan_response.json()
    encrypted_pan_number = encrypted_pan_response.get('data')
    encrypted_pan_number = [encrypted_pan_number]
    check_pan = ("select pd.pan, pd.linked_aadhar_number, is_aadhar_linked, user_id  from pan_detail pd  where pan = "
                 "%s order by pd.id desc limit 1 ;")
    cursor.execute(check_pan, encrypted_pan_number)
    pan_details = cursor.fetchall()
    for matched_pan in pan_details:
        e_pan = matched_pan[0]
        e_aadhar = matched_pan[1]
        linked_status = matched_pan[2]
        matched_user_id = matched_pan[3]
        print("encrypted_pan_number", encrypted_pan_number[0])
        print("encrypted_aadhar_number", encrypted_aadhar_number)
        if e_pan == encrypted_pan_number[0] and e_aadhar == encrypted_aadhar_number and linked_status == 1:
            update_user = "update pan_detail set user_id = %s, modified_by = %s, modified_date = %s where pan =  %s"
            cursor.execute(update_user, (user_id, 8810, datetime.datetime.today(), e_pan))
            conn.commit()
            break
    if not pan_details:
        try:
            link_response = requests.post(check_pan_aadhar_linking, headers=pan_detailed_header, data=json_payload)
            aadhar_pan_link_response = link_response.json()
            print(link_response.json())
            count += 1
            message = ''

            if aadhar_pan_link_response['status'] == 'success':
                message = aadhar_pan_link_response['result']['data']['message']
                print("message", message)
            else:
                message = aadhar_pan_link_response['error']['reason']['message']
                print("message", message)
            if (aadhar_pan_link_response['status'] == 'success' and
                    (message != 'Pan does not exist.' and message != 'Invalid PAN number.')):
                pan_data = aadhar_pan_link_response['result']['data']['panData']
                name = pan_data['name']
                email = pan_data['email']
                phone = pan_data['phone']
                gender = pan_data['gender']
                pan_nu = pan_data['pan']
                firstName = pan_data['firstName']
                middleName = pan_data['middleName']
                lastName = pan_data['lastName']
                dateOfBirth = pan_data['dateOfBirth']
                maskedAadhaarNumber = pan_data['maskedAadhaarNumber']
                maskedAadhaarNumber = maskedAadhaarNumber.replace("X", "")
                address_data = aadhar_pan_link_response['result']['data']['panData']['address']
                street = address_data['street']
                city = address_data['city']
                state = address_data['state']
                pincode = address_data['pincode']
                line1 = address_data['line1']
                line2 = address_data['line2']
                aadhar_encrypt = {
                    "value": maskedAadhaarNumber
                }
                encrypted_aadhar_response = requests.get(encrypt_pan_aadhar, headers=headers, params=aadhar_encrypt)
                encrypted_aadhar_response = encrypted_aadhar_response.json()
                encrypted_aadhar_number = encrypted_aadhar_response.get('data')
                pan_encrypt = {
                    "value": pan_nu
                }
                encrypted_pan_response = requests.get(encrypt_pan_aadhar, headers=headers, params=pan_encrypt)
                encrypted_pan_response = encrypted_pan_response.json()
                encrypted_pan_number = encrypted_pan_response.get('data')
                aadhar_linking_status = aadhar_pan_link_response['result']['data']['panData']['aadhaarLinked']
                insertInto_pan_details = """INSERT INTO db.pan_detail( pan, linked_aadhar_number, dob, name, email, phone,
                gender, first_name, middle_name, last_name, street, city, state, pincode, line1, line2, is_aadhar_linked, created_by,
                user_id) VALUES( %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                if aadhar_linking_status:
                    print('Aadhar pan link true')
                    cursor.execute(insertInto_pan_details,
                                   (encrypted_pan_number, encrypted_aadhar_number, dateOfBirth, name, email,
                                    phone, gender, firstName, middleName, lastName, street, city, state,
                                    pincode, line1, line2, 1, 8810, user_id,))
                    conn.commit()
                    aadhar_pan_linked.append(user_id)
                elif not aadhar_linking_status:
                    print('Aadhar pan link false')
                    cursor.execute(insertInto_pan_details,
                                   (encrypted_pan_number, encrypted_aadhar_number, dateOfBirth, name, email,
                                    phone, gender, firstName, middleName, lastName, street, city, state,
                                    pincode, line1, line2, 0, 8810, None,))
                    conn.commit()
                    aadhar_pan_not_linked.append(user_id)
                else:
                    continue
            else:
                print('Error in API response or Invalid Pan Number')
                pan_details_not_found.append(user_id)
                insert_unverified_pan = """INSERT INTO db.pan_detail(pan, created_by,user_id, remarks) 
                                            VALUES(%s, %s, %s, %s);"""
                cursor.execute(insert_unverified_pan, (encrypted_pan_number[0], 8810, user_id, message))
                conn.commit()
        except Exception as e:
            print("Error Is Exception:- ", e)

        time.sleep(8)
bot_message = {
    'text': f"Aadhar Pan Linking Checked For :- {count} Driver's.\nDriver List Whose Addhar Linked:- {aadhar_pan_linked} \nDriver List Whose Addhar Not Linked:- {aadhar_pan_not_linked} \nDriver List Whose pan details not found:- {pan_details_not_found}"}

print(bot_message['text'].replace('```', ''))

message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
# Prod
url = 'https://chat.googleapis.com/v1/spaces/AAAAc5Yb5K4/messages?key=-&token='

# Test
# url = 'https://chat.googleapis.com/v1/spaces/AAAAQqFHSzs/messages?key=-&token=%3D'
http_obj = Http()
response = http_obj.request(
    uri=url,
    method='POST',
    headers=message_headers,
    body=json.dumps(bot_message),
)

if __name__ == "__main__":
    print("running")
