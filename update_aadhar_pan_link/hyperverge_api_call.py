import json

import mysql.connector
import requests
from httplib2 import Http

conn = mysql.connector.connect(host="localhost:3306", user="abcd", password="abcd",
                               database="mydb")
cursor = conn.cursor()
bearer_token = ('')

headers = {
    'Authorization': f'Bearer {bearer_token}',
    'Content-Type': 'application/json'
}

encryption_api = "apiURL"
decrypt_pan_aadhar = "apiURL"
check_pan_aadhar_linking = 'apiURL'
pan_detailed_header = {
    'appId': '000',
    'appKey': '-99-090',
    'Content-Type': 'application/json'
}


def get_decrypted_pan_aadhar(pan_aadhar_number):
    aadhar_encrypt = {
        "value": pan_aadhar_number
    }
    response = requests.post(decrypt_pan_aadhar, headers=headers, data=json.dumps(aadhar_encrypt))
    encrypted_response = response.json()
    encrypted_number = encrypted_response.get('data')
    return encrypted_number


def get_encrypted_pan_aadhar(pan_aadhar_number):
    aadhar_encrypt = {
        "value": pan_aadhar_number
    }
    response = requests.get(encryption_api, headers=headers, params=aadhar_encrypt)
    encrypted_response = response.json()
    encrypted_number = encrypted_response.get('data')
    return encrypted_number


def remove_special_characters(input_value):
    # Convert to string to handle both strings and numbers
    input_str = str(input_value)
    # Filter only numeric characters
    cleaned_str = ''.join(char for char in input_str if char.isdigit())
    return cleaned_str


def update_function(pan, user_id):
    data = {
        "pan": pan
    }
    link_response = requests.post(check_pan_aadhar_linking, headers=pan_detailed_header, data=json.dumps(data))
    aadhar_pan_link_response = link_response.json()
    print(aadhar_pan_link_response)
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
        if maskedAadhaarNumber != '':
            maskedAadhaarNumber = maskedAadhaarNumber.replace("X", "")
            encrypted_aadhar = get_encrypted_pan_aadhar(maskedAadhaarNumber)
            encrypted_aadhar = [encrypted_aadhar]
            encrypted_aadhar = encrypted_aadhar[0]
            print("encrypted_aadhar", encrypted_aadhar)
        else:
            encrypted_aadhar = None
            print(encrypted_aadhar)
        address_data = aadhar_pan_link_response['result']['data']['panData']['address']
        street = address_data['street']
        city = address_data['city']
        state = address_data['state']
        pincode = address_data['pincode']
        line1 = address_data['line1']
        line2 = address_data['line2']
        encrypted_p = get_encrypted_pan_aadhar(pan_nu)
        encrypted_p = [encrypted_p]
        print("encrypted_p", encrypted_p)
        aadhar_linking_status = aadhar_pan_link_response['result']['data']['panData']['aadhaarLinked']
        if aadhar_linking_status == True:
            aadhar_linking_status = 1
            update_user_aadhar_mismatched = """UPDATE db.pan_detail SET 
                                                                                pan = %s,
                                                                                linked_aadhar_number = %s,
                                                                                dob = %s,
                                                                                name = %s,
                                                                                email = %s,
                                                                                phone = %s,
                                                                                gender = %s,
                                                                                first_name = %s,
                                                                                middle_name = %s,
                                                                                last_name = %s,
                                                                                street = %s,
                                                                                city = %s,
                                                                                state = %s,
                                                                                pincode = %s,
                                                                                line1 = %s,
                                                                                line2 = %s,
                                                                                is_aadhar_linked = %s,
                                                                                created_by = %s 
                                                                            WHERE pan = %s and user_id = %s ;"""
            cursor.execute(update_user_aadhar_mismatched, (encrypted_p[0], encrypted_aadhar, dateOfBirth, name, email,
                                                           phone, gender, firstName, middleName, lastName, street, city,
                                                           state,
                                                           pincode, line1, line2, aadhar_linking_status, 8810,
                                                           encrypted_p[0], user_id))
            conn.commit()


count = 0
pan_details_not_found = []
aadhar_pan_linked = []
aadhar_pan_not_linked = []


def insert_user_aadhar_pan_linking(pan_number, user_id):
    try:
        data = {
            "pan": pan_number
        }
        print(pan_number)
        link_response = requests.post(check_pan_aadhar_linking, headers=pan_detailed_header, data=json.dumps(data))
        aadhar_pan_link_response = link_response.json()
        print(link_response.json())
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
            encrypted_aadhar_= ''
            if maskedAadhaarNumber != '':
                maskedAadhaarNumber = maskedAadhaarNumber.replace("X", "")
                encrypted_aadhar_ = get_encrypted_pan_aadhar(maskedAadhaarNumber)
                encrypted_aadhar_ = [encrypted_aadhar_]
            if maskedAadhaarNumber == '':
                encrypted_aadhar_ = None
                print(encrypted_aadhar_)
            encrypted_pan_number_ = get_encrypted_pan_aadhar(pan_nu)
            encrypted_pan_number_ = [encrypted_pan_number_]
            aadhar_linking_status = aadhar_pan_link_response['result']['data']['panData']['aadhaarLinked']
            insertInto_pan_details = """INSERT INTO db.pan_detail( pan, linked_aadhar_number, dob, name, email, phone,
            gender, first_name, middle_name, last_name, street, city, state, pincode, line1, line2, is_aadhar_linked, created_by,
            user_id) VALUES( %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            if aadhar_linking_status:
                print('Aadhar pan link true')
                cursor.execute(insertInto_pan_details,
                               (encrypted_pan_number_[0], encrypted_aadhar_[0], dateOfBirth, name, email,
                                phone, gender, firstName, middleName, lastName, street, city, state,
                                pincode, line1, line2, 1, 8810, user_id,))
                conn.commit()
                aadhar_pan_linked.append(user_id)
            elif not aadhar_linking_status:
                print('Aadhar pan link false')
                cursor.execute(insertInto_pan_details,
                               (encrypted_pan_number_[0], None, dateOfBirth, name, email,
                                phone, gender, firstName, middleName, lastName, street, city, state,
                                pincode, line1, line2, 0, 8810, user_id,))
                conn.commit()
                aadhar_pan_not_linked.append(user_id)
        else:
            print('Error in API response or Invalid Pan Number')
            pan_details_not_found.append(user_id)
            insert_unverified_pan = """INSERT INTO db.pan_detail(created_by,user_id, remarks)
                                        VALUES( %s, %s, %s);"""
            cursor.execute(insert_unverified_pan, (8810, user_id, message,))
            conn.commit()
    except Exception as e:
        print("Error Is Exception:- ", e)


