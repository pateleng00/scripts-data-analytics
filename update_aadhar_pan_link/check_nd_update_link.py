import time

import mysql
from mysql.connector import MySQLConnection

from update_aadhar_pan_link.hyperverge_api_call import insert_user_aadhar_pan_linking, \
    get_encrypted_pan_aadhar, update_function

conn = mysql.connector.connect(host="localhost:3306", user="abcd", password="abcd",
                               database="mydb")
cursor = conn.cursor()

profile_query = """select tpd.FLD_USER_ID, tpd.FLD_PAN_NUMBER, tpd.FLD_AADHAAR_NUMBER, pd.user_id, pd.pan, pd.id ,
pd.linked_aadhar_number, pd.is_aadhar_linked, pd.remarks from TBL_DRIVER_DETAILS tdd inner join TBL_PROFILE_DETAILS tpd on 
tpd.FLD_USER_ID = tdd.FLD_DRIVER_USER_ID inner join TBL_USER_DETAILS tud on tud.FLD_USER_ID = tpd.FLD_USER_ID left 
join pan_detail pd on pd.user_id = tdd.FLD_DRIVER_USER_ID where tdd.FLD_OPERATING_CITY_ID not in (46,
47) and   tpd.FLD_PAN_NUMBER in ("ooooooooo") ;
"""
cursor.execute(profile_query)
user_details = cursor.fetchall()


def remove_special_characters(input_value):
    if input_value is None:
        return ''
    input_str = str(input_value)
    cleaned_str = ''.join(char for char in input_str if char.isdigit())  # Keep only digits
    return cleaned_str

# Example of cleaning user_id


for pan in user_details:
    user_id = pan[0]
    print(user_id)
    pan_number = pan[1]
    aadhaar_number = pan[2]
    aadhaar_lst_4 = aadhaar_number[8:]
    pan_detail = pan[4]
    print("aadhaar_lst_4", aadhaar_lst_4)
    encrypted_aadhar_number = get_encrypted_pan_aadhar(aadhaar_lst_4)
    encrypted_pan_number = get_encrypted_pan_aadhar(pan_number)
    print("encrypted_aadhar_number", encrypted_aadhar_number)
    print("encrypted_pan_number", encrypted_pan_number)
    check_pan = ("select pd.pan, pd.linked_aadhar_number, is_aadhar_linked, user_id  from pan_detail pd  where pd.pan ="
                 "%s ;")
    cursor.execute(check_pan, [encrypted_pan_number],)
    pan_details = cursor.fetchall()
    print("pan_details", pan_details)
    for matched_pan in pan_details:
        e_pan = matched_pan[0]
        print("e_pan_", e_pan)
        e_aadhar = matched_pan[1]
        print(e_aadhar, "e_aadhar")
        linked_status = matched_pan[2]
        matched_user_id = matched_pan[3]
        if e_pan == encrypted_pan_number and e_aadhar == str(encrypted_aadhar_number) and linked_status == 1 and matched_user_id is None:
            update_user = "update pan_details set user_id = %s where pan = %s ;"
            cursor.execute(update_user, (user_id, e_pan))
            conn.commit()
            break
        if e_pan == encrypted_pan_number and e_aadhar is None and linked_status == 0 and matched_user_id == user_id:
            print("into update pan_details set user_id")
            update_function(pan_number, matched_user_id)
            break
        if e_aadhar != encrypted_aadhar_number:
            update_function(pan_number, (matched_user_id))
            break
    if not pan_details:
        print("Pan Details not found", pan_details)
        insert_user_aadhar_pan_linking(pan_number, user_id)
        time.sleep(5)

if __name__ == "__main__":
    print("running")
