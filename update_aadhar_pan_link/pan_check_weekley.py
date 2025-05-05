import time

import mysql
from mysql.connector import MySQLConnection

from update_aadhar_pan_link.hyperverge_api_call import update_function, get_decrypted_pan_aadhar

conn = mysql.connector.connect(host="localhost:3306", user="abcd", password="abcd",
                               database="mydb")
cursor = conn.cursor()

profile_query = """select pd.pan, pd.linked_aadhar_number from pan_detail pd
where pd.is_aadhar_linked <> 1;
"""

cursor.execute(profile_query)
not_in_pan_details = cursor.fetchall()

for pan in not_in_pan_details:
    pan_number = pan[0]
    print(pan_number)
    aadhaar_number = pan[1]
    decrypted_pan_number = get_decrypted_pan_aadhar(pan_number)
    print(decrypted_pan_number)
    check_pan = ("select FLD_PAN_NUMBER,FLD_AADHAAR_NUMBER, FLD_USER_ID from TBL_PROFILE_DETAILS where FLD_PAN_NUMBER = %s ;")
    cursor.execute(check_pan, (decrypted_pan_number,))
    profile_details = cursor.fetchall()
    print("pan_details", profile_details)
    for matched_profile in profile_details:
        profile_pan = matched_profile[0]
        print("e_pan_", profile_pan)
        profile_aadhar = matched_profile[1]
        print(profile_aadhar, "e_aadhar")
        user_id = matched_profile[2]
        update_function(profile_pan, user_id)
        time.sleep(5)





