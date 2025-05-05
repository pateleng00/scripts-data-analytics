import time

import mysql.connector

from update_aadhar_pan_link.hyperverge_api_call import get_encrypted_pan_aadhar, insert_user_aadhar_pan_linking, \
    update_function

conn = mysql.connector.connect(host="localhost:3306", user="abcd", password="abcd",
                               database="mydb")
cursor = conn.cursor()

profile_query = """select * from Temp """


profiles = """select tpd.FLD_USER_ID, tpd.FLD_PAN_NUMBER, tpd.FLD_AADHAAR_NUMBER, pd.user_id, pd.pan, 
pd.linked_aadhar_number, pd.is_aadhar_linked, pd.remarks from TBL_DRIVER_DETAILS tdd inner join TBL_PROFILE_DETAILS tpd on 
tpd.FLD_USER_ID = tdd.FLD_DRIVER_USER_ID inner join TBL_USER_DETAILS tud on tud.FLD_USER_ID = tpd.FLD_USER_ID left 
join pan_detail pd on pd.user_id = tdd.FLD_DRIVER_USER_ID 
where  pd.user_id is null and tpd.FLD_PAN_NUMBER !='' and tdd.FLD_OPERATING_CITY_ID not in (46,47) and tpd.FLD_PAN_NUMBER in ("AFKPI3338G") ;
"""

cursor.execute(profiles)
profiles = cursor.fetchall()

for profile in profiles:
    user_id = profile[0]
    pan_number = profile[1]

    aadhaar_number = profile[2]
    aadhaar_lst_4 = aadhaar_number[8:]
    # print("aadhaar_lst_4", aadhaar_lst_4)
    encrypted_pan = get_encrypted_pan_aadhar(pan_number)
    # print(encrypted_pan)
    encrypted_aadhar = get_encrypted_pan_aadhar(aadhaar_lst_4)
    encrypted_aadhar = [encrypted_aadhar]
    # print(encrypted_aadhar)
    query = "select pan  from pan_detail where pan = %s"
    cursor.execute(query, (encrypted_pan, ))
    data = cursor.fetchall()
    print("matched pan data", data)
    if not data:
        print("data not found in pan table")
        print(user_id)
        print("encrypted_pan", encrypted_pan)
        insert_user_aadhar_pan_linking(pan_number, user_id)
    elif data:
        print("data found in pan table")
        update_function(pan_number, user_id, )
    time.sleep(5)

