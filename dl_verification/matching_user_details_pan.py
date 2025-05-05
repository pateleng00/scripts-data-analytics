from fuzzywuzzy import fuzz
from mysql.connector import MySQLConnection
from python_mysql_dbconfig import read_db_config

import mysql.connector

conn = mysql.connector.connect(host="localhost:3306", user="abcd", password="abcd",
                               database="mydb")
cursor = conn.cursor()

pan_table = """select user_id , name  from pan_detail pd where user_id is not null and date(created_date) = 
CURRENT_DATE() - interval 1 day ;"""
cursor.execute(pan_table)
pan_table_user = cursor.fetchall()
print('pan_table_user', pan_table_user)

profile_table = """select FLD_USER_ID , case when FLD_LAST_NAME is not null then CONCAT(FLD_NAME,' ',FLD_LAST_NAME) 
else FLD_NAME end as UserName from TBL_PROFILE_DETAILS tpd where FLD_USER_ID in (select user_id  from pan_detail dld 
where user_id is not null and date(created_date) = CURRENT_DATE() - interval 1 day);"""
cursor.execute(profile_table)
profile_table_user = cursor.fetchall()
print('profile_table_user', profile_table_user)

# unverified_dl = """select user_id from driving_license_detail where error is not null and  date(created_at) =
# CURRENT_DATE();"""
# cursor.execute(unverified_dl)
# data = cursor.fetchall()
# print(data)

for pan_user, profile_user in zip(pan_table_user, profile_table_user):
    pan_user_id = pan_user[0]
    pan_user_name = pan_user[1]
    # print("PAN_USER_NAME", pan_user_name)
    profile_user_id = profile_user[0]
    profile_user_name = profile_user[1]
    # print("Profile_user_name", profile_user_name)
    if pan_user_id == profile_user_id:
        partial_token_ratio = fuzz.partial_ratio(pan_user_name, profile_user_name)
        token_sort_ratio = fuzz.token_sort_ratio(pan_user_name, profile_user_name)
        max_token_ratio = max(partial_token_ratio, token_sort_ratio)
        # print(f"max_token_ratio of user_id {profile_user_id} after matching name -: {max_token_ratio}")
        if max_token_ratio >= 75:
            status = f"success - {max_token_ratio}"
            query = "update TBL_PROFILE_DETAILS set FLD_PAN_STATUS = %s where FLD_USER_ID = %s; "
            query_pan_table = "update pan_detail set name_matching_ratio = %s where user_id = %s; "
            cursor.execute(query, (status, profile_user_id,))
            cursor.execute(query_pan_table, (max_token_ratio, profile_user_id,))
            conn.commit()
            print("---updated successfully > 75---")
        elif max_token_ratio < 75:
            status = f"pan verified but name mismatched - {max_token_ratio}"
            query = ("update TBL_PROFILE_DETAILS set FLD_PAN_STATUS = %s where "
                     "FLD_USER_ID = %s;")
            query_pan_table = "update pan_detail set name_matching_ratio = %s where user_id = %s; "
            cursor.execute(query, (status, profile_user_id,))
            cursor.execute(query_pan_table, (max_token_ratio, profile_user_id,))
            conn.commit()
            print("---updated successfully < 75---")

if __name__ == '__main__':
    print("")
