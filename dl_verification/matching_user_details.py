from fuzzywuzzy import fuzz
import mysql.connector

conn = mysql.connector.connect(host="localhost:3306", user="abcd", password="abcd",
                               database="mydb")
cursor = conn.cursor()

dl_table = """select user_id, name from driving_license_detail dld where error is null and  date(created_at) = 
CURRENT_DATE() order by user_id desc ;"""
cursor.execute(dl_table)
dl_table_user = cursor.fetchall()

profile_table = """select FLD_USER_ID , case when FLD_LAST_NAME is not null then CONCAT(FLD_NAME,' ',FLD_LAST_NAME) 
else FLD_NAME end as UserName from TBL_PROFILE_DETAILS tpd where FLD_USER_ID in (select user_id  from 
driving_license_detail dld where error is null and  date(created_at) = CURRENT_DATE()) order by FLD_USER_ID desc;"""
cursor.execute(profile_table)
profile_table_user = cursor.fetchall()

unverified_dl = """select user_id, error from driving_license_detail where error is not null and  date(created_at) = 
CURRENT_DATE();"""
cursor.execute(unverified_dl)
data = cursor.fetchall()

for dl_user, profile_user in zip(dl_table_user, profile_table_user):
    dl_user_id = dl_user[0]
    dl_user_name = dl_user[1]
    profile_user_id = profile_user[0]
    profile_user_name = profile_user[1]
    if dl_user_id == profile_user_id:
        partial_token_ratio = fuzz.partial_ratio(dl_user_name, profile_user_name)
        token_sort_ratio = fuzz.token_sort_ratio(dl_user_name, profile_user_name)
        max_token_ratio = max(partial_token_ratio, token_sort_ratio)
        if max_token_ratio >= 75:
            query = "update TBL_PROFILE_DETAILS set FLD_DL_STATUS = 'success' where FLD_USER_ID = %s; "
            query_dl_table = "update driving_license_detail set name_matching_ratio = %s where user_id = %s; "
            cursor.execute(query, (profile_user_id,))
            cursor.execute(query_dl_table, (max_token_ratio, profile_user_id,))
            conn.commit()
            print("---updated successfully > 75---")
        elif max_token_ratio < 75:
            status = "dl verified but name mismatched"
            query = ("update TBL_PROFILE_DETAILS set FLD_DL_STATUS = %s where "
                     "FLD_USER_ID = %s;")
            query_dl_table = ("update driving_license_detail set name_matching_ratio = %s "
                              "where user_id = %s;")
            cursor.execute(query, (status, profile_user_id,))
            cursor.execute(query_dl_table, (max_token_ratio, profile_user_id,))
            conn.commit()
            print("---updated successfully < 75---")
for user in data:
    user_id = user[0]
    error = user[1]
    if error != 'Invalid driving license format':
        query = "update TBL_PROFILE_DETAILS set FLD_DL_STATUS = 'dl details not found' where FLD_USER_ID = %s; "
        cursor.execute(query, (user_id,))
        conn.commit()
        print("---updated successfully unverified dl details---")
    elif error == "Invalid driving license format":
        query = ("update TBL_PROFILE_DETAILS set FLD_DL_STATUS = 'Invalid driving license format' where FLD_USER_ID = "
                 "%s;")
        cursor.execute(query, (user_id,))
        conn.commit()
        print("---updated successfully invalid dl details---")
if __name__ == '__main__':
    print("")
