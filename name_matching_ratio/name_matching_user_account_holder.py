from fuzzywuzzy import fuzz
from mysql.connector import MySQLConnection


import mysql.connector

conn = mysql.connector.connect(host="localhost:3306", user="abcd", password="abcd",
                               database="mydb")
cursor = conn.cursor()

user_name = """select tba.FLD_BANK_ACCOUNT_ID, tud.FLD_USER_NAME from TBL_USER_DETAILS tud 
inner join TBL_BANK_ACCOUNT tba on tba.FLD_USER_ID = tud.FLD_USER_ID 
where FLD_ACCOUNT_STATUS = 'active' order by FLD_BANK_ACCOUNT_ID ;"""
cursor.execute(user_name)
users_name = cursor.fetchall()
print('bank_benif', users_name)

verified_name = """select FLD_BANK_ACCOUNT_ID, FLD_VERIFIED_ACCOUNTHOLDER_NAME from TBL_BANK_ACCOUNT tba 
where FLD_ACCOUNT_STATUS = 'active' order by FLD_BANK_ACCOUNT_ID ;"""
cursor.execute(verified_name)
verified_user_name = cursor.fetchall()
print('verified_user_name', verified_user_name)

for user_name, verify_user in zip(users_name, verified_user_name):
    print("user_name", user_name)
    user_id = user_name[0]
    usr_name = user_name[1]
    print("verify_user", verify_user)
    verif_id = verify_user[0]
    verif_name = verify_user[1]

    if user_id == verif_id:
        partial_token_ratio = fuzz.partial_ratio(usr_name, verif_name)
        token_sort_ratio = fuzz.token_sort_ratio(usr_name, verif_name)
        max_token_ratio = max(partial_token_ratio, token_sort_ratio)
        print(f"max_token_ratio of user_id {user_id} after matching name -: {max_token_ratio}")
        if max_token_ratio >= 75:
            status = max_token_ratio
            query = "update TBL_BANK_ACCOUNT set FLD_USER_NAME_MATCHING_RATIO = %s where FLD_BANK_ACCOUNT_ID = %s; "
            cursor.execute(query, (status, verif_id,))
            conn.commit()
            print("---updated successfully > 75---")
        elif max_token_ratio < 75:
            status = max_token_ratio
            query = "update TBL_BANK_ACCOUNT set FLD_USER_NAME_MATCHING_RATIO = %s where FLD_BANK_ACCOUNT_ID = %s;"
            cursor.execute(query, (status, verif_id,))
            conn.commit()
            print("---updated successfully < 75--`-")

if __name__ == '__main__':
    print("")
