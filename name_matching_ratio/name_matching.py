from fuzzywuzzy import fuzz
from mysql.connector import MySQLConnection


import mysql.connector

conn = mysql.connector.connect(host="localhost:3306", user="abcd", password="abcd",
                               database="mydb")
cursor = conn.cursor()

bank_benif_name = """select FLD_BANK_ACCOUNT_ID, FLD_BENEFICIARY_NAME from TBL_BANK_ACCOUNT tba where 
FLD_ACCOUNT_STATUS = 'active' and FLD_NAME_MATCHING_RATIO is null order by FLD_BANK_ACCOUNT_ID;"""
cursor.execute(bank_benif_name)
bank_benif = cursor.fetchall()
print('bank_benif', bank_benif)

verified_name = """select FLD_BANK_ACCOUNT_ID, FLD_VERIFIED_ACCOUNTHOLDER_NAME from TBL_BANK_ACCOUNT tba 
where FLD_ACCOUNT_STATUS = 'active' and FLD_NAME_MATCHING_RATIO is null order by FLD_BANK_ACCOUNT_ID;"""
cursor.execute(verified_name)
verified_user_name = cursor.fetchall()
print('verified_user_name', verified_user_name)

for benife_user, verify_user in zip(bank_benif, verified_user_name):
    print("benife_user", benife_user)
    benif_id = benife_user[0]
    benif_name = benife_user[1]
    print("verify_user", verify_user)
    verif_id = verify_user[0]
    verif_name = verify_user[1]

    if benif_id == verif_id:
        partial_token_ratio = fuzz.partial_ratio(benif_name, verif_name)
        token_sort_ratio = fuzz.token_sort_ratio(benif_name, verif_name)
        max_token_ratio = max(partial_token_ratio, token_sort_ratio)
        print(f"max_token_ratio of user_id {benif_id} after matching name -: {max_token_ratio}")
        if max_token_ratio >= 75:
            status = max_token_ratio
            query = "update TBL_BANK_ACCOUNT set FLD_NAME_MATCHING_RATIO = %s where FLD_BANK_ACCOUNT_ID = %s; "
            # cursor.execute(query, (status, benif_id,))
            conn.commit()
            print("---updated successfully > 75---")
        elif max_token_ratio < 75:
            status = max_token_ratio
            query = "update TBL_BANK_ACCOUNT set FLD_NAME_MATCHING_RATIO = %s where FLD_BANK_ACCOUNT_ID = %s;"
            # cursor.execute(query, (status, benif_id,))
            conn.commit()
            print("---updated successfully < 75---")

if __name__ == '__main__':
    print("")
