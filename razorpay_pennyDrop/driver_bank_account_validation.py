import requests
from requests.auth import HTTPBasicAuth
import time
import mysql.connector

razor_pay_credentials_dict = {
    'RAZOR_PAY_KEY_ID': 'key',
    'RAZOR_PAY_KEY_SECRET': 'secret',
    'RAZOR_PAY_API_BASE_URL': 'https://api.razorpay.com/v1',
    'RAZOR_PAY_WEBHOOK_SECRET': 'webhookkey',
    'RAZOR_PAY_ACCOUNT_NUMBER': 'accountnumber', }


def _razor_pay_auth():
    return HTTPBasicAuth(
        username=razor_pay_credentials_dict["RAZOR_PAY_KEY_ID"],
        password=razor_pay_credentials_dict["RAZOR_PAY_KEY_SECRET"]
    )


conn_prod = mysql.connector.connect(host="localhost:3306", user="live_user", password="1PC^&uO$5",
                               database="mydb")

cursor = conn_prod.cursor()

query = ("""select tba.FLD_USER_ID , FLD_BANK_NAME , FLD_BENEFICIARY_NAME , FLD_IFSC_CODE , tud.FLD_LOGIN_NAME,  
 FLD_ACCOUNT_NUMBER, FLD_FUND_ACCOUNT_LAST_VALIDATION_ID from 
TBL_BANK_ACCOUNT tba inner join TBL_USER_DETAILS tud on tud.FLD_USER_ID  = tba.FLD_USER_ID where tba.FLD_STATUS 
='PENDING' and tud.FLD_STATUS <> 6 and tba.FLD_BENEFICIARY_NAME <> 
"DUMMY" and tba.FLD_USER_ID in (5659) GROUP BY FLD_ACCOUNT_NUMBER ;""")

cursor.execute(query)
user_details = cursor.fetchall()

update_approved_bank = (
    "update TBL_BANK_ACCOUNT set FLD_STATUS = 'APPROVED', FLD_FUND_ACCOUNT_LAST_VALIDATION_ID = %s , "
    "FLD_FUND_ACCOUNT_ID = %s , FLD_CONTACT_ACCOUNT_ID = %s where FLD_STATUS = 'PENDING' and "
    "FLD_ACCOUNT_NUMBER = %s")

update_last_validation_details = ("update TBL_BANK_ACCOUNT set FLD_FUND_ACCOUNT_LAST_VALIDATION_ID = %s , "
                                  "FLD_FUND_ACCOUNT_ID = %s , FLD_CONTACT_ACCOUNT_ID = %s where FLD_STATUS = "
                                  "'PENDING' and FLD_ACCOUNT_NUMBER = %s")


def fetch_account_validation_transactions_by_id(fund_validation_id):
    print("fetching fund Accounts details by validation id:- ", fund_validation_id)
    response = requests.get(
        f'https://api.razorpay.com/v1/fund_accounts/validations/{fund_validation_id}',
        auth=_razor_pay_auth()
    )
    data = response.json()
    fund_account_data = data.get('fund_account')
    result = data.get('results')
    print("result :- ", result)
    print("result[account_status] :- ", result['account_status'])
    bank_account_data = fund_account_data['bank_account']
    account_number_bank = bank_account_data['account_number']
    fund_account_id = fund_account_data['id']
    print(f"fund_account_id: {fund_account_id}")
    contact_id = fund_account_data['contact_id']
    print(f"contact_id: {contact_id}")
    fund_account_validation_id = data.get('id')
    print(f"fund_account_validation_id: {fund_account_validation_id}")
    if result['account_status'] == 'active':
        print("Into IF statement")
        cursor.execute(update_approved_bank,
                       (fund_account_validation_id, fund_account_id, contact_id, account_number_bank,))
        print(f"executed update -> query data fetched by fund account validation by id: {account_number_bank}")
        conn_prod.commit()
    elif result['account_status'] == 'None' or result['account_status'] == 'invalid':
        print("Into ELIF statement")
        cursor.execute(update_last_validation_details,
                       (fund_account_validation_id, fund_account_id, contact_id, account_number_bank,))
        conn_prod.commit()
    return response


def create_contact(cc_payload):
    response = requests.post(
        'https://api.razorpay.com/v1/contacts',
        json=cc_payload,
        auth=_razor_pay_auth()
    )
    return response


def create_fund_accounts(cc_payload):
    response = requests.post(
        'https://api.razorpay.com/v1/fund_accounts',
        json=cc_payload,
        auth=_razor_pay_auth()
    )
    return response


def fund_accounts_validation(validation_account):
    print(" calling api")
    response = requests.post(
        'https://api.razorpay.com/v1/fund_accounts/validations',
        json=validation_account,
        auth=_razor_pay_auth()
    )
    print("response :- ", response.json())
    return response


def validate_accounts(details):
    print("Validating fund Accounts :- ", resp_fund_accounts_validation.json())
    data = resp_fund_accounts_validation.json()
    result = data.get('results')
    print("result :- ", result)
    print("result[account_status] :- ", result['account_status'])
    bank_account_data = resp_fund_accounts_validation.json()['fund_account']['bank_account']
    account_number_bank = bank_account_data['account_number']
    fund_account_id = resp_fund_accounts_validation.json()['fund_account']['id']
    print(f"fund_account_id: {fund_account_id}")
    contact_id = resp_fund_accounts_validation.json()['fund_account']['contact_id']
    print(f"contact_id: {contact_id}")
    fund_account_validation_id = data.get('id')
    print(f"fund_account_validation_id: {fund_account_validation_id}")
    if result['account_status'] == 'active':
        print("Into IF statement")
        cursor.execute(update_approved_bank,
                       (fund_account_validation_id, fund_account_id, contact_id, account_number_bank,))
        print(f"executed update -> query data fetched by fund account validation: {account_number_bank}")
        conn_prod.commit()
    elif result['account_status'] == 'None' or result['account_status'] == 'invalid':
        print("Into ELIF statement")
        cursor.execute(update_last_validation_details,
                       (fund_account_validation_id, fund_account_id, contact_id, account_number_bank,))
        conn_prod.commit()


for user in user_details:
    mobile_number = user[4]
    user_id = user[0]
    bank_name = user[1]
    account_holder_name = user[2]
    ifsc = user[3]
    account_number = user[5]
    fund_validation_id = user[6]
    if fund_validation_id != None:
        fetch_account_validation_transactions_by_id(fund_validation_id)
    else:
        try:
            create_contact_payload = {
                "name": str(account_holder_name),
                "email": "test@example.com",
                "contact": int(mobile_number),
                "type": "vendor",
                "reference_id": str(user_id),
                "notes": {
                    "note_key": "Beam me up Scotty Updated"
                }
            }
            print(create_contact_payload)

            resp_create_contact = create_contact(create_contact_payload)
            c_id = resp_create_contact.json()["id"]
            print("c_id", c_id)

            funds_account = {
                "contact_id": c_id,
                "account_type": "bank_account",
                "bank_account": {
                    "name": str(account_holder_name),
                    "ifsc": str(ifsc),
                    "account_number": str(account_number)
                }
            }

            resp_fund_accounts = create_fund_accounts(funds_account)
            f_id = resp_fund_accounts.json()["id"]
            print("f_id", f_id)

            validation_account = {
                "account_number": razor_pay_credentials_dict['RAZOR_PAY_ACCOUNT_NUMBER'],
                "fund_account": {
                    "id": f_id
                },
                "amount": 100,
                "currency": "INR",
                "notes": {
                    "random_key_1": "Make it so.",
                    "random_key_2": "Tea. Earl Grey. Hot."
                }
            }
            resp_fund_accounts_validation = fund_accounts_validation(validation_account)
            print(resp_fund_accounts_validation)
            validate_accounts(resp_fund_accounts_validation)
        except:
            pass
            time.sleep(2)

if __name__ == '__main__':
    print("")
