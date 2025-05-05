import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import time

df_file = pd.read_excel(
    r"bank_master.xlsx",
    "Sheet3"
)


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


def validate_accounts(resp_fund_accounts_validation, index):
    print("Validating fund Accounts :- ", resp_fund_accounts_validation.json())
    print(df_file.head())
    df_file.at[index, 'Validation Id'] = resp_fund_accounts_validation.json()["id"]
    df_file.at[index, 'Account Status'] = resp_fund_accounts_validation.json()["results"]["account_status"]
    df_file.at[index, 'UTR'] = resp_fund_accounts_validation.json()["utr"]
    df_file.at[index, 'Created At'] = resp_fund_accounts_validation.json()["created_at"]
    df_file.to_csv("TT.csv")
    return df_file


index = 0
for index, row in df_file.iterrows():
    try:
        razor_pay_credentials_dict = {
            'RAZOR_PAY_KEY_ID': 'key',
            'RAZOR_PAY_KEY_SECRET': 'secret',
            'RAZOR_PAY_API_BASE_URL': 'https://api.razorpay.com/v1',
            'RAZOR_PAY_WEBHOOK_SECRET': 'webhookkey',
            'RAZOR_PAY_ACCOUNT_NUMBER': 'accountnumber', }

        create_contact_payload = {
            "name": str(df_file["account_holder_name"][index]),
            "email": "test@example.com",
            "contact": int(df_file["contact_number"][index]),
            "type": "vendor",
            "reference_id": str(df_file["user_id"][index]),
            "notes": {
                "note_key": "Beam me up Scotty Updated"
            }
        }
        print(create_contact_payload)


        def _razor_pay_auth():
            return HTTPBasicAuth(
                username=razor_pay_credentials_dict["RAZOR_PAY_KEY_ID"],
                password=razor_pay_credentials_dict["RAZOR_PAY_KEY_SECRET"]
            )


        resp_create_contact = create_contact(create_contact_payload)
        print("response:- ", resp_create_contact.json())
        c_id = resp_create_contact.json()["id"]
        print("c_id", c_id)

        funds_account = {
            "contact_id": c_id,
            "account_type": "bank_account",
            "bank_account": {
                "name": str(df_file["account_holder_name"][index]),
                "ifsc": str(df_file["ifsc_code"][index]),
                "account_number": str(df_file["account_number"][index])
            }
        }
        print(funds_account)

        resp_fund_accounts = create_fund_accounts(funds_account)
        print("Creating_fund_accounts :- ", resp_fund_accounts.json())
        f_id = resp_fund_accounts.json()["id"]
        print("f_id", f_id)

        # validating back accounts

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
        print("validation_account:- ", validation_account)
        resp_fund_accounts_validation = fund_accounts_validation(validation_account)
        validate_accounts(resp_fund_accounts_validation, index)
    except:
        pass
        index += 1
        time.sleep(4)
if __name__ == '__main__':
    print("")
