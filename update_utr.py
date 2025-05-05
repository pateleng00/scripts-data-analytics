import time

import mysql
import requests
from mysql.connector import MySQLConnection


conn = mysql.connector.connect(host="localhost:3306", user="live_user", password="1PC^&uO$5",
                               database="mydb")
cursor = conn.cursor()


def get_pending_invoices_details():
    print("Connection established")
    cursor.execute(
        """SELECT id, invoice_number, transaction_status, utr_number, status
           FROM driver_invoice di
           WHERE invoice_month = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH))
             AND invoice_year = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH))
             AND transaction_status not in ('processed')
             AND remarks IS NULL ;"""
    )

    data = cursor.fetchall()

    bearer_token = 'eyJhbGciOiJIUzI1NiJ9..-xp_V0Q4vrnQ'

    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'Content-Type': 'application/json'
    }
    update_query = """update driver_invoice set utr_number =%s, utr_updated_at =now(), transaction_status =%s, 
    transaction_description = %s where id = %s ;"""
    for status_pening_invoice in data:
        invoice_id = status_pening_invoice[0]
        invoice_number = status_pening_invoice[1]
        print("invoice_id = ", invoice_id, "invoice_number = ", invoice_number)
        url = f'https://apiURL/finance/driver-invoice/verify-payout/{invoice_number}'
        response = requests.get(url, headers=headers)
        response = response.json()
        print(response)
        if response['success'] == True:
            data = response['data']
            utr = data['utr']
            status = data['status']
            message = data['message']
            cursor.execute(update_query, (utr, invoice_id, status, message,))
            conn.commit()
            print(f"payout status updated for invoice id- {invoice_id}.")
        else:
            continue
        time.sleep(2)

    conn.close()


if __name__ == '__main__':
    get_pending_invoices_details()
