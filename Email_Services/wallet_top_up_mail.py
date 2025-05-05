import datetime
from email.mime.text import MIMEText

import pandas as pd
from mysql.connector import MySQLConnection
from python_mysql_dbconfig import read_db_config
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


class wallet_topup_details:
    def fetch_wallet_data(self):
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor()
        print("Connected")
        sqlquery = "select date(l.transaction_date)  as TopUpDate, tld.FLD_LOCATION_NAME as City, concat(concat(" \
                   "'MOEV', upper(substring(tld.FLD_LOCATION_NAME, 1, 3))), l.user_id) as 'DriverId', " \
                   "tud.FLD_USER_NAME as DriverName , l.amount, (select  sum(l2.amount) from ledger l2 where month(" \
                   "l2.transaction_date) = month(NOW()) and YEAR(l2.transaction_date) = YEAR(now()) and l2.user_id = " \
                   "l.user_id and l2.transaction_detail in (41,49) and l2.status_id = 43 and l2.is_active = 1 GROUP by " \
                   "user_id) as MonthlyTotal  from ledger l inner join TBL_USER_DETAILS tud on " \
                   "tud.FLD_USER_ID  = l.user_id inner join TBL_LOCATION_DETAILS tld on tld.FLD_LOCATION_ID  = " \
                   "tud.FLD_OPERATING_CITY_ID where date(transaction_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) " \
                   "and l.transaction_detail in (41, 49) and l.status_id = 43 and l.is_active = 1 and " \
                   "tud.FLD_OPERATING_CITY_ID not in (46,47) order by l.transaction_date desc ;"
        cursor.execute(sqlquery)
        data = cursor.fetchall()
        print(data)
        column_names = ["TopUpDate", "City", 'DriverId', 'DriverName', 'Amount', 'MonthlyTotal']
        df = pd.DataFrame(data, columns=column_names)

        # Convert DataFrame to Excel
        excel_file = 'data.xlsx'
        df.to_excel(excel_file, index=False)

        # Email Configuration
        day = datetime.datetime.today() - datetime.timedelta(days=1)
        yesterday = day.strftime("%A %d. %B %Y")
        sender_email = 'connect@abcd.com'
        receiver_emails = ['mywnail@sbcd.com', 'myemail1@sbcd.com']
        body = "\nHi All\nPlease find the attached Excel file containing Last day's driver wallet top-up data.\nDated " \
               ":-  " + str(yesterday)
        subject = "Last day's driver wallet top-up"
        print(body)
        # Create the email message
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = ', '.join(receiver_emails)
        message['Subject'] = subject
        message.attach(MIMEText(body, "plain"))

        # Attach the Excel file
        attachment = open(excel_file, 'rb')
        excel_attachment = MIMEBase('application', 'octet-stream')
        excel_attachment.set_payload(attachment.read())
        encoders.encode_base64(excel_attachment)
        excel_attachment.add_header('Content-Disposition', "attachment; filename= %s" % excel_file)
        message.attach(excel_attachment)

        # SMTP Configuration and Sending Email
        smtp_server = 'smtp.gmail.com'
        smtp_port = 587
        smtp_username = 'sender_email'
        smtp_password = 'sender_password'

        # Create a secure connection with the SMTP server
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)

        # Send the email
        server.sendmail(sender_email, receiver_emails, message.as_string())
        server.quit()

        print('Email sent successfully!')


if __name__ == '__main__':
    wallet_topup_details().fetch_wallet_data()
