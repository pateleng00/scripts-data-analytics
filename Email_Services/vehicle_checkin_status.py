import datetime
from email.mime.text import MIMEText

import pandas as pd
from mysql.connector import MySQLConnection
from python_mysql_dbconfig import read_db_config
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


class vehicle_details:
    def fetch_vehicle_data(self):
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor()
        print("Connected")
        sqlquery = ("select tvd.FLD_VEHICLE_ID as VehicleId , tvd.FLD_RC_NUMBER as RcNumber, case when  "
                    "tdfdd.FLD_DRIVER_CHECKIN_TIME is not NULL THEN 1 ELSE 0 END AS CheckInDoneNot, tud.FLD_USER_NAME "
                    "as DriverName, tud.FLD_LOGIN_NAME as MobileNumber from  TBL_VEHICLE_DETAILS tvd left outer join "
                    "TBL_DEMAND_FULFILMENT_DRIVER_DETAILS tdfdd  on tdfdd.FLD_VEHICLE_ID  =  tvd.FLD_VEHICLE_ID and "
                    "date(tdfdd.FLD_DRIVER_CHECKIN_TIME)= CURRENT_DATE()  - INTERVAL 1 day left join TBL_USER_DETAILS "
                    "tud on tud.FLD_USER_ID = tdfdd.FLD_DRIVER_USER_ID where tvd.FLD_STATUS = 3 and "
                    "tvd.FLD_VEHICLE_CITY_ID not in (46,47) ;")
        cursor.execute(sqlquery)
        data = cursor.fetchall()
        print(data)
        column_names = ["VehicleId", "RcNumber", 'CheckInDone-Not', 'Driver Name', 'Mobile Number']
        df = pd.DataFrame(data, columns=column_names)

        # Convert DataFrame to Excel
        excel_file = 'data.xlsx'
        df.to_excel(excel_file, index=False)

        # Email Configuration
        day = datetime.datetime.today() - datetime.timedelta(days=1)
        yesterday = day.strftime("%A %d. %B %Y")
        sender_email = 'connect@abcd.com'
        receiver_emails = ['akash.kumar@abcd.com', 'program@abcd.com']
        body = "\nHi All\nPlease find the attached Excel file containing Last day's vehicle check-in data.\nDated :-  " + str(
            yesterday)
        subject = "Last day's vehicle check-in data"
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
        smtp_username = 'system.insights@abcd.com'
        smtp_password = 'abcd abcd abcd abcd'

        # Create a secure connection with the SMTP server
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)

        # Send the email
        server.sendmail(sender_email, receiver_emails, message.as_string())
        server.quit()

        print('Email sent successfully!')
