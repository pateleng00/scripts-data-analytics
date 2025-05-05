# Import the required modules
import imaplib
import email
import csv
import mysql.connector
from datetime import datetime, timedelta


# Define the email and password for login
email1 = "abcd@myorg.com"
password = "abcd abcs abcd abcd"
# Define the subject to search for
current_date = datetime.now()
new_date = current_date - timedelta(days=1)
subject = "3W - EV daily report - Dated:" + new_date.strftime("%d-%m-%Y")
# Create an IMAP4_SSL object and login
mail = imaplib.IMAP4_SSL("imap.gmail.com")
mail.login(email1, password)
# Select the inbox folder
mail.select("inbox")
# Get the current date in the format required by IMAP
date = datetime.now().strftime("%d-%b-%Y")
# Search for emails with the given subject and date
typ, data = mail.search(None, '(SUBJECT "{}" SINCE "{}")'.format(subject, date))
# MySQL database connection & smtp email details
host = "localhost"
database = "mydb"
user = "myuser"
password = "mypass"
# Create a connection to the MySQL database
db = mysql.connector.connect(host=host, user=user, password=password, database=database)
cursor = db.cursor()
# Loop through the ids of the matching emails
for num in data[0].split():
    # Fetch the email content
    typ, data = mail.fetch(num, "(RFC822)")
    # Parse the email into an email message object
    msg = email.message_from_bytes(data[0][1])
    # Print the sender and the subject
    print("From:", msg["From"])
    print("Subject:", msg["Subject"])
    # Loop through the parts of the email
    for part in msg.walk():
        # If the part is an attachment
        if part.get_content_maintype() == "multipart":
            continue
        if part.get("Content-Disposition") is None:
            continue
        # If the attachment is a csv file
        if part.get_filename().startswith("driver"):
            # Read the attachment content as bytes
            content = part.get_payload(decode=True)
            # Decode the content as a string
            content = content.decode("utf-8")
            # Create a csv reader object
            reader = csv.reader(content.splitlines())
            # Loop through the rows of the csv file
            for row in reader:
                if row[0] != "Date":
                    # Remove hyphens from the vehicle_number field
                    row[4] = row[4].replace("-", "") if row[4] else None
                    # cursor.execute('Insert into porter_driver values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', row)
                    insertqry = """insert into client_mis_from_porter (Date, driver_name, driver_mobile, driver_geo_region, vehicle_number, vehicle_type, time_spent_on_orders_in_clusters_in_hrs, time_spent_idle_in_clusters_in_hrs, total_time_spent_in_clusters_hrs, time_spent_on_orders_in_hrs, time_spent_idle_in_hrs, total_login_time_in_hrs, distance_travelled_during_order_in_clusters_kms, distance_travelled_while_idle_in_clusters_kms, total_distance_in_clusters_kms, first_recorded_login_time_in_cluster, first_location_in_cluster, last_recorded_login_time_in_cluster, last_location_in_cluster, total_notifs_below_20_km, accept_notifs_below_20_km, missed_notifs_below_20_km, pct_notifs_acceptance_below_20_km, total_notifs_overall, accept_notifs_overall, missed_notifs_overall, pct_notifs_acceptance_overall, allocated_orders, completed_orders, cancelled_reasons, rated_orders, avg_driver_rating, trip_fare, helper_charges, toll_charges, dryrun_incentives, trip_incentives, login_incentives, mbg_incentives, manual_adjustments, post_migration_payout, rnr_awards, withdrawals, trip_fare_components, commission, cash_collected, wallet_transactions, vendor_email)
                                values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    cursor.execute(insertqry, row)
db.commit()
cursor.close()
db.close()
print("Mail " + subject + " inserted in database successfully")
