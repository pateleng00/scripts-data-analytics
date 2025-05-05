from datetime import datetime
from vehicle_checkin_status import vehicle_details
from wallet_top_up_mail import wallet_topup_details


import os

os.chdir('/Users/akashkumar/Documents/GitHub/miscellaneous_code/Email_Services/')

currentDateAndTime = datetime.now()

# To fire at the beginning of the day
# wallet_credis = wallet_credits_last_day()
# wallet_credis.fetch_data()

vehicle_details = vehicle_details()
vehicle_details.fetch_vehicle_data()

# vehicle_details = vehicle_details()
# print(vehicle_details.fetch_vehicle_data())
#
# wallet_topup = wallet_topup_details()
# wallet_topup.fetch_wallet_data()

# backendjob = backend_jobs_alert()
# backendjob.fetch_data()

# # To fire during multiple hours to let space members know about the current app adoption #if
# currentDateAndTime.hour == 9 or currentDateAndTime.hour == 14 or currentDateAndTime.hour == 17 or
# currentDateAndTime.hour == 23: adoptionnos = app_adoption_nos() adoptionnos.fetch_data()
