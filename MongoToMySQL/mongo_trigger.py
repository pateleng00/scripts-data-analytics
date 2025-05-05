from datetime import datetime

from deep_discharge_check import call_data_insert

import os

os.chdir('/Users/akashkumar/Documents/GitHub/miscellaneous_code/NotBot/')
# os.chdir('/home/ubuntu/mongo_to_mysql/')

currentDateAndTime = datetime.now()

call_data_insert()

