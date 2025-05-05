from datetime import datetime

from windblade_api_call_for_push_noti import PushNotification
from driver_training_completed import PushNotificationTrainingCompleted
from checkin_reminder import PushNotificationCheckIn
import os

os.chdir('/Users/akashkumar/Documents/GitHub/miscellaneous_code/push_notification/')

currentDateAndTime = datetime.now()


# trigger_push_not = PushNotification()
# trigger_push_not.get_user_session()

# training_completed = PushNotificationTrainingCompleted()
# training_completed.get_supervisor_details()

checkin = PushNotificationCheckIn()
checkin.get_attendance_details()






