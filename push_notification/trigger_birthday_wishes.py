from datetime import datetime


from driver_birthday_wishes import BirthdayWishes
import os

os.chdir('/Users/akashkumar/Documents/GitHub/miscellaneous_code/push_notification/')

currentDateAndTime = datetime.now()


# trigger_push_not = PushNotification()
# trigger_push_not.get_user_session()

# training_completed = PushNotificationTrainingCompleted()
# training_completed.get_supervisor_details()

checkin = BirthdayWishesW()
checkin.get_drivers_detail()






