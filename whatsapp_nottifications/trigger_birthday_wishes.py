from datetime import datetime


# from birthday_wishes import BirthdayWhatsApp
from birthday_noti_windblade import BirthdayWhatsAppWindblade
import os

os.chdir('/Users/akashkumar/Documents/GitHub/miscellaneous_code/whatsapp_nottifications/')

currentDateAndTime = datetime.now()


birthday = BirthdayWhatsAppWindblade()
birthday.get_drivers_detail()






