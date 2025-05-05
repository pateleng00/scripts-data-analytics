import datetime
import operator
from json import dumps

import pymongo
import pytz
from httplib2 import Http
from mysql.connector import MySQLConnection

from python_mysql_dbconfig import read_db_config

# MongoDB's connection settings
mongo_client = pymongo.MongoClient(
    "mongodb://user:pass@hostname:27017/")
mongo_db = mongo_client["dbName"]
mongo_collection = mongo_db["Collection"]

dbconfig = read_db_config()
mysql_connection = MySQLConnection(**dbconfig)
cursor = mysql_connection.cursor()


def get_latest_timestamp_data(vehicle_no):
    # print(vehicle_no)
    pipeline = [
        {
            "$group": {
                "_id": "$_id",  # Replace "key" with the MongoDB key field
                "latest_timestamp": {"$max": "$beaconDateTime"},  # Replace "timestamp" with your timestamp field
                "data": {"$first": "$$ROOT"}  # This will get the entire document with the latest timestamp
            },
        }
    ]

    result = mongo_collection.find_one(vehicle_no)
    result_list = result["canMap"]
    # print(len(result_list))
    return result_list


def update_mysql_table():
    truncate_table = "TRUNCATE db.vehicle_soc_data "
    cursor.execute(truncate_table)
    print("table truncation done")

    vehicle_details = ("SELECT FLD_RC_NUMBER from moevingdb.TBL_VEHICLE_DETAILS WHERE "
                       "TBL_VEHICLE_DETAILS.FLD_STATUS = 3 ")

    cursor.execute(vehicle_details)
    data = cursor.fetchall()
    print("CURSOR_EXECUTED")
    return data


def ter_process(some_list):
    try:
        some_list.sort(key=operator.itemgetter('beaconDateTime'), reverse=False)
        data = some_list[-1]
        return data
    except Exception as e:
        pass


def post_to_webhook(bot_msg):
    bot_message = bot_msg
    bot_message = {
        'text': f"```{bot_message}```"}

    print(bot_message['text'].replace('```', ''))
    message_headers = {'Content-Type': 'application/json; charset=UTF-8'}

    # Test
    # url = ('https://chat.googleapis.com/v1/spaces/AAAAQqFHSzs/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI'
    #        '&token=Ob')

    # Prod
    url = ('https://chat.googleapis.com/v1/spaces/AAAAKqbLC0g/messages?key=-WEfRq3CPzqKqqsHI'
           '&token=RFAWBu3kmrLlG')
    http_obj = Http()
    response = http_obj.request(
        uri=url,
        method='POST',
        headers=message_headers,
        body=dumps(bot_message)
    )


def call_data_insert():
    IST = pytz.timezone('Asia/Kolkata')
    start_time = datetime.datetime.now(IST).strftime('%a, %d %b %Y %H:%M:%S %Z(%z)')
    start_message = {"Data transfer started from MongoDB to MySQL at: " + str(start_time)}
    today = datetime.datetime.today().date()

    post_to_webhook(start_message)

    insert_query = "INSERT INTO db.vehicle_soc_data (vehicle_id, soc, updated_on) VALUES (%s, %s, %s)"
    data_to_be_inserted = update_mysql_table()

    some_result = {}
    for row in data_to_be_inserted:
        try:
            resp = ter_process(get_latest_timestamp_data(row[0]))
            some_result[row[0]] = resp
        except Exception as e:
            pass

    # print(len(some_result))
    count = 0
    total_count = 0
    for vehicle_id in some_result.keys():
        soc = some_result[vehicle_id]["soc"]
        updated_on = some_result[vehicle_id]["beaconDateTime"]
        # print(vehicle_id, soc, updated_on)
        cursor.execute(insert_query, (vehicle_id, soc, updated_on))
        print("Inserting into moevingdb.vehicle_soc_data for " + vehicle_id)
        total_count = total_count + 1
        if updated_on.date() == today:
            count = count + 1
        mysql_connection.commit()
    end_time = datetime.datetime.now(IST).strftime('%a, %d %b %Y %H:%M:%S %Z(%z)')
    end_message = {"Data transfer has been done from MongoDB to MySQL at: " + str(end_time) + "Total "
                                                                                              "Vehicles "
                                                                                              "count "
                                                                                              "with "
                                                                                              "latest "
                                                                                              "data of "
                                                                                              "current "
                                                                                              "date are: "
                                                                                              "" + str(
        count) + "/" + str(total_count)}
    post_to_webhook(end_message)
    mysql_connection.close()
