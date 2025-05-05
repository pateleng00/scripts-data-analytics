import datetime
from json import dumps

import pytz
from httplib2 import Http
import psycopg2
from mysql.connector import MySQLConnection

from python_mysql_dbconfig import read_db_config

current_date = datetime.datetime.now()

formatted_string_gps = current_date.strftime("gps_data_%Y_%m_%d")
print(formatted_string_gps)

formatted_string_can = current_date.strftime("can_data_%Y_%m_%d")
conn = psycopg2.connect(database="post",
                        user="postgres",
                        host='yourdbhost',
                        password="pass",
                        port=5432)
cursor = conn.cursor()
print("Connecting to postgres")
gps_result = (f"select distinct on (vehicle_no) vehicle_no ,distance,  speed,  beacon_date_time from {formatted_string_gps} "
              f"gd order by vehicle_no, beacon_date_time desc ;")
cursor.execute(gps_result)
gps_rows = cursor.fetchall()
# print("GPS data", gps_rows)

can_result = (f"select distinct on (vehicle_no) vehicle_no, soc, motor_temperature , controller_temperature, "
              f"charge_cycle, beacon_date_time, odometer from {formatted_string_can} cd order by vehicle_no, "
              f"beacon_date_time desc ;")
cursor.execute(can_result)
can_rows = cursor.fetchall()
# print("CAN result", can_rows)


dbconfig = read_db_config()
mysql_connection = MySQLConnection(**dbconfig)
mysql_cursor = mysql_connection.cursor()


def insert_to_mysql_table():
    truncate_table = "TRUNCATE db.vehicle_soc_data "
    mysql_cursor.execute(truncate_table)
    print("table truncation done")

    vehicle_details = "SELECT FLD_RC_NUMBER from moevingdb.TBL_VEHICLE_DETAILS"

    mysql_cursor.execute(vehicle_details)
    data = mysql_cursor.fetchall()
    print("CURSOR_EXECUTED")
    return data


def post_to_webhook(bot_msg):
    bot_msg = {
        'text': f"```{bot_msg}```"}

    print(bot_msg['text'].replace('```', ''))
    message_headers = {'Content-Type': 'application/json; charset=UTF-8'}

    # Test
    # url = (
    #     'https://chat.googleapis.com/v1/spaces/AAAAQqFHSzs/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&')

    # Prod
    url = (
        'https://chat.googleapis.com/v1/spaces/AAAAKqbLC0g/messages?key=-WEfRq3CPzqKqqsHI&token=')

    http_obj = Http()
    response = http_obj.request(
        uri=url,
        method='POST',
        headers=message_headers,
        body=dumps(bot_msg)
    )


def select_from_deep_discharge():
    from_discharge_data = ("select vehicle_number,soc_threshold_count from "
                           "db.vehicle_deep_discharge_details")

    mysql_cursor.execute(from_discharge_data)
    print("deep discharge curser executed")
    discharge_data = mysql_cursor.fetchall()
    return discharge_data


def select_from_demand_fulfillment():
    demand_data = ("select tvd.FLD_RC_NUMBER , date(FLD_DRIVER_CHECKIN_TIME) from "
                   " moevingdb.TBL_DEMAND_FULFILMENT_DRIVER_DETAILS tdfdd inner join TBL_VEHICLE_DETAILS tvd on "
                   " db.tvd.FLD_VEHICLE_ID  = tdfdd.FLD_VEHICLE_ID where date(FLD_DRIVER_CHECKIN_TIME) = "
                   " CURRENT_DATE();")

    mysql_cursor.execute(demand_data)
    print("Data from demand fulfillment driver fetched")
    demand_fulfillment = mysql_cursor.fetchall()
    return demand_fulfillment


def call_data_insert():
    IST = pytz.timezone('Asia/Kolkata')
    start_time = datetime.datetime.now(IST).strftime('%a, %d %b %Y %H:%M:%S %Z(%z)')
    start_message = {"Data transfer started from Postgres to MySQL at: " + str(start_time)}
    today = datetime.datetime.today().date()
    current_date = datetime.datetime.today()

    post_to_webhook(start_message)

    insert_query = (
        "INSERT INTO db.vehicle_soc_data (vehicle_id, soc, updated_on, charge_cycles, odometer,motor_temperature, "
        "controller_temperature) VALUES (%s, %s, %s, %s, %s, %s, %s)")
    insert_to_mysql_table()

    insert_soc_threshold = ("INSERT INTO vehicle_deep_discharge_details (vehicle_number, soc_threshold_count, "
                            "updated_on) VALUES (%s, %s, %s)")
    update_deep_discharge_flag = ("update vehicle_deep_discharge_details set is_deep_discharged = %s, updated_on = "
                                  "%s, soc_threshold_count = %s  where vehicle_number = (%s)")
    update_soc_counter = ("update vehicle_deep_discharge_details set soc_threshold_count = %s, updated_on =%s where "
                          "vehicle_number = %s")
    reset_counter_deep_discharge = ("update vehicle_deep_discharge_details set soc_threshold_count = 0, "
                                    "is_deep_discharged = 0, updated_on = %s where vehicle_number = %s")
    update_vehicle_master_soc = "update db.TBL_VEHICLE_DETAILS set SOC = %s where FLD_RC_NUMBER = %s "
    update_vehicle_master_discharge_flag = ("update moevingdb.TBL_VEHICLE_DETAILS set FLD_IS_DEEP_DISCHARGED = 1 where "
                                            "FLD_RC_NUMBER = %s")
    reset_vehicle_deep_discharge_flag = ("update db.TBL_VEHICLE_DETAILS set FLD_IS_DEEP_DISCHARGED = 0 where "
                                         "FLD_RC_NUMBER = %s")

    deep_discharge = select_from_deep_discharge()
    result_dictionary = dict((x, y) for x, y in deep_discharge)
    checkin_data = select_from_demand_fulfillment()
    checkin_result = dict((x, y) for x, y in checkin_data)

    count = 0
    total_count = 0

    for vehicle in can_rows:
        vehicle_number = vehicle[0]
        print("can_veh", vehicle_number)
        soc = vehicle[1]
        print("can_soc", soc)
        updated_on = vehicle[5]
        print("can beacon", updated_on)
        charge_cycle = vehicle[4]
        print("can_charge_cycle", charge_cycle)
        odometer = vehicle[6]
        print("can_odo", odometer)
        motor_temp = vehicle[2]
        print("can_motor_temp", motor_temp)
        controller_temp = vehicle[3]
        print("can_controller", controller_temp)
        mysql_cursor.execute(insert_query,
                             (vehicle_number, soc, updated_on, charge_cycle, odometer, motor_temp, controller_temp))
        mysql_cursor.execute(update_vehicle_master_soc, (soc, vehicle_number,))
        dd_vehicle_threshold = result_dictionary.get(vehicle_number) if vehicle_number in result_dictionary else 0
        print("threshold", dd_vehicle_threshold)
        checkin_vehicle = checkin_result.get(vehicle_number) if vehicle_number in checkin_result else None
        print("checkin_vehicle", checkin_vehicle)
        if soc != None and soc < 25:
            if vehicle_number in result_dictionary:
                if dd_vehicle_threshold >= 72:
                    mysql_cursor.execute(update_deep_discharge_flag,
                                         (1, updated_on, dd_vehicle_threshold + 1, vehicle_number,))
                    mysql_cursor.execute(update_vehicle_master_discharge_flag, (vehicle_number,))
                    mysql_connection.commit()
                else:
                    mysql_cursor.execute(update_soc_counter, (dd_vehicle_threshold + 1, updated_on, vehicle_number,))
                    mysql_connection.commit()
            else:
                mysql_cursor.execute(insert_soc_threshold, (vehicle_number, 1, updated_on))
                mysql_connection.commit()
        else:
            mysql_cursor.execute(reset_counter_deep_discharge, (current_date, vehicle_number,))
            mysql_cursor.execute(reset_vehicle_deep_discharge_flag, (vehicle_number,))
            mysql_connection.commit()
        if vehicle_number in checkin_result.keys():
            if checkin_vehicle:
                mysql_cursor.execute(reset_counter_deep_discharge, (current_date, vehicle_number,))
                mysql_connection.commit()
        print("Inserting into moevingdb.vehicle_soc_data for:-" + vehicle_number + ", Updated On:" + str(
            updated_on) + ", SOC:" + str(dd_vehicle_threshold))
        total_count = total_count + 1
        if updated_on.date() == today:
            count = count + 1
        mysql_connection.commit()
    for vehicle in gps_rows:
        if vehicle[0] is not None and "distance" in vehicle[0]:
            if gps_rows[0] != None:
                vehicle_id = vehicle[0]
                print("Vehicle", vehicle_id)
                distance_covered = vehicle[1]
                print("Distance covered", distance_covered)
                updated_on = vehicle[3]
                print("gps beacon", updated_on)
                speed = vehicle[2]
                print("gps_speeed", speed)
                if updated_on.date() == today and distance_covered > 2:
                    if vehicle_id in result_dictionary:
                        mysql_cursor.execute(reset_counter_deep_discharge, (current_date, vehicle_id,))
                        mysql_connection.commit()
                        print(
                            "Reset deep discharge flag based on distance covered and speed of vehicle "
                            "db.deep_dischage_flag for:- " + vehicle_id + ", GPS data updated at:" + str(
                                updated_on) + ", distance covered:- " + str(distance_covered) + ", speed:- " + str(
                                speed))

    end_time = datetime.datetime.now(IST).strftime('%a, %d %b %Y %H:%M:%S %Z(%z)')
    end_message = {"Data transfer has been done from Postgres to MySQL at: " + str(end_time) + "Total "
                                                                                               "Vehicles "
                                                                                               "count "
                                                                                               "with "
                                                                                               "latest "
                                                                                               "data of "
                                                                                               "current "
                                                                                               "date are: "
                                                                                               "" + str(
        count) + "/" + str(total_count)}
    mysql_connection.commit()
    mysql_connection.close()
    post_to_webhook(end_message)


if __name__ == '__main__':
    call_data_insert()
