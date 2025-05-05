from datetime import datetime

import mysql
import psycopg2
from mysql.connector import MySQLConnection

conn = psycopg2.connect(database="post",
                        user="postgres",
                        host='yourdbhost',
                        password="pass",
                        port=5432)
cursor = conn.cursor()
print("Connecting to postgres")
can_result_4w = """select  distinct on (vehicle_no) vehicle_no, tvd.fld_vehicle_id, beacon_date_time, odometer from can_data cd 
inner join tbl_vehicle_details tvd on tvd.fld_rc_number = cd.vehicle_no
where  odometer >0 and tvd.fld_vehicle_type_id in (3) and tvd.oem_id not in (12)
order by vehicle_no, beacon_date_time desc;
"""
cursor.execute(can_result_4w)
can_rows_4w = cursor.fetchall()

mysql_conn = mysql.connector.connect(host="localhost:3306", user="abcd", password="abcd",
                               database="mydb")
mysql_cursor = mysql_conn.cursor()
print("Connecting to mysql...")

data = """select vehicle_id from last_odometer_data lod ;"""
mysql_cursor.execute(data)
vehicle_details = mysql_cursor.fetchall()


insert_query = """INSERT INTO moevingdb.last_odometer_data
(vehicle_id, vehicle_no, last_odometer_reading_time, last_odometer_reading)
VALUES(%s, %s, %s, %s);"""

update_query = """update moevingdb.last_odometer_data set last_odometer_reading_time = %s, last_odometer_reading =%s 
where vehicle_id = %s"""

print("Vehicle details", vehicle_details)

for odometer in can_rows_4w:
    vehicle_number = odometer[0]
    vehicle_id = odometer[1]
    last_odometer_time = odometer[2]
    odometer_reading = odometer[3]
    timestamp = last_odometer_time.strftime("%Y-%m-%d %H:%M:%S")
    vehicle_found = False
    for vehicle in vehicle_details:
        if vehicle[0] == vehicle_id:
            print(f"Vehicle details.Id: {vehicle_id}")
            mysql_cursor.execute(update_query, (timestamp, odometer_reading, vehicle_id,))
            mysql_conn.commit()
            print(f"Updating 4 wheeler data other than switch mobility for {vehicle_number}.")
            vehicle_found = True
            break
    if not vehicle_found:
        mysql_cursor.execute(insert_query, (vehicle_id, vehicle_number, timestamp, odometer_reading))
        mysql_conn.commit()
        print(f"Inserting 4 wheeler data other than switch mobility for {vehicle_number}.")

if __name__ == "__main__":
    print("Starting")
