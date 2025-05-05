import json
import time
import urllib.parse
from datetime import datetime

import certifi
import pymongo
import requests
from flask import Flask, request, jsonify
from mysql.connector import MySQLConnection

from python_mysql_dbconfig import read_db_config

dbconfig = read_db_config()
conn = MySQLConnection(**dbconfig)
cursor = conn.cursor()
# MongoDB connection URI components
username = 'user'
password = urllib.parse.quote_plus("password")
host = 'host'
port = '27017'
database_name = 'dbName'

db = "db"
collection = 'whatsapp_bot_message'


# print("Connected to db")


def connect_to_mongo(username, password, host, port, db_name, collection_name):
    # Construct MongoDB connection URI
    uri = f'mongodb://{username}:{password}@{host}:{port}/'

    while True:
        try:
            # Establish connection to MongoDB
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)

            # Try to access the database to check if the connection is successful
            db = client[db_name]
            collection = db[collection_name]

            # If no exception is raised, the connection is successful
            print("Connected to MongoDB and database")
            return client, db, collection
        except pymongo.errors.ConnectionFailure as e:
            print(f"Failed to connect to MongoDB: {e}")
            print("Retrying in 1 hour...")
            time.sleep(600)  # Wait for 1 hour before retrying


client, db, collection = connect_to_mongo(username, password, host, port, db, collection)

app = Flask(__name__)

current_date = datetime.today()


def get_address(latitude, longitude):
    url = f"https://nominatim.openstreetmap.org/reverse?lat={latitude}&lon={longitude}&format=json&addressdetails=1"
    response = requests.get(url, verify=certifi.where())  # Verifies SSL certificate
    data = response.json()
    return data.get('display_name', 'Address not found')


def save_incoming_msg(data):
    try:
        src_addr = data['mobile']
        text = data['text']

        # Constructing the conversation object
        conversation = {
            "_id": src_addr,  # Assuming dest_addr is unique
            "chat": [
                {
                    "createdDateTime": current_date,
                    "sender": "user",
                    "message": text,
                }
            ]
        }
        # Inserting or updating the conversation
        collection.update_one({"_id": src_addr}, {"$push": {"chat": {"$each": conversation["chat"]}}}, upsert=True)

        print("--------------message data inserted to mongo------------------")

    except Exception as e:
        print("Error saving to mongo incoming:", e)


def save_incoming_msg_lead_type(data, lead_type):
    try:
        src_addr = data['mobile']
        text = data['text']

        # Constructing the conversation object
        conversation = {
            "_id": src_addr,  # Assuming dest_addr is unique
            "chat": [
                {
                    "createdDateTime": current_date,
                    "sender": "user",
                    "message": text if text else None,
                    "leadTypeId": lead_type
                }
            ]
        }
        # Inserting or updating the conversation
        collection.update_one({"_id": src_addr}, {"$push": {"chat": {"$each": conversation["chat"]}}}, upsert=True)

        print("--------------message data inserted to mongo------------------")

    except Exception as e:
        print("Error saving to mongo incoming lead type:", e)


def save_outgoing_msg(phone, status, msg_id, sent_msg):
    try:
        phone_number = phone
        # Constructing the conversation object
        conversation = {
            "_id": phone_number,  # Assuming dest_addr is unique
            "chat": [
                {
                    "createdDateTime": current_date,
                    "sender": "bot",
                    "message": sent_msg,
                    "msg_id": msg_id if msg_id else None,
                    "status": status,
                }
            ]
        }
        # Inserting or updating the conversation
        collection.update_one({"_id": phone_number}, {"$push": {"chat": {"$each": conversation["chat"]}}}, upsert=True)

        print("--------------message data inserted to mongo------------------")

    except Exception as e:
        print("Error saving to mongo outgoing msg:", e)


initial_msg = {
    'msg': """I'm here to make your Electric Vehicle driving experience smoother and stress-free.
Here you will get assistance for one of the following menu options:

1.Assigned Vehicle and Check-in information 🚚
2. Assigned Client and Reporting Location 🧑‍💼
3.Nearby Charging Stations 🔌
4.MoEVing Hub Parking Details 🆘"""}

initial_headers = {
    'header': "Welcome to MoEVing's WhatsApp Chatbot Service!",
}


def get_driver_demand_details(contact_number):
    dbconfig = read_db_config()
    conn = MySQLConnection(**dbconfig)
    cursor = conn.cursor()
    query = (
        f"select tvd.FLD_RC_NUMBER, tdfdd.FLD_DRIVER_CHECKIN_TIME, tcm.FLD_NAME , "
        f"tcrl.FLD_REPORTING_LOCATION_SHORT_NAME, tpd.FLD_NAME , tpd.FLD_PHONE_NUMBER , th.FLD_HubName , "
        f"th.FLD_latitude , th.FLD_longitude , tld.FLD_LOCATION_NAME, tcrl.FLD_ADDRESS, tcrl.FLD_LAT , tcrl.FLD_LON   "
        f"from TBL_USER_DETAILS tud left join "
        f"TBL_DEMAND_FULFILMENT_DRIVER_DETAILS tdfdd on tdfdd.FLD_DRIVER_USER_ID = tud.FLD_USER_ID left join "
        f"TBL_VEHICLE_DETAILS tvd on tvd.FLD_VEHICLE_ID  = tdfdd.FLD_VEHICLE_ID left join TBL_DEMAND_TEMPLATE tdt on "
        f"tdt.FLD_DEMAND_TEMPLATE_ID = tdfdd.FLD_DEMAND_TEMPLATE_ID  left join TBL_COMPANY_MASTER tcm on "
        f"tcm.FLD_COMPANY_ID  = tdt.FLD_CLIENT_COMPANY_ID left join TBL_COMPANY_REPORTING_LOCATION tcrl on "
        f"tcrl.FLD_COMPANY_REPORTING_LOCATION_ID = tdt.FLD_REPORTING_LOCATION_ID "
        f"left join TBL_DRIVER_DETAILS tdd on tdd.FLD_DRIVER_USER_ID = tud.FLD_USER_ID left join "
        f"TBL_ENTREPRENEUR_DETAILS ted on ted.FLD_ENTREPRENEUR_USER_ID = tdd.FLD_ENTREPRENEUR_USER_ID left join "
        f"TBL_PROFILE_DETAILS tpd on tpd.FLD_USER_ID = ted.FLD_ENTREPRENEUR_USER_ID  left join TBL_HUBMASTER th on "
        f"th.FLD_HubId  = tdd.FLD_MOEVING_HUB_ID  left join TBL_LOCATION_DETAILS tld on tld.FLD_LOCATION_ID = "
        f"tdd.FLD_OPERATING_CITY_ID"
        f"  where date(tdfdd.FLD_MODIFIED_DATE) "
        f"= CURRENT_DATE() and tud.FLD_LOGIN_NAME = {contact_number} ;")
    cursor.execute(query)
    result = cursor.fetchall()
    demand_status = bool(result)
    return result, demand_status


def get_user_details(contact_number):
    dbconfig = read_db_config()
    conn = MySQLConnection(**dbconfig)
    cursor = conn.cursor()
    str_ = contact_number
    contact = str_[2:]
    query = (
        f"select tud.FLD_USER_ID from TBL_USER_DETAILS tud LEFT JOIN TBL_DRIVER_DETAILS tdd on tdd.FLD_DRIVER_USER_ID = "
        f"tud.FLD_USER_ID WHERE FLD_LOGIN_NAME = {contact} and tdd.FLD_STATUS = 3 and tud.FLD_STATUS =3;")
    cursor.execute(query)
    result = cursor.fetchall()
    return bool(result)


def get_vehicle_current_coordinates(rc_number):
    current_coordinates_url = "apiURL"
    payload = {"vehicleNoList": [rc_number]}
    json_payload = json.dumps(payload)
    header = {
        'authorization': 'Bearer token',
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", current_coordinates_url, headers=header, data=json_payload)
    result = response.json()
    latitude = result['data']['vehiclesCurrentCoordinates'][0][rc_number]['latitude']
    longitude = result['data']['vehiclesCurrentCoordinates'][0][rc_number]['longitude']
    return latitude, longitude


def get_near_by_charger(lat, long):
    current_coordinates_url = "apiURL"
    header = {
        'authorization': 'Bearer token',
        'X-Latitude': str(lat), 'X-Longitude': str(long)
    }
    param = {"radius": 5}
    response = requests.request("GET", current_coordinates_url, headers=header, params=param)
    result = response.json()
    chargers_data = result['data']['stations']
    return chargers_data


def save_to_database_text(data):
    try:
        dest_addr = data['waNumber']
        src_addr = data['mobile']
        name = data['name']
        text = data['text']
        messageId = data['messageId']
        query = ("INSERT INTO realtime_status_events_gupshup (dest_addr, src_addr, text, name, messageId) "
                 "VALUES (%s, %s, %s, %s, %s)")
        cursor.execute(query, (dest_addr, src_addr, text, name, messageId,))
        print("--------------message data inserted to db------------------")
        conn.commit()
    except Exception as e:
        print("Error saving to database:", e)
    finally:
        cursor.close()


def save_to_database(data):
    dbconfig = read_db_config()
    conn = MySQLConnection(**dbconfig)
    cursor = conn.cursor()
    try:
        external_id = data['externalId']
        event_type = data['eventType']
        event_ts = data['eventTs']
        dest_addr = data['destAddr']
        src_addr = data['srcAddr']
        cause = data['cause']
        error_code = data['errorCode']
        channel = data['channel']

        query = ("INSERT INTO realtime_status_events_gupshup (external_id, event_type, event_ts, dest_addr, "
                 "src_addr, cause,error_code, channel) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
        cursor.execute(query, (external_id, event_type, event_ts, dest_addr, src_addr, cause, error_code, channel))
        conn.commit()
        print("---------------CommittedToProd-----------------")
    except Exception as e:
        print("Error saving to database:", e)
    finally:
        cursor.close()


@app.route('/RealTimeDLR/readurl', methods=['POST'])
def callback():
    try:
        data = request.json
        if data and isinstance(data, list):
            for event in data:
                save_to_database(event)
            return jsonify({'success': True}), 200
        else:
            return jsonify({'error': 'Invalid or empty data provided'}), 400
    except Exception as e:
        print("Error processing callback:", e)
        return jsonify({'error': 'Internal server error'}), 500


def open_google_map(lat, long):
    url = f"https://www.google.com/maps/search/?api=1&query={lat},{long}"
    return url


sms_service_provider_api = 'apiURL'
headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
}

msg_no_demand_found = f"As you haven't assigned any vehicle today, So can't provide required details."
header_no_demand_found = f"Thank you for connecting !"


@app.route('/RealTimeDLR/readurl/text', methods=['POST'])
def callback_chat_bot():
    data = request.json
    print(data)
    contact = data.get('mobile')  # Use .get() to safely access the 'mobile' key
    if not contact:
        return jsonify({'error': 'Mobile number not provided'}), 400
    user_details = get_user_details(contact)
    print(user_details, "Contact")
    sms_message = ''  # Get the SMS message based on 'Gurgaon' key
    sms_header = ''  # Get the SMS header based on 'Gurgaon' key
    params = {
        'send_to': contact,
        'msg_type': 'Text',
        'userid': 'chatserviceproviderID In my case this was gupshup',
        'password': 'password',
        'method': 'SendMessage',
        'v': '1.1',
        'format': 'json',
        'msg': sms_message,
        'header': sms_header,
        'isTemplate': 'true'
    }
    if not user_details and data and any(word in data.get('text', '') for word in ['hi', 'Hi', 'Hello', 'hello']):
        save_incoming_msg_lead_type(data, 'UnknownUser')
        unknown_user_msg = """I am here to assist you with predefined questions. Do you want to join MoEVing as we are unable in finding your existing details with us? Let me know how I can help you today.

1. Join us as a Driver 🚚
2. Join us as a Driver Cum Owner (DCO) 🛻
3. Join us as a Client 🧑‍💼

Reply with the number of your choice to continue."""
        unknown_user_header = """Welcome to MoEVing Assistant!"""
        params['msg'] = unknown_user_msg
        params['header'] = unknown_user_header
        response = requests.post(sms_service_provider_api, headers=headers, params=params)
        response = response.text
        parsed_response = json.loads(response)
        phone_number = parsed_response['response']['phone']
        status = parsed_response['response']['status']
        msg_id = parsed_response['response']['id']
        sent_msg = params.get('msg')
        save_outgoing_msg(phone_number, status, msg_id, sent_msg)
        return str(response), 200
    elif not user_details and data and any(word in data.get('text', '') for word in ['1', '2', '3']):
        incoming_word = next(word for word in ['1', '2', '3'] if word in data.get('text', ''))
        save_incoming_msg_lead_type(data, incoming_word)
        user_text = data.get('text')
        print(f'user_details not found and sent {user_text}', user_details)
        msg = f"""Our team will contact you within 48 working hours."""
        header = f"Thank you for showing your interest in MoEVing."  # Get the SMS header based on 'Gurgaon' key
        params['msg'] = msg
        params['header'] = header
        response = requests.post(sms_service_provider_api, headers=headers, params=params)
        response = response.text
        parsed_response = json.loads(response)
        phone_number = parsed_response['response']['phone']
        status = parsed_response['response']['status']
        msg_id = parsed_response['response']['id']
        sent_msg = params.get('msg')
        save_outgoing_msg(phone_number, status, msg_id, sent_msg)
        return str(response), 200
    elif user_details and data and any(word in data.get('text', '') for word in ['hi', 'Hi', 'Hello', 'hello']):
        print('user_details', user_details)
        sms_message = initial_msg.get('msg', '')  # Get the SMS message based on 'Gurgaon' key
        sms_header = initial_headers.get('header', '')  # Get the SMS header based on 'Gurgaon' key
        params['msg'] = sms_message
        params['header'] = sms_header
        save_incoming_msg(data)
        response = requests.post(sms_service_provider_api, headers=headers, params=params)
        response = response.text
        parsed_response = json.loads(response)
        phone_number = parsed_response['response']['phone']
        status = parsed_response['response']['status']
        msg_id = parsed_response['response']['id']
        sent_msg = params.get('msg')
        save_outgoing_msg(phone_number, status, msg_id, sent_msg)
        return str(response), 200
    elif user_details and data and any(number in data.get('text', '') for number in ['1']):
        save_incoming_msg(data)
        str_ = contact
        contact = str_[2:]
        demand_details = get_driver_demand_details(contact)
        demand_details_ = demand_details[0]
        demand_status = bool(demand_details_)
        print(demand_details)
        if demand_status:
            assigned_vehicle = demand_details_[0][0]
            checkin_time = demand_details_[0][1]
            formatted_datetime = checkin_time.strftime("%b %d, %Y %I:%M %p")
            choice_1_msg = f"""Vehicle No:-  {assigned_vehicle}
Check In Time:-  {formatted_datetime}"""
            choice_1_header = f"Vehicle Details:"  # Get the SMS header based on 'Gurgaon' key
            params['msg'] = choice_1_msg
            params['header'] = choice_1_header
            response = requests.post(sms_service_provider_api, headers=headers, params=params)
            response = response.text
            print(response)
            parsed_response = json.loads(response)
            phone_number = parsed_response['response']['phone']
            status = parsed_response['response']['status']
            msg_id = parsed_response['response']['id']
            sent_msg = params.get('msg')
            save_outgoing_msg(phone_number, status, msg_id, sent_msg)
            return str(response), 200
        else:
            params['msg'] = msg_no_demand_found
            params['header'] = header_no_demand_found
            response = requests.post(sms_service_provider_api, headers=headers, params=params)
            response = response.text
            parsed_response = json.loads(response)
            phone_number = parsed_response['response']['phone']
            status = parsed_response['response']['status']
            msg_id = parsed_response['response']['id']
            sent_msg = params.get('msg')
            save_outgoing_msg(phone_number, status, msg_id, sent_msg)
            return str(response), 200
    elif user_details and data and any(number in data.get('text', '') for number in ['2']):
        save_incoming_msg(data)
        str_ = contact
        contact = str_[2:]
        demand_details = get_driver_demand_details(contact)
        demand_details_ = demand_details[0]
        demand_status = bool(demand_details)
        if demand_status:
            client_name = demand_details_[0][2]
            reporting_location = demand_details_[0][3]
            reporting_location_address = demand_details_[0][10]
            city = demand_details_[0][9]
            reporting_lat = demand_details_[0][11]
            reporting_lon = demand_details_[0][12]
            location_link = open_google_map(reporting_lat, reporting_lon)
            choice_2_msg = f"""Client:- {client_name}
Reporting Location:- {reporting_location}, {reporting_location_address}, {city}
Please Use This Link To Get The Location:-  {location_link}"""
            choice_2_header = """Client and Reporting location:"""
            params['msg'] = choice_2_msg
            params['header'] = choice_2_header
            response = requests.post(sms_service_provider_api, headers=headers, params=params)
            response = response.text
            parsed_response = json.loads(response)
            phone_number = parsed_response['response']['phone']
            status = parsed_response['response']['status']
            msg_id = parsed_response['response']['id']
            sent_msg = params.get('msg')
            save_outgoing_msg(phone_number, status, msg_id, sent_msg)
            return str(response), 200
        else:
            params['msg'] = msg_no_demand_found
            params['header'] = header_no_demand_found
            response = requests.post(sms_service_provider_api, headers=headers, params=params)
            response = response.text
            parsed_response = json.loads(response)
            phone_number = parsed_response['response']['phone']
            status = parsed_response['response']['status']
            msg_id = parsed_response['response']['id']
            sent_msg = params.get('msg')
            save_outgoing_msg(phone_number, status, msg_id, sent_msg)
            return str(response), 200
    elif user_details and data and any(number in data.get('text', '') for number in ['3']):
        save_incoming_msg(data)
        str_ = contact
        contact = str_[2:]
        demand_details = get_driver_demand_details(contact)
        demand_details_ = demand_details[0]
        demand_status = bool(demand_details)
        if demand_status:
            assigned_vehicle = demand_details_[0][0]
            lat, long = get_vehicle_current_coordinates(assigned_vehicle)
            chargers = get_near_by_charger(lat, long)
            online_chargers = [charger for charger in chargers if charger['online']]
            # Ensure only 5 chargers are considered
            online_chargers = online_chargers[:5]
            responses = []
            count = 1
            for charger in online_chargers:
                provider = charger['name']
                address = charger['address']
                charger_lat = charger['latitude']
                charger_long = charger['longitude']
                location_link = open_google_map(charger_lat, charger_long)
                choice_3_msg = f"""{count} of {len(online_chargers)}
Location:- {provider}, {address}
Distance:- within 5 kms
Use This Link To Go To Charger's Location:- {location_link}"""
                choice_3_header = """Nearby Charging Stations:"""
                sms_message = choice_3_msg  # Get the SMS message based on 'Gurgaon' key
                sms_header = choice_3_header  # Get the SMS header based on 'Gurgaon' key
                params['msg'] = sms_message
                params['header'] = sms_header
                response = requests.post(sms_service_provider_api, headers=headers, params=params)
                response = response.text
                parsed_response = json.loads(response)
                phone_number = parsed_response['response']['phone']
                status = parsed_response['response']['status']
                msg_id = parsed_response['response']['id']
                sent_msg = params.get('msg')
                save_outgoing_msg(phone_number, status, msg_id, sent_msg)
                responses.append(response)
                time.sleep(5)
                count += 1
            return '\n'.join(responses), 200
        else:
            params['msg'] = msg_no_demand_found
            params['header'] = header_no_demand_found
            response = requests.post(sms_service_provider_api, headers=headers, params=params)
            response = response.text
            parsed_response = json.loads(response)
            phone_number = parsed_response['response']['phone']
            status = parsed_response['response']['status']
            msg_id = parsed_response['response']['id']
            sent_msg = params.get('msg')
            save_outgoing_msg(phone_number, status, msg_id, sent_msg)
            return str(response), 200
    elif user_details and data and any(number in data.get('text', '') for number in ['4']):
        save_incoming_msg(data)
        str_ = contact
        contact = str_[2:]
        demand_details = get_driver_demand_details(contact)
        demand_details_ = demand_details[0]
        demand_status = bool(demand_details)
        if demand_status:
            choice_4_msg = {'msg': "Please Use This Link To Get The Location"}
            choice_4_header = {'header': """Client and Reporting location:"""}
            sms_message = choice_4_msg.get('msg', '')  # Get the SMS message based on 'Gurgaon' key
            sms_header = choice_4_header.get('header', '')  # Get the SMS header based on 'Gurgaon' key
            params['msg'] = sms_message
            params['header'] = sms_header
            response = requests.post(sms_service_provider_api, headers=headers, params=params)
            response = response.text
            parsed_response = json.loads(response)
            phone_number = parsed_response['response']['phone']
            status = parsed_response['response']['status']
            msg_id = parsed_response['response']['id']
            sent_msg = params.get('msg')
            save_outgoing_msg(phone_number, status, msg_id, sent_msg)
            return str(response), 200
        else:
            params['msg'] = msg_no_demand_found
            params['header'] = header_no_demand_found
            response = requests.post(sms_service_provider_api, headers=headers, params=params)
            response = response.text
            parsed_response = json.loads(response)
            phone_number = parsed_response['response']['phone']
            status = parsed_response['response']['status']
            msg_id = parsed_response['response']['id']
            sent_msg = params.get('msg')
            save_outgoing_msg(phone_number, status, msg_id, sent_msg)
            return str(response), 200
    else:
        print("Into Else Statement")
        button_data = json.loads(data['button'])

        # Extract the 'text' value
        text_value = button_data['text']

        # Add the 'text' value to the data dictionary
        data['text'] = text_value
        save_incoming_msg_lead_type(data, 'UnknownUser')
        unknown_user_msg = """I am here to assist you with predefined questions. Do you want to join MoEVing as we are unable in finding your existing details with us? Let me know how I can help you today.

        1. Join us as a Driver 🚚
        2. Join us as a Driver Cum Owner (DCO) 🛻
        3. Join us as a Client 🧑‍💼

        Reply with the number of your choice to continue."""
        unknown_user_header = """Welcome to MoEVing Assistant!"""
        params['msg'] = unknown_user_msg
        params['header'] = unknown_user_header
        response = requests.post(sms_service_provider_api, headers=headers, params=params)
        response = response.text
        parsed_response = json.loads(response)
        phone_number = parsed_response['response']['phone']
        status = parsed_response['response']['status']
        msg_id = parsed_response['response']['id']
        sent_msg = params.get('msg')
        save_outgoing_msg(phone_number, status, msg_id, sent_msg)
        return str(response), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
