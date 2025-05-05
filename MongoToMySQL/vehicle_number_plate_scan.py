from _datetime import datetime
from datetime import timedelta

import pandas as pd
from json import dumps

from pymongo import MongoClient
from httplib2 import Http
from tabulate import tabulate

# MongoDB's connection settings
client = MongoClient(
    "mongodb://user:password@host:27017/")
db = client["Fleet"]
collection = db["rc_validation_stats"]

print("Database connection", collection)
# Get current start and end dates
current_date = datetime.today().date()
current_date = current_date - timedelta(days=1)
print("Current date", current_date)
start_date = datetime(current_date.year, current_date.month, current_date.day, 0, 0, 0)
# print(start_date)
end_date = datetime(current_date.year, current_date.month, current_date.day, 23, 59, 59, 999000)
# print(end_date)
# Define the aggregation pipelines
pipeline_total = [
    {
        '$match': {
            'scanTime': {
                '$gte': start_date,  # Beginning of today
                '$lte': end_date  # End of today
            }
        }
    },
    {
        '$group': {
            '_id': None,
            'total_scan': {'$sum': 1}
        }
    }
]

pipeline_with_identified_vehicle_no = [
    {
        '$match': {
            'scanTime': {
                '$gte': start_date,  # Beginning of today
                '$lte': end_date  # End of today
            },
            'identifiedVehicleNo': {
                '$exists': True,
                '$nin': ["error", "Error"]
            }
        }
    },
    {
        '$group': {
            '_id': None,
            'total_identified_vehicle': {'$sum': 1}
        }
    }
]

matched_vehicle_no_with_database = [
    {
        '$match': {
            'scanTime': {
                '$gte': start_date,  # Beginning of today
                '$lte': end_date  # End of today
            },
            'identifiedVehicleNo': {'$exists': True},
            'isVehicleValidated': True,
            'uri': {'$exists': True}
        }
    },
    {
        '$match': {
            'isVehicleValidated': True  # or False depending on your requirement
        }
    },
    {
        '$group': {
            '_id': None,
            'total_matched_vehicle': {'$sum': 1}
        }
    }
]

unmatched_vehicle_no = [
    {
        '$match': {
            'scanTime': {
                '$gte': start_date,  # Beginning of today
                '$lte': end_date  # End of today
            },
            'identifiedVehicleNo': {
                '$exists': True,
                '$nin': ["error", "Error"]
            },
            'isVehicleValidated': False,
        }
    },
    {
        '$group': {
            '_id': None,
            'total_unmatched_vehicle': {'$sum': 1}
        }
    }
]
unidentified_vehicle = [
    {
        '$match': {
            'scanTime': {
                '$gte': start_date,  # Beginning of today
                '$lte': end_date  # End of today
            },
            'identifiedVehicleNo': {
                '$exists': True,
                '$in': ["error", "Error"]
            }
        }
    },
    {
        '$group': {
            '_id': None,
            'total_unidentified_vehicle': {'$sum': 1}
        }
    }
]
# Execute the aggregation queries
total_scanned = collection.aggregate(pipeline_total)
total_identified_vehicle_no = collection.aggregate(pipeline_with_identified_vehicle_no)
total_matched_vehicle = collection.aggregate(matched_vehicle_no_with_database)
total_unmatched_vehicle = collection.aggregate(unmatched_vehicle_no)
total_unidentified_vehicle = collection.aggregate(unidentified_vehicle)


# Extract the results
results = {
    'total': list(total_scanned),
    'IdentifiedVehicle': list(total_identified_vehicle_no),
    'MatchedWithDB': list(total_matched_vehicle),
    'UnmatchedWithDB': list(total_unmatched_vehicle),
    'unIdentifiedVehicle': list(total_unidentified_vehicle)

}
total_scan = results['total'][0]
total_identified_vehicle = results['IdentifiedVehicle'][0]
total_Matched_with_db = results['MatchedWithDB'][0]
total_unmatched = results['UnmatchedWithDB'][0]
total_unidentyfied = results['unIdentifiedVehicle'][0]

successful_percentage = round(
    (total_identified_vehicle.get('total_identified_vehicle') / total_scan.get('total_scan')) * 100)
matched_percentage = round((total_Matched_with_db.get('total_matched_vehicle') / total_scan.get('total_scan')) * 100)
unmatched_percentage = round((total_unmatched.get('total_unmatched_vehicle') / total_scan.get('total_scan')) * 100)
failed_percentage = round((total_unidentyfied.get('total_unidentified_vehicle') / total_scan.get('total_scan')) * 100)

message_to_webhook = {
    "Total Scan Attempted ": total_scan.get('total_scan'),
    "Total Successfully Scanned ": f"{total_identified_vehicle.get('total_identified_vehicle')} ({successful_percentage} %)",
    "Total Matched Vehicle With DB ": f"{total_Matched_with_db.get('total_matched_vehicle')} ({matched_percentage} %)",
    "Total Unmatched Vehicle With DB ": f"{total_unmatched.get('total_unmatched_vehicle')} ({unmatched_percentage} %)",
    "Total Failed Scan": f"{total_unidentyfied.get('total_unidentified_vehicle')} ({failed_percentage} %)"

}


# message_to_webhook = list(message_to_webhook)
# print(message_to_webhook)


def post_to_webhook(bot_msg):
    # Wrap each value in a list
    bot_msg_lists = {key: [value] for key, value in bot_msg.items()}

    # Convert to DataFrame
    records = pd.DataFrame(bot_msg_lists).to_dict(orient="list")

    # Convert the DataFrame dictionary to a list of lists
    table_data = [[k, *v] for k, v in records.items()]

    # Create the tabular string
    tabular_string = tabulate(table_data, headers=["Scanned Vehicle Parameter", " Values ", *bot_msg.keys()],
                              tablefmt="github")

    # Construct bot_message
    bot_message = {
        'text': f"```\n{tabular_string}\n```"
    }

    print(bot_message['text'].replace('```', ''))
    message_headers = {'Content-Type': 'application/json; charset=UTF-8'}

    # Test
    # url = ('https://chat.googleapis.com/v1/spaces/AAAAQqFHSzs/messages?key=-WEfRq3CPzqKqqsHI'
    #        '&token=ObHS4RW')

    # Prod
    url = ('https://chat.googleapis.com/v1/spaces/AAAAc5Yb5K4/messages?key=-WEfRq3CPzqKqqsHI'
           '&token=cDQjg_fqvYj')

    http_obj = Http()
    response = http_obj.request(
        uri=url,
        method='POST',
        headers=message_headers,
        body=dumps(bot_message)
    )


if __name__ == '__main__':
    post_to_webhook(message_to_webhook)
