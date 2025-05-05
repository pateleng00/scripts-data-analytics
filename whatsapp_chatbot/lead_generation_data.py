from _datetime import datetime

from pymongo import MongoClient

# MongoDB's connection settings
client = MongoClient(
    "mongodb://username:password@host:27017/")
db = client["Fleet"]
collection = db["whatsapp_bot_message"]

current_date = datetime.today().date()
print("Current date", current_date)
# current_date = current_date - timedelta(days=1)
start_date = datetime(current_date.year, current_date.month, current_date.day, 0, 0, 0)
end_date = datetime(current_date.year, current_date.month, current_date.day, 23, 59, 59, 999000)

# Define the aggregation pipelines

# Query documents where leadTypeId is in the list
data = ([
    {
        "$match": {
            "leadTypeId": {"$in": ["1", "2", "3"]}
        }
    },
    {
        "$unwind": "$chat"
    },
    {
        "$match": {
            "chat.leadTypeId": {"$in": ["1", "2", "3"]}
        }
    },
    {
        "$project": {
            "_id": 1,
            "chat.createdDateTime": 1,
            "chat.leadTypeId": 1
        }
    }
])
result = db.connection.aggregate(data)
print(list(result))
for _data in result:
    print(_data)

if __name__ == '__main__':
    print("")
