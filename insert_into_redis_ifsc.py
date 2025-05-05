import redis
import json


ifsc_detail_dict = {
    "MICR": str(),
    "BRANCH": str(),
    "ADDRESS": str(),
    "STATE": str(),
    "CONTACT": str(),
    "UPI": str(),
    "RTGS": str(),
    "CITY": str(),
    "CENTRE": str(),
    "DISTRICT": str(),
    "NEFT": str(),
    "IMPS": str(),
    "SWIFT": str(),
    "BANK": str(),
    "IFSC": str()
}


# Function to connect to Redis
def connect_to_redis(host, port, password):
    return redis.StrictRedis(
        host=host,
        port=port,
        password=password,
        decode_responses=True
    )


# Function to push a dictionary to Redis
def push_dict_to_redis(redis_conn, redis_key, data_dict):
    # Convert the dictionary to a JSON string
    json_data = json.dumps(data_dict)
    # Set the JSON string in Redis
    redis_conn.set(redis_key, json_data)


# Replace these with your Redis configuration details
aws_redis_host = 'your_redis_host'
aws_redis_port = 6379  # Default Redis port
aws_redis_password = 'your_redis_password'

# Example dictionary to push to Redis
example_dict = {
    'key1': 'value1',
    'key2': 'value2',
    'key3': 'value3'
}

# Redis key under which the dictionary will be stored
redis_key = 'example_dict_key'

# Connect to Redis
redis_conn = connect_to_redis(aws_redis_host, aws_redis_port, aws_redis_password)

# Push the dictionary to Redis
push_dict_to_redis(redis_conn, redis_key, example_dict)

print(f"Dictionary pushed to Redis under key: {redis_key}")
 # KA05AP0213