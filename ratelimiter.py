import pymongo
from redis import Redis
from datetime import timedelta, datetime

mongo_client = pymongo.MongoClient('localhost', 27017)
redis_client = Redis(host='localhost', port=6379, db=0)

db = mongo_client["rate-limiter"]
collection = db["blocked-requests"]

def blocked_request(r: Redis, username: str, limit: int, period: timedelta):
    if r.setnx(username, limit):
        r.expire(username, int(period.total_seconds()))
    
    value = r.get(username)

    if value and int(value) > 0:
        r.decrby(username, 1)
        return False
    return True

def push_in_mongo(username: str):
    query = {
        "username": username
    }
    now = datetime.now() # current date and time

    # Insert
    if collection.count_documents(query)==0:
        details = {
            "username": username,
            "count": 1,
            "time": [now.strftime("%m/%d/%Y, %H:%M:%S")] 
        }
        collection.insert_one(details)

    # Update
    else:
        curr_obj = collection.find({"username": username})[0]

        #Get the list of times at which user was blocked
        time_list = curr_obj['time']

        #Append the new time
        time_list.append(now.strftime("%m/%d/%Y, %H:%M:%S"))

        newvalues = {
            "$set": {
                "count": curr_obj['count'] + 1,
                "time": time_list
            }
        }
        collection.update_one({"username": username}, newvalues)

# Test Request for username aditya: 20 requests will be allowed and 5 will be blocked
for i in range(25):
    if blocked_request(redis_client, 'aditya', 20, timedelta(seconds=60)):
        print("Pushed into Mongo")
        push_in_mongo('aditya')
    else:
        print("Allowed")
        

