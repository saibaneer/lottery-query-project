import requests
import time
import os
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ServerSelectionTimeoutError


# Attempt to connect to the MongoDB server
try:
    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
    db = client['lottery_db']
    collection = db['winners']

    # Ensure unique index on the 'state' field to have distinct winners per state
    collection.create_index("state", unique=True)

except ServerSelectionTimeoutError:
    print("Failed to connect to MongoDB. Please ensure MongoDB is running.")
    exit(1) 


api_link = "https://random-data-api.com/api/users/random_user?size=5"

def log_winners():
    """Print winners in this collection"""
    print("Current Winners Table:")
    for winner in collection.find():
        print(winner)

def get_random_users():
    """Query the API to get random users."""
    response = requests.get(api_link)
    return response.json()

def insert_or_update_winner_collection(user):
    """Insert or update a winner in the MongoDB collection."""
    user_id = user['id']
    email = user['email']
    state = user['address']['state']

    filter_query = {"state": state} 
    update_operation = {
        "$set": {
            "id": user_id,
            "email": email,
            "state": state
        }
    }

    result = collection.update_one(filter_query, update_operation, upsert=True)

    if result.matched_count > 0:
        print(f'Replaced winner from {state}')
    else:
        print(f'Added new winner from {state}')


def main():
    """Main loop to query API and update winners."""
    while True:
        users = get_random_users()
        for user in users:
            insert_or_update_winner_collection(user)

        log_winners()

        count = collection.count_documents({})
        if count >= 25:
            print("All 25 winners found!")
            break
        time.sleep(10)
        log_winners()



if __name__ == "__main__":
    main()

    # Close the MongoDB connection
    client.close()