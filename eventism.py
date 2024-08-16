import mysql.connector
import json
import random
from datetime import datetime, timedelta

# Step 1: Connect to the MySQL database
def connect_to_database():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="new_password",
        database="million_songs"
    )

# Step 2: Fetch the data from the database
def fetch_data_from_database(connection):
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM songs")
    return cursor.fetchall()

# Step 3: Generate random events
def generate_random_events(num_events):
    events = []
    event_types = ["streamed song", "added to playlist", "removed from playlist", "liked song", "disliked song"]
    age_distribution = {
        "18-24 years old": 30,
        "25-34 years old": 32,
        "35-44 years old": 17,
        "45-54 years old": 13,
        "55-64 years old": 8
    }
    for _ in range(num_events):
        event = {
            "event_type": random.choice(event_types),
            "timestamp": (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),
            "user": {
                "id": random.randint(1, 1000),  # Associate each event with a user ID from 1 to 1000
                "name": f"User{random.randint(1, 1000)}",
                "state": random.choice(["California", "Texas", "New York", "Florida", "Illinois"]),
                "age": random.choice(list(age_distribution.keys()))
            },
            "platform": random.choice(["Spotify", "Apple Music", "YouTube Music"])
        }
        events.append(event)
    return events


# Step 4: Combine the data and events
def combine_data_and_events(data, events):
    combined = []
    for record in data:
        record_events = random.sample(events, random.randint(1, len(events)))
        combined.append({
            "record": record,
            "events": record_events
        })
    return combined

# Step 5: Write the combined data to a JSON file
def write_to_json_file(data, filename):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)

def main():
    connection = connect_to_database()
    data = fetch_data_from_database(connection)
    events = generate_random_events(10)  # Generate 100 random events
    combined_data = combine_data_and_events(data, events)
    write_to_json_file(combined_data, 'output.json')
    connection.close()

if __name__ == "__main__":
    main()