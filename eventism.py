import mysql.connector
import random
import json
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
    event_types = {"streamed song": 65,
                   "added to playlist": 10,
                   "removed from playlist":5,
                   "liked song": 15,
                   "disliked song": 5
                   }
    age_distribution = {
        "18-24 years old": 30,
        "25-34 years old": 32,
        "35-44 years old": 17,
        "45-54 years old": 13,
        "55-64 years old": 8
    }
    gender_distribution = {
    "Male": 55,
    "Female": 40,
    "Non-binary": 5
    }
    main_genre_distribution = {
    "Hip-Hop/R&B": 26.6,
    "Rock": 16.2,
    "Pop": 12.6,
    "Country": 11.6,
    "Latin": 5.8,
    "Electronic/Dance": 4.9,
    "Classical": 3.1,
    "Jazz": 2.3,
    "Indie/Alternative": 2.1,
    "Other": 14.8  # Includes various other genres
    }
    paid_user_probability = 0.578
    platform_distribution = {
    "Spotify": 36,
    "Apple Music": 30.7,
    "Amazon Music": 23.8,
    "YouTube Music": 6.8,
    "Pandora Premium": 1.9,
    "Tidal": 0.5,
    "SoundCloud": 0.3
    }
    state_distribution = {
    "Alabama": 1,
    "Alaska": 1,
    "Arizona": 2,
    "Arkansas": 1,
    "California": 14,
    "Colorado": 2,
    "Connecticut": 1,
    "Delaware": 1,
    "Florida": 7,
    "Georgia": 4,
    "Hawaii": 1,
    "Idaho": 1,
    "Illinois": 5,
    "Indiana": 2,
    "Iowa": 1,
    "Kansas": 1,
    "Kentucky": 1,
    "Louisiana": 1,
    "Maine": 1,
    "Maryland": 2,
    "Massachusetts": 2,
    "Michigan": 3,
    "Minnesota": 2,
    "Mississippi": 1,
    "Missouri": 2,
    "Montana": 1,
    "Nebraska": 1,
    "Nevada": 1,
    "New Hampshire": 1,
    "New Jersey": 3,
    "New Mexico": 1,
    "New York": 8,
    "North Carolina": 3,
    "North Dakota": 1,
    "Ohio": 4,
    "Oklahoma": 1,
    "Oregon": 1,
    "Pennsylvania": 5,
    "Rhode Island": 1,
    "South Carolina": 1,
    "South Dakota": 1,
    "Tennessee": 2,
    "Texas": 10,
    "Utah": 1,
    "Vermont": 1,
    "Virginia": 2,
    "Washington": 2,
    "Washington D.C.": 1,
    "West Virginia": 1,
    "Wisconsin": 2,
    "Wyoming": 1
    }
    system_distribution = {
    "Android": 28.28,
    "iOS": 70.92,
    "Windows": 0.41,
    "macOS": 0.20,
    "Other": 0.19
    }
    page_distribution = {
    "Home/Discover Page": 35,
    "Now Playing Page": 25,
    "Library/My Music Page": 15,
    "Search Page": 10,
    "Playlist Page": 5,
    "Artist Page": 3,
    "Album Page": 2,
    "Trending/Charts Page": 2,
    "Settings Page": 1,
    "Profile Page": 1,
    "Genre/Category Page": 1
    }
    for _ in range(num_events):
        event = {
            "event_type": random.choices(list(event_types.keys()), weights=event_types.values())[0],
            "timestamp": (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),
            "user": {
                "id": random.randint(1, 1000),  # Associate each event with a user ID from 1 to 1000
                "name": f"User{random.randint(1, 1000)}",
                "age": random.choices(list(age_distribution.keys()), weights=age_distribution.values())[0],
                "gender": random.choices(list(gender_distribution.keys()), weights=gender_distribution.values())[0],
                "main_genre": random.choices(list(main_genre_distribution.keys()), weights=main_genre_distribution.values())[0],
                "subscription_plan": random.choices(['Paid', 'UnPaid'], weights=[paid_user_probability, 1 - paid_user_probability])[0],
                "platform": random.choices(list(platform_distribution.keys()), weights=platform_distribution.values())[0],
                "state": random.choices(list(state_distribution.keys()), weights=state_distribution.values())[0],
                "OS": random.choices(list(system_distribution.keys()), weights=system_distribution.values()),
                "current_page": random.choices(list(page_distribution.keys()), weights=page_distribution.values())[0]
            },
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

# Step 5: Write the combined data to a SQLite database
def write_to_json_file(data, filename):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)




def main():
    connection = connect_to_database()
    data = fetch_data_from_database(connection)
    events = generate_random_events(10)  # Generate 10 random events
    combined_data = combine_data_and_events(data, events)
    write_to_json_file(combined_data, 'output.json')
    connection.close()

if __name__ == "__main__":
    main()