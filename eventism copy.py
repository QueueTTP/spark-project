import mysql.connector
import random
import json
from datetime import datetime, timedelta
import os
import time
from config_source import db_config  # Import the configuration
from mysql.connector import Error


def clear_console():
    if os.name == 'nt':
        _ = os.system('clear')

def connect_to_database():
    return mysql.connector.connect(**db_config)

def fetch_data_from_database(connection, limit=1000):
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM songs ORDER BY RAND() LIMIT %s", (limit,))
    return cursor.fetchall()

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

def create_folder(base_path):
    folder_count = 1
    while True:
        folder_name = f"folder_{folder_count:03d}"
        folder_path = os.path.join(base_path, folder_name)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            return folder_path
        folder_count += 1

def combine_data_and_events(data, events):
    combined = []
    for record in data:
        record_events = random.sample(events, random.randint(1, min(5, len(events))))  # Limit to max 5 events per record
        combined.append({
            "record": record,
            "events": record_events
        })
    return combined

def write_to_sql_file(data, filename):
    with open(filename, 'w') as sql_file:
        # Write CREATE TABLE statement for the combined events table
        sql_file.write("""
CREATE TABLE IF NOT EXISTS events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_type VARCHAR(50),
    timestamp DATETIME,
    user_id INT,
    user_name VARCHAR(255),
    user_age VARCHAR(50),
    user_gender VARCHAR(20),
    user_main_genre VARCHAR(50),
    user_subscription_plan VARCHAR(20),
    user_platform VARCHAR(50),
    user_state VARCHAR(50),
    user_os VARCHAR(20),
    user_current_page VARCHAR(50),
    track_id VARCHAR(255),
    title VARCHAR(255),
    song_id VARCHAR(255),
    release VARCHAR(255),
    artist_id VARCHAR(255),
    artist_mbid VARCHAR(255),
    artist_name VARCHAR(255),
    duration FLOAT,
    artist_familiarity FLOAT,
    artist_hotttnesss FLOAT,
    year INT,
    track_7digitalid INT
);

BEGIN;
""")

        for entry in data:
            record = entry["record"]
            events = entry["events"]
            
            # Escape single quotes in string fields
            escaped_record = {k: v.replace("'", "''") if isinstance(v, str) else v for k, v in record.items()}
            
            # Write SQL INSERT statements for the events, including song data
            for event in events:
                user = event["user"]
                escaped_user = {k: v.replace("'", "''") if isinstance(v, str) else v for k, v in user.items()}
                sql_file.write(f"""INSERT INTO events (
                    event_type, timestamp, user_id, user_name, user_age, user_gender, user_main_genre, 
                    user_subscription_plan, user_platform, user_state, user_os, user_current_page,
                    track_id, title, song_id, release, artist_id, artist_mbid, artist_name, duration, 
                    artist_familiarity, artist_hotttnesss, year, track_7digitalid
                ) VALUES (
                    '{event['event_type']}', '{event['timestamp']}', {escaped_user['id']}, '{escaped_user['name']}', 
                    '{escaped_user['age']}', '{escaped_user['gender']}', '{escaped_user['main_genre']}', 
                    '{escaped_user['subscription_plan']}', '{escaped_user['platform']}', '{escaped_user['state']}', 
                    '{escaped_user['OS'][0]}', '{escaped_user['current_page']}', '{escaped_record['track_id']}', 
                    '{escaped_record['title']}', '{escaped_record['song_id']}', '{escaped_record['release']}', 
                    '{escaped_record['artist_id']}', '{escaped_record['artist_mbid']}', '{escaped_record['artist_name']}', 
                    {escaped_record['duration']}, {escaped_record['artist_familiarity']}, 
                    {escaped_record['artist_hotttnesss']}, {escaped_record['year']}, {escaped_record['track_7digitalid']}
                );
""")

        sql_file.write("COMMIT;\n")

def run_simulation():
    base_path = "simulation_data"
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    
    current_folder = create_folder(base_path)
    file_count = 0
    total_iterations = 1 * 30  # 1 hours * 30 files per hour

    connection = connect_to_database()

    for i in range(total_iterations):
        clear_console()
        current_time = datetime.now()
        print(f"Iteration {i+1} - Time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        data = fetch_data_from_database(connection, 1000)
        events = generate_random_events(5000)  # Generate more events to ensure enough for combination
        
        if data:
            combined_data = combine_data_and_events(data, events)
            
            # Save to SQL file
            file_name = f"data_{file_count:03d}.sql"
            file_path = os.path.join(current_folder, file_name)
            write_to_sql_file(combined_data, file_path)
            print(f"Data saved to {file_path}")
            
            file_count += 1
            
            # Create a new folder every 30 files
            if file_count % 3 == 0:
                current_folder = create_folder(base_path)
                file_count = 0
        else:
            print("No data retrieved")
        
        # Wait for 2 minutes before the next iteration
        if i < total_iterations - 1:
            print("\nWaiting for 2 minutes...")
            time.sleep(10)

    connection.close()

if __name__ == "__main__":
    run_simulation()
