import mysql.connector
import random
from datetime import datetime, timedelta
import time
import os
import signal
from mysql.connector import Error
from config_source import db_config
from config_destination import db_config_destination


def get_weighted_time_of_day():
    time_of_day_weights = {
        "00:00-04:00": 2,    # Early morning, less likely
        "04:00-06:00": 5,    # Early morning
        "06:00-09:00": 15,   # Morning commute
        "09:00-12:00": 25,   # Morning work hours
        "12:00-15:00": 30,   # Afternoon work hours
        "15:00-18:00": 35,   # Evening commute
        "18:00-21:00": 50,   # Evening leisure time, most likely
        "21:00-23:59": 20    # Late evening
    }

    chosen_range = random.choices(list(time_of_day_weights.keys()), weights=time_of_day_weights.values())[0]
    start, end = chosen_range.split('-')
    
    start_time = datetime.strptime(start, "%H:%M")
    end_time = datetime.strptime(end, "%H:%M") if end != "23:59" else datetime.strptime("23:59", "%H:%M")
    
    total_minutes = int((end_time - start_time).total_seconds() // 60)
    random_minutes = random.randint(0, total_minutes)
    random_time = (start_time + timedelta(minutes=random_minutes)).time()
    
    return random_time

def generate_random_timestamp():
    random_date = datetime.now() - timedelta(days=random.randint(0, 365))
    random_time = get_weighted_time_of_day()
    return datetime.combine(random_date.date(), random_time)

# Global flag to indicate if the program should exit
should_exit = False

def signal_handler(signum, frame):
    global should_exit
    print("\nReceived exit signal. Finishing current iteration and exiting...")
    should_exit = True

# Set up the signal handler
signal.signal(signal.SIGINT, signal_handler)

def clear_console():
    if os.name == 'nt':
        _ = os.system('clear')

def connect_to_database(db_config):
    return mysql.connector.connect(**db_config)

def fetch_data_from_database(connection, limit=1000):
    cursor = connection.cursor(dictionary=True)
    cursor.execute("""
    SELECT *, `release` AS `release_date` 
    FROM songs 
    ORDER BY RAND() 
    LIMIT %s
    """, (limit,))
    return cursor.fetchall()

# Option 2: Ensuring that some users listen to multiple songs
user_id_pool = [random.randint(1, 1000) for _ in range(100)]  # Create a pool of 100 user IDs

def generate_random_events(num_events):
    events = []

    # Event types and distributions
    event_types = {
        "streamed song": 65, 
        "added to playlist": 10, 
        "removed from playlist": 5, 
        "liked song": 15, 
        "disliked song": 5
    }

    # Updated age distribution to reflect fewer older users
    age_distribution = {
        "18-24 years old": 50,   # Increase weight for younger users
        "25-34 years old": 35,
        "35-44 years old": 10,
        "45-54 years old": 4,
        "55-64 years old": 1     # Decrease weight for older users
    }
    gender_distribution = {"Male": 55, "Female": 40, "Non-binary": 5}
    
    # Genre popularity trends over the years (simplified)
    genre_trends = {
        "2000": {
            "pop": 31, "rock": 34.8, "R&B": 17.5, "hip hop": 10, "country": 13.8,
            "electronic": 3.0, "latin": 2.9, "jazz": 2.3, "classical": 2.8,
            "alternative": 17.4, "metal": 10.2, "reggae": 1.5, "blues": 1.2
        },
        "2010": {
            "pop": 33, "rock": 30, "R&B": 18, "hip hop": 12, "country": 14,
            "electronic": 4.5, "latin": 3.5, "jazz": 2.0, "classical": 2.5,
            "alternative": 18, "metal": 9, "reggae": 1.8, "blues": 1.0, "dance": 5
        },
        "2020": {
            "pop": 31, "rock": 16.2, "R&B": 17.5, "hip hop": 26.6, "country": 11.6,
            "electronic": 4.9, "latin": 5.8, "jazz": 2.3, "classical": 3.1,
            "alternative": 2.1, "trap": 8, "reggaeton": 4, "K-pop": 3, "indie": 2.5
        }
    }

    # State distribution
    state_distribution = {
        "Alabama": 1, "Alaska": 1, "Arizona": 2, "Arkansas": 1, "California": 14,
        "Colorado": 2, "Connecticut": 1, "Delaware": 1, "Florida": 7, "Georgia": 4,
        "Hawaii": 1, "Idaho": 1, "Illinois": 5, "Indiana": 2, "Iowa": 1, "Kansas": 1,
        "Kentucky": 1, "Louisiana": 1, "Maine": 1, "Maryland": 2, "Massachusetts": 2,
        "Michigan": 3, "Minnesota": 2, "Mississippi": 1, "Missouri": 2, "Montana": 1,
        "Nebraska": 1, "Nevada": 1, "New Hampshire": 1, "New Jersey": 3, "New Mexico": 1,
        "New York": 8, "North Carolina": 3, "North Dakota": 1, "Ohio": 4, "Oklahoma": 1,
        "Oregon": 1, "Pennsylvania": 5, "Rhode Island": 1, "South Carolina": 1, "South Dakota": 1,
        "Tennessee": 2, "Texas": 10, "Utah": 1, "Vermont": 1, "Virginia": 2, "Washington": 2,
        "Washington D.C.": 1, "West Virginia": 1, "Wisconsin": 2, "Wyoming": 1
    }

    # Generate random events based on genre popularity trends
    for _ in range(num_events):
        current_year = random.choice(list(genre_trends.keys()))
        genre_distribution = genre_trends[current_year]
        
        user_id = random.choice(user_id_pool)

        event = {
            "event_type": random.choices(list(event_types.keys()), weights=event_types.values())[0],
            "timestamp": generate_random_timestamp().isoformat(),
            "user": {
                "id": user_id,
                "name": f"User{user_id}",
                "age": random.choices(list(age_distribution.keys()), weights=age_distribution.values())[0],
                "gender": random.choices(list(gender_distribution.keys()), weights=gender_distribution.values())[0],
                "main_genre": random.choices(list(genre_distribution.keys()), weights=genre_distribution.values())[0],
                "subscription_plan": random.choices(['Paid', 'UnPaid'], weights=[0.578, 0.422])[0],
                "platform": random.choices(["Spotify", "Apple Music", "Amazon Music", "YouTube Music", "Pandora Premium", "Tidal", "SoundCloud"], weights=[36, 30.7, 23.8, 6.8, 1.9, 0.5, 0.3])[0],
                "state": random.choices(list(state_distribution.keys()), weights=state_distribution.values())[0],
                "OS": random.choices(["Android", "iOS", "Windows", "macOS", "Other"], weights=[28.28, 70.92, 0.41, 0.2, 0.19])[0],
                "current_page": random.choices(["Home/Discover Page", "Now Playing Page", "Library/My Music Page", "Search Page", "Playlist Page", "Artist Page", "Album Page", "Trending/Charts Page", "Settings Page", "Profile Page", "Genre/Category Page"], weights=[35, 25, 15, 10, 5, 3, 2, 2, 1, 1, 1])[0],
            },
        }
        events.append(event)
    return events

def combine_data_and_events(data, events):
    combined = []
    for record in data:
        record_events = random.sample(events, random.randint(1, min(5, len(events))))
        combined.append({
            "record": record,
            "events": record_events
        })
    return combined

def create_connection(db_config):
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

def write_to_event_sim_db(data, connection):
    try:
        cursor = connection.cursor()

        # Create the events table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS `events` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `event_type` VARCHAR(50),
            `timestamp` DATETIME,
            `user_id` INT,
            `user_name` VARCHAR(255),
            `user_age` VARCHAR(50),
            `user_gender` VARCHAR(20),
            `user_main_genre` VARCHAR(50),
            `user_subscription_plan` VARCHAR(20),
            `user_platform` VARCHAR(50),
            `user_state` VARCHAR(50),
            `user_os` VARCHAR(20),
            `user_current_page` VARCHAR(50),
            `track_id` VARCHAR(255),
            `title` VARCHAR(255),
            `song_id` VARCHAR(255),
            `release` VARCHAR(255),
            `artist_id` VARCHAR(255),
            `artist_mbid` VARCHAR(255),
            `artist_name` VARCHAR(255),
            `duration` FLOAT,
            `artist_familiarity` FLOAT,
            `artist_hotttnesss` FLOAT,
            `year` INT,
            `track_7digitalid` INT
        )
        """
        cursor.execute(create_table_query)
        connection.commit()

        # Prepare the INSERT statement
        insert_query = """
        INSERT INTO `events` (
            `event_type`, `timestamp`, `user_id`, `user_name`, `user_age`, `user_gender`, `user_main_genre`, 
            `user_subscription_plan`, `user_platform`, `user_state`, `user_os`, `user_current_page`,
            `track_id`, `title`, `song_id`, `release`, `artist_id`, `artist_mbid`, `artist_name`, 
            `duration`, `artist_familiarity`, `artist_hotttnesss`, `year`, `track_7digitalid`
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        for entry in data:
            record = entry["record"]
            events = entry["events"]
            
            for event in events:
                user = event["user"]
                insert_data = (
                    event['event_type'], 
                    event['timestamp'],  # This is now the weighted random timestamp
                    user['id'], 
                    user['name'], 
                    user['age'], 
                    user['gender'], 
                    user['main_genre'], 
                    user['subscription_plan'], 
                    user['platform'], 
                    user['state'], 
                    user['OS'], 
                    user['current_page'],
                    record['track_id'], 
                    record['title'], 
                    record['song_id'], 
                    record['release'], 
                    record['artist_id'], 
                    record['artist_mbid'], 
                    record['artist_name'], 
                    record['duration'], 
                    record['artist_familiarity'], 
                    record['artist_hotttnesss'], 
                    record['year'], 
                    record['track_7digitalid']
                )
                cursor.execute(insert_query, insert_data)

        connection.commit()
        print(f"Successfully inserted {cursor.rowcount} rows into the events table.")

    except mysql.connector.Error as e:
        print(f"Error: {e}")
        connection.rollback()
        print("Transaction rolled back due to error.")

    finally:
        cursor.close()

def run_simulation(db_config, db_config_destination):
    global should_exit
    
    connection_source = create_connection(db_config)
    connection_destination = create_connection(db_config_destination)

    if not connection_source or not connection_destination:
        print("Failed to establish database connections. Exiting.")
        return

    total_iterations = 1 * 30  # 30 hours * 30 files per hour

    try:
        for i in range(total_iterations):
            if should_exit:
                print("Exiting simulation...")
                break

            clear_console()
            current_time = datetime.now()
            print(f"Iteration {i+1} - Time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            data = fetch_data_from_database(connection_source, 1000)
            events = generate_random_events(5000)
            
            if data:
                combined_data = combine_data_and_events(data, events)
                write_to_event_sim_db(combined_data, connection_destination)
            else:
                print("No data retrieved")
            
            if i < total_iterations - 1 and not should_exit:
                print("\nWaiting for 2 minutes... (Press Ctrl+C to exit)")
                for _ in range(10):
                    if should_exit:
                        break
                    time.sleep(1)
    finally:
        if connection_source:
            connection_source.close()
        if connection_destination:
            connection_destination.close()
        print("Simulation ended. Database connections closed.")

if __name__ == "__main__":
    from config_source import db_config
    from config_destination import db_config_destination

    run_simulation(db_config, db_config_destination)
