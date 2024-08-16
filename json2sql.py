import sqlite3
import json

# Step 1: Read the JSON file
def read_json_file(filename):
    with open(filename, 'r') as json_file:
        return json.load(json_file)

# Step 2: Create the SQLite database and tables
def create_tables(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_id TEXT,
            title TEXT,
            song_id TEXT,
            release TEXT,
            artist_id TEXT,
            artist_mbid TEXT,
            artist_name TEXT,
            duration REAL,
            artist_familiarity REAL,
            artist_hotttnesss REAL,
            year INTEGER,
            track_7digitalid INTEGER
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id INTEGER,
            event_type TEXT,
            timestamp TEXT,
            user_id INTEGER,
            user_name TEXT,
            user_age TEXT,
            user_gender TEXT,
            user_main_genre TEXT,
            user_subscription_plan TEXT,
            user_platform TEXT,
            user_state TEXT,
            user_os TEXT,
            FOREIGN KEY(record_id) REFERENCES records(id)
        )
    ''')

# Step 3: Insert the data into the tables
def insert_data(cursor, data):
    for record in data:
        record_data = record['record']
        print(f"Inserting record: {record_data}")  # Debugging print statement
        cursor.execute('''
            INSERT INTO records (track_id, title, song_id, release, artist_id, artist_mbid, artist_name, duration, artist_familiarity, artist_hotttnesss, year, track_7digitalid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (record_data['track_id'], record_data['title'], record_data['song_id'], record_data['release'], record_data['artist_id'], record_data['artist_mbid'], record_data['artist_name'], record_data['duration'], record_data['artist_familiarity'], record_data['artist_hotttnesss'], record_data['year'], record_data['track_7digitalid']))
        
        record_id = cursor.lastrowid
        
        for event in record['events']:
            user_data = event['user']
            print(f"Inserting event: {event}")  # Debugging print statement
            cursor.execute('''
                INSERT INTO events (record_id, event_type, timestamp, user_id, user_name, user_age, user_gender, user_main_genre, user_subscription_plan, user_platform, user_state, user_os)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (record_id, event['event_type'], event['timestamp'], user_data['id'], user_data['name'], user_data['age'], user_data['gender'], user_data['main_genre'], user_data['subscription_plan'], user_data['platform'], user_data['state'], user_data['OS'][0]))  # Use user_data['OS'][0] to get the first element of the list

def main():
    # Read the JSON data
    data = read_json_file('output.json')
    print(f"Read {len(data)} records from JSON file")  # Debugging print statement
    
    # Connect to the SQLite database
    conn = sqlite3.connect('million_events.db')
    cursor = conn.cursor()
    
    # Create the tables
    create_tables(cursor)
    
    # Insert the data
    insert_data(cursor, data)
    
    # Commit the changes and close the connection
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()