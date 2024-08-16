import json

def main():
    # Read the JSON data
    data = read_json_file('output.json')
    print(f"Read {len(data)} records from JSON file")  # Debugging print statement

    # Open the .sql file for writing
    with open('output.sql', 'w') as sql_file:
        # Write the SQL commands to create tables
        create_tables(sql_file)

        # Write the SQL commands to insert data
        insert_data(sql_file, data)

def read_json_file(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data

def create_tables(sql_file):
    sql_file.write('''
        CREATE TABLE IF NOT EXISTS records (
            id INT AUTO_INCREMENT PRIMARY KEY,
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
            track_7digitalid INT,
            shs_perf INT,
            shs_work INT
        );
    ''')

    sql_file.write('''
        CREATE TABLE IF NOT EXISTS events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            record_id INT,
            event_type VARCHAR(255),
            timestamp DATETIME,
            user_id INT,
            user_name VARCHAR(255),
            user_age VARCHAR(255),
            user_gender VARCHAR(255),
            user_main_genre VARCHAR(255),
            user_subscription_plan VARCHAR(255),
            user_platform VARCHAR(255),
            user_state VARCHAR(255),
            user_os VARCHAR(255),
            current_page VARCHAR(255),
            FOREIGN KEY(record_id) REFERENCES records(id)
        );
    ''')

def insert_data(sql_file, data):
    for record in data:
        record_data = record['record']
        print(f"Inserting record: {record_data}")  # Debugging print statement
        sql_file.write(f'''
            INSERT INTO records (track_id, title, song_id, release, artist_id, artist_mbid, artist_name, duration, artist_familiarity, artist_hotttnesss, year, track_7digitalid)
            VALUES ('{record_data['track_id']}', '{record_data['title']}', '{record_data['song_id']}', '{record_data['release']}', '{record_data['artist_id']}', '{record_data['artist_mbid']}', '{record_data['artist_name']}', {record_data['duration']}, {record_data['artist_familiarity']}, {record_data['artist_hotttnesss']}, {record_data['year']}, {record_data['track_7digitalid']});
        ''')

        record_id = 'LAST_INSERT_ID()'  # Use MySQL function to get the last inserted ID

        for event in record['events']:
            user_data = event['user']
            print(f"Inserting event: {event}")  # Debugging print statement
            sql_file.write(f'''
                INSERT INTO events (record_id, event_type, timestamp, user_id, user_name, user_age, user_gender, user_main_genre, user_subscription_plan, user_platform, user_state, user_os, current_page)
                VALUES ({record_id}, '{event['event_type']}', '{event['timestamp']}', {user_data['id']}, '{user_data['name']}', '{user_data['age']}', '{user_data['gender']}', '{user_data['main_genre']}', '{user_data['subscription_plan']}', '{user_data['platform']}', '{user_data['state']}', '{user_data['OS'][0]}', '{event.get('current_page', '')}');
            ''')

if __name__ == "__main__":
    main()