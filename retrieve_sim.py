import mysql.connector
import pandas as pd
import time
import os
from config import db_config  # Import the configuration

def clear_console():
    
    if os.name == 'nt':
        _ = os.system('clear')

# Function to get random data from MySQL
def get_random_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT `track_id`, `title`, `song_id`, `release`, `artist_id`, `artist_mbid`, 
               `artist_name`, `duration`, `artist_familiarity`, `artist_hotttnesss`, 
               `year`, `track_7digitalid`
        FROM songs
        ORDER BY RAND()
        LIMIT 5
        """
        cursor.execute(query)
        results = cursor.fetchall()
        
        df = pd.DataFrame(results)
        return df
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return pd.DataFrame()
    
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Function to simulate a listen event
def simulate_listen_event(track):
    print(f"Simulated listen event for track: {track['title']} by {track['artist_name']}")

# Main simulation loop
def run_simulation():
    for i in range(5):  # 5 iterations = 10 minutes
        clear_console()
        print(f"Iteration {i+1} - Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        df = get_random_data()
        if not df.empty:
            print(df)
            # Simulate a listen event for each track
            for _, track in df.iterrows():
                simulate_listen_event(track)
        else:
            print("No data retrieved")
        
        # Wait for 2 minutes before the next iteration
        if i < 4:  # Don't wait after the last iteration
            print("\nWaiting for 2 minutes...")
            time.sleep(120)

# Run the simulation
if __name__ == "__main__":
    run_simulation()