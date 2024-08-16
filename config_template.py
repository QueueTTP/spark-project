import mysql.connector

db_config = mysql.connector.connect(
                host="localhost",
                user="root",
                password="new_password",
                database="million_songs"
            )       