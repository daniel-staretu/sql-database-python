import mysql.connector
from mysql.connector import Error
import os
import dotenv

dotenv.load_dotenv()

def create_server_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
        )
        print("Server connection established.")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def create_db_connection(db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=db_name
        )
        print(f"Connection established with {db_name} database.")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully.")
    except Error as err:
        print(f"Error: '{err}'")