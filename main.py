import database as db
import queries as q

if __name__ == '__main__':
    connection = db.create_server_connection()
    query = "CREATE DATABASE test"

    db.create_database(connection, query)
    db.create_db_connection("test")