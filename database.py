import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
from contextlib import contextmanager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class DatabaseConfig:

    def __init__(self):
        self.host = os.getenv('DB_HOST')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.database = os.getenv('DB_NAME', None)

        required_vars = [self.host, self.user, self.password]
        if not all(required_vars):
            raise ValueError("Missing required database environment variables")

    def get_connection_params(self, database=None):
        params = {
            'host': self.host,
            'user': self.user,
            'password': self.password,
            'autocommit': False,
            'charset': 'utf8mb4'
        }

        if database:
            params['database'] = database
        elif self.database:
            params['database'] = self.database

        return params


class DatabaseConnection:

    def __init__(self):
        self.config = DatabaseConfig()

    def create_connection(self, database=None):
        try:
            connection_params = self.config.get_connection_params(database)
            connection = mysql.connector.connect(**connection_params)

            db_info = f"database '{database}'" if database else "server"
            logger.info(f"Successfully connected to {db_info}")
            return connection

        except Error as err:
            logger.error(f"Failed to connect to database: {err}")
            raise

    @contextmanager
    def get_cursor(self, database=None, dictionary=False):
        connection = None
        cursor = None

        try:
            connection = self.create_connection(database)
            cursor = connection.cursor(dictionary=dictionary)
            yield connection, cursor

        except Error as err:
            logger.error(f"Database operation failed: {err}")
            if connection:
                connection.rollback()
            raise

        finally:
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()
                logger.info("Database connection closed")

    def execute_query(self, query, params=None, database=None, fetch=False, dictionary=False):
        with self.get_cursor(database, dictionary) as (connection, cursor):
            cursor.execute(query, params or ())

            if fetch:
                if 'SELECT' in query.upper():
                    results = cursor.fetchall()
                    logger.info(f"Query returned {len(results)} rows")
                    return results
                else:
                    logger.warning("Fetch requested for non-SELECT query")
                    return None
            else:
                connection.commit()
                affected_rows = cursor.rowcount
                logger.info(f"Query executed successfully, {affected_rows} rows affected")
                return affected_rows

    def execute_many(self, query, param_list, database=None):
        with self.get_cursor(database) as (connection, cursor):
            cursor.executemany(query, param_list)
            connection.commit()
            affected_rows = cursor.rowcount
            logger.info(f"Batch query executed successfully, {affected_rows} rows affected")
            return affected_rows

    def create_database(self, database_name):
        query = f"CREATE DATABASE IF NOT EXISTS `{database_name}`"
        self.execute_query(query)
        logger.info(f"Database '{database_name}' created successfully")

    def test_connection(self, database=None):
        try:
            with self.get_cursor(database) as (connection, cursor):
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception as err:
            logger.error(f"Connection test failed: {err}")
            return False