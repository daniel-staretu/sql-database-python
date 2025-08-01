import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
from contextlib import contextmanager
import logging
from typing import Dict, List, Optional, Union, Tuple

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

    def _create_connection(self, database=None):
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
            connection = self._create_connection(database)
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

    def execute_query(self, query: str, params: Optional[Tuple] = None,
                      database: Optional[str] = None, dictionary: bool = False,
                      fetch: bool = True) -> Union[List, int]:
        with self.get_cursor(database, dictionary) as (connection, cursor):
            cursor.execute(query, params or ())

            if fetch:
                results = cursor.fetchall()
                logger.info(f"Query returned {len(results)} rows")
                return results
            else:
                connection.commit()
                affected_rows = cursor.rowcount
                logger.info(f"Query executed successfully, {affected_rows} rows affected")
                return affected_rows

    def execute_many(self, query: str, param_list: List[Tuple], database: Optional[str] = None) -> int:
        with self.get_cursor(database) as (connection, cursor):
            cursor.executemany(query, param_list)
            connection.commit()
            affected_rows = cursor.rowcount
            logger.info(f"Batch query executed successfully, {affected_rows} rows affected")
            return affected_rows

    def insert(self, table: str, data: Union[Dict, List[Dict]],
               database: Optional[str] = None, on_duplicate: Optional[str] = None) -> int:

        if isinstance(data, dict):
            data = [data]

        if not data:
            return 0

        columns = ', '.join(data[0].keys())
        placeholders = ', '.join(['%s'] * len(data[0]))

        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        if on_duplicate:
            query += f" ON DUPLICATE KEY UPDATE {on_duplicate}"

        if len(data) == 1:
            return self.execute_query(query, tuple(data[0].values()), database, fetch=False)
        else:
            param_list = [tuple(record.values()) for record in data]
            return self.execute_many(query, param_list, database)

    def select(self, table: str, columns: str = "*", where: Optional[str] = None,
               params: Optional[List] = None, order_by: Optional[str] = None,
               limit: Optional[int] = None, offset: Optional[int] = None,
               database: Optional[str] = None, dictionary: bool = True) -> List:
        query = f"SELECT {columns} FROM {table}"
        query_params = params or []

        if where:
            query += f" WHERE {where}"

        if order_by:
            query += f" ORDER BY {order_by}"

        if limit:
            query += f" LIMIT {limit}"

        if offset:
            query += f" OFFSET {offset}"

        return self.execute_query(query, tuple(query_params), database, dictionary)

    def update(self, table: str, data: Dict, where: str,
               params: Optional[List] = None, database: Optional[str] = None) -> int:
        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        query_params = list(data.values()) + (params or [])
        return self.execute_query(query, tuple(query_params), database, fetch=False)

    def delete(self, table: str, where: str, params: Optional[List] = None,
               database: Optional[str] = None, soft: bool = False) -> int:
        if soft:
            return self.update(table, {'deleted_at': 'NOW()'}, where, params, database)
        else:
            query = f"DELETE FROM {table} WHERE {where}"
            return self.execute_query(query, tuple(params or []), database, fetch=False)

    def exists(self, table: str, where: str, params: Optional[List] = None,
               database: Optional[str] = None) -> bool:
        query = f"SELECT 1 FROM {table} WHERE {where} LIMIT 1"
        result = self.execute_query(query, tuple(params or []), database)
        return len(result) > 0

    def count(self, table: str, where: Optional[str] = None,
              params: Optional[List] = None, database: Optional[str] = None) -> int:
        query = f"SELECT COUNT(*) FROM {table}"
        if where:
            query += f" WHERE {where}"
        result = self.execute_query(query, tuple(params or []), database)
        return result[0][0]

    def upsert(self, table: str, data: Dict, update_fields: Optional[List[str]] = None,
               database: Optional[str] = None) -> int:
        if update_fields:
            updates = ', '.join([f"{field} = VALUES({field})" for field in update_fields])
        else:
            updates = ', '.join([f"{k} = VALUES({k})" for k in data.keys()])

        return self.insert(table, data, database, on_duplicate=updates)

    def create_database(self, database_name: str):
        query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
        self.execute_query(query, fetch=False)
        logger.info(f"Database '{database_name}' created successfully")

    def table_exists(self, table_name: str, database: Optional[str] = None) -> bool:
        db_name = database or self.config.database
        query = """
                SELECT COUNT(*) as count
                FROM information_schema.tables
                WHERE table_schema = %s \
                  AND table_name = %s
                """
        result = self.execute_query(query, (db_name, table_name))
        return result[0][0] > 0

    def get_table_info(self, table_name: str, database: Optional[str] = None) -> List[Dict]:
        if not table_name.replace('_', '').replace('-', '').isalnum():
            raise ValueError("Invalid table name")
        query = f"DESCRIBE {table_name}"
        return self.execute_query(query, database=database, dictionary=True)

    def drop_table(self, table_name: str, database: Optional[str] = None) -> int:
        if not table_name.replace('_', '').replace('-', '').isalnum():
            raise ValueError("Invalid table name")
        query = f"DROP TABLE IF EXISTS {table_name}"
        return self.execute_query(query, database=database, fetch=False)

    def test_connection(self, database: Optional[str] = None) -> bool:
        try:
            result = self.execute_query("SELECT 1", database=database)
            return result[0][0] == 1
        except Exception as err:
            logger.error(f"Connection test failed: {err}")
            return False

    def get_last_insert_id(self, database: Optional[str] = None) -> int:
        result = self.execute_query("SELECT LAST_INSERT_ID()", database=database)
        return result[0][0]

    @contextmanager
    def transaction(self, database: Optional[str] = None):
        connection = None
        try:
            connection = self._create_connection(database)
            connection.start_transaction()
            yield connection
            connection.commit()
            logger.info("Transaction committed successfully")
        except Exception as err:
            if connection:
                connection.rollback()
                logger.error(f"Transaction rolled back due to error: {err}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()

    def batch_update(self, table: str, updates: List[Dict], key_field: str,
                     database: Optional[str] = None) -> int:
        if not updates:
            return 0

        total_affected = 0
        with self.transaction(database) as conn:
            for update_data in updates:
                key_value = update_data.pop(key_field)
                if update_data:
                    affected = self.update(table, update_data, f"{key_field} = %s", [key_value], database)
                    total_affected += affected
                update_data[key_field] = key_value

        return total_affected

    def paginate(self, table: str, page: int = 1, per_page: int = 10,
                 columns: str = "*", where: Optional[str] = None,
                 params: Optional[List] = None, order_by: Optional[str] = None,
                 database: Optional[str] = None) -> Dict:
        offset = (page - 1) * per_page

        total = self.count(table, where, params, database)

        records = self.select(
            table, columns, where, params, order_by,
            per_page, offset, database
        )

        return {
            'data': records,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'pages': (total + per_page - 1) // per_page,
                'has_next': page * per_page < total,
                'has_prev': page > 1
            }
        }