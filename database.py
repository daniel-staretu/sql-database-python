import mysql.connector
from mysql.connector import Error, pooling
import os
from dotenv import load_dotenv
from contextlib import contextmanager
import logging
from typing import Dict, List, Optional, Union, Tuple

# Library best practice: don't configure logging, let the caller decide
logging.getLogger(__name__).addHandler(logging.NullHandler())
logger = logging.getLogger(__name__)

load_dotenv()


class DatabaseConfig:
    def __init__(self):
        self.host = os.getenv('DB_HOST')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.database = os.getenv('DB_NAME', None)
        self.pool_size = int(os.getenv('DB_POOL_SIZE', '5'))

        required = {'DB_HOST': self.host, 'DB_USER': self.user, 'DB_PASSWORD': self.password}
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    def get_connection_params(self, database=None) -> Dict:
        params = {
            'host': self.host,
            'user': self.user,
            'password': self.password,
            'autocommit': False,
            'charset': 'utf8mb4',
        }
        db = database or self.database
        if db:
            params['database'] = db
        return params


class DatabaseConnection:
    def __init__(self, use_pool: bool = True):
        self.config = DatabaseConfig()
        self._pool = None

        if use_pool and self.config.database:
            try:
                self._pool = pooling.MySQLConnectionPool(
                    pool_name="db_pool",
                    pool_size=self.config.pool_size,
                    **self.config.get_connection_params(),
                )
                logger.info(f"Connection pool created (size={self.config.pool_size})")
            except Error as err:
                logger.warning(f"Could not create connection pool: {err}. Using direct connections.")

    def _create_connection(self, database=None) -> mysql.connector.MySQLConnection:
        # Use pool only when no override database is specified
        if self._pool and not database:
            try:
                return self._pool.get_connection()
            except Error:
                pass  # fall through to direct connection

        try:
            connection = mysql.connector.connect(**self.config.get_connection_params(database))
            db_label = f"'{database}'" if database else f"'{self.config.database}'" if self.config.database else "server"
            logger.debug(f"Connected to {db_label}")
            return connection
        except Error as err:
            logger.error(f"Failed to connect: {err}")
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

    def execute_query(self, query: str, params: Optional[Tuple] = None,
                      database: Optional[str] = None, dictionary: bool = False,
                      fetch: bool = True, return_lastrowid: bool = False) -> Union[List, int]:
        with self.get_cursor(database, dictionary) as (connection, cursor):
            cursor.execute(query, params or ())
            if fetch:
                results = cursor.fetchall()
                logger.debug(f"Query returned {len(results)} rows")
                return results
            else:
                connection.commit()
                if return_lastrowid:
                    return cursor.lastrowid
                affected = cursor.rowcount
                logger.debug(f"Query affected {affected} rows")
                return affected

    def execute_many(self, query: str, param_list: List[Tuple], database: Optional[str] = None) -> int:
        with self.get_cursor(database) as (connection, cursor):
            cursor.executemany(query, param_list)
            connection.commit()
            affected = cursor.rowcount
            logger.debug(f"Batch query affected {affected} rows")
            return affected

    def insert(self, table: str, data: Union[Dict, List[Dict]],
               database: Optional[str] = None, on_duplicate: Optional[str] = None) -> int:
        """Insert one or many records. Returns last insert ID for single inserts, rowcount for batch."""
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
            return self.execute_query(query, tuple(data[0].values()), database, fetch=False, return_lastrowid=True)
        else:
            param_list = [tuple(record.values()) for record in data]
            return self.execute_many(query, param_list, database)

    def select(self, table: str, columns: str = "*", where: Optional[str] = None,
               params: Optional[List] = None, order_by: Optional[str] = None,
               limit: Optional[int] = None, offset: Optional[int] = None,
               database: Optional[str] = None, dictionary: bool = True,
               joins: Optional[Union[str, List[str]]] = None) -> List:
        """
        Select records from a table.

        Args:
            joins: JOIN clause(s) as a string or list of strings.
                   e.g. "JOIN orders ON users.id = orders.user_id"
                   e.g. ["JOIN orders ON ...", "LEFT JOIN products ON ..."]
        """
        query = f"SELECT {columns} FROM {table}"

        if joins:
            join_clauses = joins if isinstance(joins, list) else [joins]
            query += " " + " ".join(join_clauses)

        query_params = list(params or [])

        if where:
            query += f" WHERE {where}"
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit is not None:
            query += f" LIMIT {limit}"
        if offset is not None:
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
            # Use NOW() as a SQL expression, not a string value
            query = f"UPDATE {table} SET deleted_at = NOW() WHERE {where}"
            return self.execute_query(query, tuple(params or []), database, fetch=False)
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
        fields = update_fields if update_fields else list(data.keys())
        updates = ', '.join([f"{field} = VALUES({field})" for field in fields])
        return self.insert(table, data, database, on_duplicate=updates)

    def create_database(self, database_name: str) -> None:
        query = f"CREATE DATABASE IF NOT EXISTS `{database_name}`"
        self.execute_query(query, fetch=False)
        logger.info(f"Database '{database_name}' created or already exists")

    def create_table(self, table_name: str, columns: Dict[str, str],
                     database: Optional[str] = None, if_not_exists: bool = True) -> None:
        """
        Create a table from a column definition dict.

        Args:
            columns: dict mapping column name to its SQL definition.
                     e.g. {"id": "INT AUTO_INCREMENT PRIMARY KEY", "name": "VARCHAR(100) NOT NULL"}
        """
        col_defs = ',\n  '.join(f"{name} {definition}" for name, definition in columns.items())
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        query = f"CREATE TABLE {exists_clause}`{table_name}` (\n  {col_defs}\n)"
        self.execute_query(query, database=database, fetch=False)
        logger.info(f"Table '{table_name}' created or already exists")

    def table_exists(self, table_name: str, database: Optional[str] = None) -> bool:
        db_name = database or self.config.database
        query = """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """
        result = self.execute_query(query, (db_name, table_name))
        return result[0][0] > 0

    def get_table_info(self, table_name: str, database: Optional[str] = None) -> List[Dict]:
        if not table_name.replace('_', '').replace('-', '').isalnum():
            raise ValueError(f"Invalid table name: {table_name!r}")
        query = f"DESCRIBE `{table_name}`"
        return self.execute_query(query, database=database, dictionary=True)

    def drop_table(self, table_name: str, database: Optional[str] = None) -> None:
        if not table_name.replace('_', '').replace('-', '').isalnum():
            raise ValueError(f"Invalid table name: {table_name!r}")
        query = f"DROP TABLE IF EXISTS `{table_name}`"
        self.execute_query(query, database=database, fetch=False)
        logger.info(f"Table '{table_name}' dropped")

    def test_connection(self, database: Optional[str] = None) -> bool:
        try:
            result = self.execute_query("SELECT 1", database=database)
            return result[0][0] == 1
        except Exception as err:
            logger.error(f"Connection test failed: {err}")
            return False

    @contextmanager
    def transaction(self, database: Optional[str] = None):
        """Context manager for explicit transactions. Yields the connection object."""
        connection = None
        try:
            connection = self._create_connection(database)
            connection.start_transaction()
            yield connection
            connection.commit()
            logger.debug("Transaction committed")
        except Exception as err:
            if connection:
                connection.rollback()
                logger.error(f"Transaction rolled back: {err}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()

    def batch_update(self, table: str, updates: List[Dict], key_field: str,
                     database: Optional[str] = None) -> int:
        """
        Update multiple rows in a single transaction.

        Args:
            updates: list of dicts, each containing key_field and fields to update.
                     Input dicts are NOT mutated.
        """
        if not updates:
            return 0

        total_affected = 0
        with self.transaction(database):
            for record in updates:
                key_value = record[key_field]
                data = {k: v for k, v in record.items() if k != key_field}
                if data:
                    total_affected += self.update(table, data, f"{key_field} = %s", [key_value], database)

        return total_affected

    def paginate(self, table: str, page: int = 1, per_page: int = 10,
                 columns: str = "*", where: Optional[str] = None,
                 params: Optional[List] = None, order_by: Optional[str] = None,
                 database: Optional[str] = None,
                 joins: Optional[Union[str, List[str]]] = None) -> Dict:
        """
        Returns a page of results along with pagination metadata.

        Returns:
            {
                "data": [...],
                "pagination": {
                    "page": 1, "per_page": 10, "total": 100,
                    "pages": 10, "has_next": True, "has_prev": False
                }
            }
        """
        offset = (page - 1) * per_page
        total = self.count(table, where, params, database)
        records = self.select(table, columns, where, params, order_by, per_page, offset, database, joins=joins)

        return {
            'data': records,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'pages': (total + per_page - 1) // per_page if total else 0,
                'has_next': page * per_page < total,
                'has_prev': page > 1,
            }
        }
