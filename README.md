# sql-database-python

A Python MySQL wrapper built on `mysql-connector-python` that simplifies common database operations with a clean, consistent API.


## Features

`DatabaseConnection` handles connection management, optional connection pooling, and exposes methods for every common database task. All credentials are loaded from a `.env` file.

**CRUD**: `insert()` accepts a single record or a list of records and returns the last insert ID for single-row inserts. `select()` supports WHERE clauses, ORDER BY, LIMIT, OFFSET, and one or more JOIN clauses. `update()` and `delete()` both accept parameterized conditions. `delete()` supports soft deletion by setting a `deleted_at` timestamp instead of removing the row. `upsert()` inserts or updates on a duplicate key.

**Aggregates and utilities**: `count()` and `exists()` provide quick aggregate queries without writing raw SQL.

**Pagination**: `paginate()` returns a page of results alongside metadata including the total record count, total pages, and `has_next` / `has_prev` flags.

**Transactions**: `transaction()` is a context manager that commits on success and rolls back automatically on any exception.

**Batch operations**: `batch_update()` updates multiple rows in a single transaction without mutating the input data.

**Schema helpers**: `create_database()`, `create_table()`, `table_exists()`, `drop_table()`, and `get_table_info()` cover common schema management tasks.

## Setup

Create a `.env` file in the project root:

```
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=yourpassword
DB_NAME=your_database
DB_POOL_SIZE=5
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

```python
from database import DatabaseConnection

db = DatabaseConnection(use_pool=True)

db.create_database("mydb")
db.create_table("users", {
    "id": "INT AUTO_INCREMENT PRIMARY KEY",
    "name": "VARCHAR(100) NOT NULL",
    "email": "VARCHAR(150) NOT NULL UNIQUE",
    "deleted_at": "DATETIME DEFAULT NULL",
}, database="mydb")

user_id = db.insert("users", {"name": "Alice", "email": "alice@example.com"}, database="mydb")

users = db.select("users", order_by="id", database="mydb")

db.update("users", {"name": "Alice Smith"}, "id = %s", [user_id], database="mydb")

db.delete("users", "id = %s", [user_id], database="mydb", soft=True)

page = db.paginate("users", page=1, per_page=10, order_by="id", database="mydb")

with db.transaction(database="mydb") as conn:
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET name = 'TX Test' WHERE id = %s", (user_id,))
    cursor.close()
```

Run the full demo against a live MySQL server:

```bash
python main.py
```
