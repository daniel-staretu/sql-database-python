"""
Demo: exercises the main features of DatabaseConnection against a live MySQL server.
Requires a .env file with DB_HOST, DB_USER, DB_PASSWORD (DB_NAME is optional here).
"""
import logging
from database import DatabaseConnection

# Configure logging for the demo (do this in your app, not in the library)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

TEST_DB = 'demo_db'
TEST_TABLE = 'users'


def run_demo():
    db = DatabaseConnection(use_pool=False)

    print("\n--- Connection test ---")
    ok = db.test_connection()
    print(f"Connected: {ok}")

    print("\n--- Create database and table ---")
    db.create_database(TEST_DB)
    db.create_table(
        TEST_TABLE,
        columns={
            'id': 'INT AUTO_INCREMENT PRIMARY KEY',
            'name': 'VARCHAR(100) NOT NULL',
            'email': 'VARCHAR(150) NOT NULL UNIQUE',
            'deleted_at': 'DATETIME DEFAULT NULL',
        },
        database=TEST_DB,
    )

    print("\n--- Insert single record (returns last insert ID) ---")
    user_id = db.insert(TEST_TABLE, {'name': 'Alice', 'email': 'alice@example.com'}, database=TEST_DB)
    print(f"Inserted user ID: {user_id}")

    print("\n--- Insert batch ---")
    db.insert(TEST_TABLE, [
        {'name': 'Bob', 'email': 'bob@example.com'},
        {'name': 'Carol', 'email': 'carol@example.com'},
        {'name': 'Dave', 'email': 'dave@example.com'},
    ], database=TEST_DB)

    print("\n--- Select all ---")
    users = db.select(TEST_TABLE, database=TEST_DB, order_by='id')
    for u in users:
        print(u)

    print("\n--- Count ---")
    print(f"Total users: {db.count(TEST_TABLE, database=TEST_DB)}")

    print("\n--- Exists ---")
    print(f"alice exists: {db.exists(TEST_TABLE, 'email = %s', ['alice@example.com'], database=TEST_DB)}")
    print(f"nobody exists: {db.exists(TEST_TABLE, 'email = %s', ['nobody@example.com'], database=TEST_DB)}")

    print("\n--- Update ---")
    rows = db.update(TEST_TABLE, {'name': 'Alice Smith'}, 'id = %s', [user_id], database=TEST_DB)
    print(f"Updated {rows} row(s)")

    print("\n--- Upsert ---")
    db.upsert(TEST_TABLE, {'name': 'Eve', 'email': 'alice@example.com'}, update_fields=['name'], database=TEST_DB)
    updated = db.select(TEST_TABLE, where='id = %s', params=[user_id], database=TEST_DB)
    print(f"After upsert: {updated[0]}")

    print("\n--- Pagination ---")
    page = db.paginate(TEST_TABLE, page=1, per_page=2, order_by='id', database=TEST_DB)
    print(f"Pagination meta: {page['pagination']}")
    print(f"Page 1 data: {page['data']}")

    print("\n--- Batch update ---")
    db.batch_update(
        TEST_TABLE,
        updates=[
            {'id': 2, 'name': 'Bob Updated'},
            {'id': 3, 'name': 'Carol Updated'},
        ],
        key_field='id',
        database=TEST_DB,
    )

    print("\n--- Soft delete ---")
    db.delete(TEST_TABLE, 'email = %s', ['dave@example.com'], database=TEST_DB, soft=True)
    active = db.select(TEST_TABLE, where='deleted_at IS NULL', database=TEST_DB, order_by='id')
    print(f"Active users after soft delete: {[u['name'] for u in active]}")

    print("\n--- Transaction (manual) ---")
    try:
        with db.transaction(database=TEST_DB) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET name = 'TX Test' WHERE id = %s", (user_id,))
            cursor.close()
            # Simulate error to demonstrate rollback:
            # raise ValueError("Simulated error — transaction will roll back")
        print("Transaction committed")
    except Exception as e:
        print(f"Transaction rolled back: {e}")

    print("\n--- Table info ---")
    info = db.get_table_info(TEST_TABLE, database=TEST_DB)
    for col in info:
        print(col)

    print("\n--- Cleanup ---")
    db.drop_table(TEST_TABLE, database=TEST_DB)
    print("Table dropped. Done.")


if __name__ == '__main__':
    run_demo()
