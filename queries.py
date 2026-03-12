"""
Example query functions built on top of DatabaseConnection.

These show common patterns: filtering, joining, aggregating, and pagination.
"""
from typing import Dict, List, Optional
from database import DatabaseConnection

db = DatabaseConnection()


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

def get_user_by_id(user_id: int) -> Optional[Dict]:
    results = db.select('users', where='id = %s', params=[user_id])
    return results[0] if results else None


def get_user_by_email(email: str) -> Optional[Dict]:
    results = db.select('users', where='email = %s', params=[email])
    return results[0] if results else None


def get_active_users(page: int = 1, per_page: int = 20) -> Dict:
    return db.paginate(
        'users',
        page=page,
        per_page=per_page,
        where='deleted_at IS NULL',
        order_by='created_at DESC',
    )


def create_user(name: str, email: str) -> int:
    """Returns the new user's ID."""
    return db.insert('users', {'name': name, 'email': email})


def update_user_email(user_id: int, new_email: str) -> int:
    return db.update('users', {'email': new_email}, 'id = %s', [user_id])


def soft_delete_user(user_id: int) -> int:
    return db.delete('users', 'id = %s', [user_id], soft=True)


def user_exists(email: str) -> bool:
    return db.exists('users', 'email = %s', [email])


def count_active_users() -> int:
    return db.count('users', 'deleted_at IS NULL')


# ---------------------------------------------------------------------------
# Users + Orders (JOIN example)
# ---------------------------------------------------------------------------

def get_users_with_order_count() -> List[Dict]:
    """Returns each user with a count of their orders."""
    return db.select(
        table='users u',
        columns='u.id, u.name, u.email, COUNT(o.id) AS order_count',
        joins='LEFT JOIN orders o ON u.id = o.user_id',
        where='u.deleted_at IS NULL',
        order_by='order_count DESC',
    )


def get_user_orders(user_id: int) -> List[Dict]:
    """Returns all orders for a given user with product details."""
    return db.select(
        table='orders o',
        columns='o.id, o.created_at, p.name AS product, p.price',
        joins=[
            'JOIN users u ON o.user_id = u.id',
            'JOIN products p ON o.product_id = p.id',
        ],
        where='o.user_id = %s',
        params=[user_id],
        order_by='o.created_at DESC',
    )
