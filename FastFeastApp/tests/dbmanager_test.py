import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db.database_manager import DatabaseManager

def test_snowflake_connectivity():
    db = DatabaseManager()

    # Test 1 — connection object exists and is open
    assert db.connection is not None
    assert not db.connection.is_closed(), "Connection should be open"
    print("✓ Connection established successfully")

    # Test 2 — can actually execute a query
    rows = db.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
    assert rows is not None
    user, role, warehouse = rows[0]
    print(f"✓ Connected as user    : {user}")
    print(f"✓ Active role          : {role}")
    print(f"✓ Active warehouse     : {warehouse}")

    # Test 3 — singleton holds (same instance returned twice)
    db2 = DatabaseManager()
    assert db is db2, "DatabaseManager must be a singleton"
    print("✓ Singleton pattern verified")

    # Test 4 — cursor_scope works
    with db.cursor_scope() as cursor:
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        database, schema = cursor.fetchone()
        print(f"✓ Active database      : {database}")
        print(f"✓ Active schema        : {schema}")

    db.close()
    print("✓ Connection closed cleanly")


if __name__ == "__main__":
    test_snowflake_connectivity()