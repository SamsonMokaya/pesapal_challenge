#!/usr/bin/env python3
"""
Setup script to create demo tables for the RDBMS web app.
Run this script to populate the database with example tables and data.
"""

from db.engine import DatabaseEngine
from db.storage import Storage
from db.parser import SQLParser
import os
import shutil

def setup_demo_tables():
    """Create demo tables with sample data."""
    
    # Clean up existing data (optional - comment out if you want to keep existing data)
    # if os.path.exists("data"):
    #     shutil.rmtree("data")
    
    # Initialize components
    storage = Storage("data")
    engine = DatabaseEngine(storage)
    parser = SQLParser()
    
    print("=" * 70)
    print("SETTING UP DEMO TABLES")
    print("=" * 70)
    print()
    
    # Create users table
    print("Creating 'users' table...")
    if not engine.storage.table_exists("users"):
        engine.create_table("users", [
            {"name": "id", "type": "INT", "primary_key": True, "auto_increment": True},
            {"name": "name", "type": "TEXT"},
            {"name": "email", "type": "TEXT", "unique": True},
            {"name": "age", "type": "INT", "nullable": True}
        ])
        print("  ✓ Created")
    else:
        print("  ✓ Already exists (skipping)")
    
    # Insert sample users (only if table is empty or we want to add more)
    print("Inserting sample users...")
    try:
        existing_rows = engine.storage.get_all_rows("users")
        if len(existing_rows) == 0:
            engine.insert("users", [None, "John Doe", "john@example.com", 30])
            engine.insert("users", [None, "Jane Smith", "jane@example.com", 25])
            engine.insert("users", [None, "Bob Johnson", "bob@example.com", 35])
            print("  ✓ Inserted 3 users")
        else:
            print(f"  ✓ Table already has {len(existing_rows)} rows (skipping insert)")
    except Exception as e:
        print(f"  ⚠ Could not insert users: {e}")
    
    # Create products table
    print("\nCreating 'products' table...")
    if not engine.storage.table_exists("products"):
        engine.create_table("products", [
            {"name": "id", "type": "INT", "primary_key": True, "auto_increment": True},
            {"name": "name", "type": "TEXT"},
            {"name": "price", "type": "FLOAT"},
            {"name": "description", "type": "TEXT", "nullable": True},
            {"name": "in_stock", "type": "BOOL", "nullable": True}
        ])
        print("  ✓ Created")
    else:
        print("  ✓ Already exists (skipping)")
    
    # Insert sample products
    print("Inserting sample products...")
    try:
        existing_rows = engine.storage.get_all_rows("products")
        if len(existing_rows) == 0:
            engine.insert("products", [None, "Laptop", 999.99, "High-performance laptop", True])
            engine.insert("products", [None, "Mouse", 29.99, "Wireless mouse", True])
            engine.insert("products", [None, "Keyboard", 79.99, "Mechanical keyboard", False])
            engine.insert("products", [None, "Monitor", 299.99, "27-inch 4K monitor", True])
            print("  ✓ Inserted 4 products")
        else:
            print(f"  ✓ Table already has {len(existing_rows)} rows (skipping insert)")
    except Exception as e:
        print(f"  ⚠ Could not insert products: {e}")
    
    # Create orders table
    print("\nCreating 'orders' table...")
    if not engine.storage.table_exists("orders"):
        engine.create_table("orders", [
            {"name": "id", "type": "INT", "primary_key": True, "auto_increment": True},
            {"name": "user_id", "type": "INT"},
            {"name": "total", "type": "FLOAT"},
            {"name": "status", "type": "TEXT"},
            {"name": "created_at", "type": "TEXT", "nullable": True}
        ])
        print("  ✓ Created")
    else:
        print("  ✓ Already exists (skipping)")
    
    # Insert sample orders
    print("Inserting sample orders...")
    try:
        existing_rows = engine.storage.get_all_rows("orders")
        if len(existing_rows) == 0:
            engine.insert("orders", [None, 1, 999.99, "pending", "2024-01-15"])
            engine.insert("orders", [None, 1, 29.99, "completed", "2024-01-16"])
            engine.insert("orders", [None, 2, 79.99, "pending", "2024-01-17"])
            engine.insert("orders", [None, 3, 299.99, "completed", "2024-01-18"])
            print("  ✓ Inserted 4 orders")
        else:
            print(f"  ✓ Table already has {len(existing_rows)} rows (skipping insert)")
    except Exception as e:
        print(f"  ⚠ Could not insert orders: {e}")
    
    # Create order_items table
    print("\nCreating 'order_items' table...")
    if not engine.storage.table_exists("order_items"):
        engine.create_table("order_items", [
            {"name": "id", "type": "INT", "primary_key": True, "auto_increment": True},
            {"name": "order_id", "type": "INT"},
            {"name": "product_id", "type": "INT"},
            {"name": "quantity", "type": "INT"}
        ])
        print("  ✓ Created")
    else:
        print("  ✓ Already exists (skipping)")
    
    # Insert sample order items
    print("Inserting sample order items...")
    try:
        existing_rows = engine.storage.get_all_rows("order_items")
        if len(existing_rows) == 0:
            engine.insert("order_items", [None, 1, 1, 1])  # Order 1: 1 Laptop
            engine.insert("order_items", [None, 2, 2, 1])  # Order 2: 1 Mouse
            engine.insert("order_items", [None, 3, 3, 1])  # Order 3: 1 Keyboard
            engine.insert("order_items", [None, 4, 4, 1])  # Order 4: 1 Monitor
            print("  ✓ Inserted 4 order items")
        else:
            print(f"  ✓ Table already has {len(existing_rows)} rows (skipping insert)")
    except Exception as e:
        print(f"  ⚠ Could not insert order_items: {e}")
    
    print("\n" + "=" * 70)
    print("DEMO TABLES SETUP COMPLETE!")
    print("=" * 70)
    print("\nTables created:")
    print("  - users (3 rows)")
    print("  - products (4 rows)")
    print("  - orders (4 rows)")
    print("  - order_items (4 rows)")
    print("\nYou can now start the web app with:")
    print("  python -m uvicorn web.app:app --reload")
    print("\nThen visit http://localhost:8000/docs for Swagger UI")
    print("=" * 70)


if __name__ == "__main__":
    setup_demo_tables()
