#!/usr/bin/env python3
"""
Setup script to create demo tables for the RDBMS web app.
Run this script to populate the database with example tables and data.
"""

from db.engine import DatabaseEngine
from db.storage import Storage
from db.parser import SQLParser

def setup_demo_tables():
    """Create demo tables with sample data.
    
    This script will delete and recreate all demo tables fresh each time it's run.
    """
    
    # Initialize components
    storage = Storage("data")
    engine = DatabaseEngine(storage)
    parser = SQLParser()
    
    print("=" * 70)
    print("SETTING UP DEMO TABLES")
    print("=" * 70)
    print()
    
    # Create users table (delete if exists first)
    print("Creating 'users' table...")
    if engine.storage.table_exists("users"):
        engine.storage.drop_table("users")
        print("  ✓ Deleted existing table")
    engine.create_table("users", [
        {"name": "id", "type": "INT", "primary_key": True, "auto_increment": True},
        {"name": "name", "type": "TEXT"},
        {"name": "email", "type": "TEXT", "unique": True},
        {"name": "age", "type": "INT", "nullable": True}
    ])
    print("  ✓ Created")
    
    # Insert sample users
    print("Inserting sample users...")
    engine.insert("users", [None, "John Doe", "john@example.com", 30])
    engine.insert("users", [None, "Jane Smith", "jane@example.com", 25])
    engine.insert("users", [None, "Bob Johnson", "bob@example.com", 35])
    print("  ✓ Inserted 3 users")
    
    # Create products table (delete if exists first)
    print("\nCreating 'products' table...")
    if engine.storage.table_exists("products"):
        engine.storage.drop_table("products")
        print("  ✓ Deleted existing table")
    engine.create_table("products", [
        {"name": "id", "type": "INT", "primary_key": True, "auto_increment": True},
        {"name": "name", "type": "TEXT"},
        {"name": "price", "type": "FLOAT"},
        {"name": "description", "type": "TEXT", "nullable": True},
        {"name": "in_stock", "type": "BOOL", "nullable": True}
    ])
    print("  ✓ Created")
    
    # Insert sample products
    print("Inserting sample products...")
    engine.insert("products", [None, "Laptop", 999.99, "High-performance laptop", True])
    engine.insert("products", [None, "Mouse", 29.99, "Wireless mouse", True])
    engine.insert("products", [None, "Keyboard", 79.99, "Mechanical keyboard", False])
    engine.insert("products", [None, "Monitor", 299.99, "27-inch 4K monitor", True])
    print("  ✓ Inserted 4 products")
    
    # Create orders table (delete if exists first)
    print("\nCreating 'orders' table...")
    if engine.storage.table_exists("orders"):
        engine.storage.drop_table("orders")
        print("  ✓ Deleted existing table")
    engine.create_table("orders", [
        {"name": "id", "type": "INT", "primary_key": True, "auto_increment": True},
        {"name": "user_id", "type": "INT"},
        {"name": "total", "type": "FLOAT"},
        {"name": "status", "type": "TEXT"},
        {"name": "created_at", "type": "TEXT", "nullable": True}
    ])
    print("  ✓ Created")
    
    # Insert sample orders
    print("Inserting sample orders...")
    engine.insert("orders", [None, 1, 999.99, "pending", "2024-01-15"])
    engine.insert("orders", [None, 1, 29.99, "completed", "2024-01-16"])
    engine.insert("orders", [None, 2, 79.99, "pending", "2024-01-17"])
    engine.insert("orders", [None, 3, 299.99, "completed", "2024-01-18"])
    print("  ✓ Inserted 4 orders")
    
    # Create order_items table (delete if exists first)
    print("\nCreating 'order_items' table...")
    if engine.storage.table_exists("order_items"):
        engine.storage.drop_table("order_items")
        print("  ✓ Deleted existing table")
    engine.create_table("order_items", [
        {"name": "id", "type": "INT", "primary_key": True, "auto_increment": True},
        {"name": "order_id", "type": "INT"},
        {"name": "product_id", "type": "INT"},
        {"name": "quantity", "type": "INT"}
    ])
    print("  ✓ Created")
    
    # Insert sample order items
    print("Inserting sample order items...")
    engine.insert("order_items", [None, 1, 1, 1])  # Order 1: 1 Laptop
    engine.insert("order_items", [None, 2, 2, 1])  # Order 2: 1 Mouse
    engine.insert("order_items", [None, 3, 3, 1])  # Order 3: 1 Keyboard
    engine.insert("order_items", [None, 4, 4, 1])  # Order 4: 1 Monitor
    print("  ✓ Inserted 4 order items")
    
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
