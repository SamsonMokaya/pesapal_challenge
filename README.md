# Pesapal RDBMS Challenge

A simple relational database management system (RDBMS) implementation with SQL-like interface and interactive REPL.

## Features

- **Table Creation**: Create tables with column data types (INT, TEXT, BOOL, FLOAT)
- **CRUD Operations**: Create, Read, Update, Delete operations
- **Constraints**: Primary key, unique key, and foreign key support
- **Indexing**: Basic indexing for fast lookups (automatic on PRIMARY KEY and UNIQUE columns)
- **Joins**: INNER JOIN operations with multiple table support
- **Foreign Keys**: Referential integrity with RESTRICT behavior (prevents deletion if child records exist)
- **REPL**: Interactive command-line interface

## Project Structure

```
pesapal_challenge/
├── db/
│   ├── __init__.py
│   ├── engine.py      # Core DB logic (CRUD, constraints)
│   ├── parser.py      # SQL-like command parser
│   ├── storage.py     # File-based JSON storage
│   └── repl.py        # REPL interface
├── web/               # Web app
│   ├── app.py         # FastAPI app
│   └── models.py      # Pydantic models for the API
├── data/              # Database files (auto-created)
├── main.py            # REPL entry point
├── requirements.txt   # List of dependencies
└── README.md          # This file
```

## Installation

1. Clone the repository:

```bash
git clone <repo-url>
cd pesapal_challenge
```

2. Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

All commands should be run from the project root directory (`pesapal_challenge/`).

### Start the REPL

```bash
python main.py
```

### Comprehensive Testing Guide

Follow these commands step-by-step to test all functionality:

#### 1. Basic CRUD Operations

```sql
-- Create table with primary key, auto-increment, and unique constraint
CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, email TEXT UNIQUE, age INT);

-- Insert records (ID auto-incremented when NULL)
INSERT INTO users VALUES (NULL, 'John Doe', 'john@example.com', 30);
INSERT INTO users VALUES (NULL, 'Jane Smith', 'jane@example.com', 25);
INSERT INTO users VALUES (NULL, 'Bob Johnson', 'bob@example.com', 35);

-- Select all records
SELECT * FROM users;

-- Select specific columns
SELECT name, email FROM users;

-- Select with WHERE clause (equality)
SELECT name, email FROM users WHERE id=1;

-- Update record by ID
UPDATE users SET age=31 WHERE id=1;

-- Update multiple records with WHERE clause
UPDATE users SET age=30 WHERE age=35;

-- Delete record by ID
DELETE FROM users WHERE id=2;

-- Delete with WHERE clause
DELETE FROM users WHERE age=30;
```

#### 2. Data Types (INT, TEXT, BOOL, FLOAT)

```sql
-- Create table with all data types
CREATE TABLE products (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, price FLOAT, in_stock BOOL, description TEXT);

-- Insert with all data types
INSERT INTO products VALUES (NULL, 'Laptop', 999.99, true, 'High-performance laptop');
INSERT INTO products VALUES (NULL, 'Mouse', 29.99, false, 'Wireless mouse');
INSERT INTO products VALUES (NULL, 'Keyboard', 79.50, true, 'Mechanical keyboard');

-- Select all products
SELECT * FROM products;

-- Query with FLOAT (equality only - > and < operators not supported)
SELECT * FROM products WHERE price = 79.50;

-- Query with BOOL
SELECT * FROM products WHERE in_stock = true;
```

#### 3. Constraints and Error Cases

```sql
-- Auto-increment: ID automatically generated
INSERT INTO users VALUES (NULL, 'Alice Brown', 'alice@example.com', 28);
-- Verify ID was auto-generated
SELECT * FROM users WHERE name='Alice Brown';

-- Error: Unique constraint violation (duplicate email)
INSERT INTO users VALUES (NULL, 'Duplicate', 'john@example.com', 28);
-- Expected error: "Unique constraint violation: duplicate value 'john@example.com' in column 'email'"

-- Error: Primary key violation (duplicate ID)
INSERT INTO users VALUES (1, 'Duplicate ID', 'duplicate@example.com', 30);
-- Expected error: "Primary key violation: duplicate value '1' in column 'id'"

-- Test multiple unique columns
CREATE TABLE accounts (id INT PRIMARY KEY AUTO_INCREMENT, username TEXT UNIQUE, email TEXT UNIQUE, balance FLOAT);
INSERT INTO accounts VALUES (NULL, 'user1', 'user1@example.com', 100.0);
-- Error: Duplicate username
INSERT INTO accounts VALUES (NULL, 'user1', 'user2@example.com', 200.0);
-- Error: Duplicate email
INSERT INTO accounts VALUES (NULL, 'user2', 'user1@example.com', 200.0);
```

#### 4. WHERE Clause Features

```sql
-- Case-insensitive TEXT comparison
SELECT * FROM users WHERE name='john doe';
-- Should match 'John Doe' (case-insensitive)

-- LIKE operator with wildcards
INSERT INTO users VALUES (NULL, 'Johnny Walker', 'johnny@example.com', 40);
INSERT INTO users VALUES (NULL, 'John Smith', 'john.smith@example.com', 32);

-- LIKE with % wildcard (matches zero or more characters - use this to find "anything with John")
SELECT * FROM users WHERE name LIKE 'John%';
-- Matches: 'John', 'John Doe', 'Johnny Walker', 'John Smith', 'JohnX'
-- Use % when you want to find anything starting with "John"

SELECT * FROM users WHERE email LIKE '%@example.com';
-- Matches all emails ending with @example.com

-- LIKE with _ wildcard (matches exactly ONE character - not for finding "anything with John")
SELECT * FROM users WHERE name LIKE 'John_';
-- Matches ONLY: 'JohnX', 'John1', 'JohnA' (exactly 5 characters: "John" + 1 char)
-- Does NOT match: 'John' (too short), 'John Doe' (too long - has 4 chars after "John")
-- Use _ only when you need exactly one character after "John"
```

#### 5. JOIN Operations

```sql
-- Create related tables
CREATE TABLE orders (id INT PRIMARY KEY AUTO_INCREMENT, user_id INT, total FLOAT, status TEXT);
INSERT INTO orders VALUES (NULL, 1, 100.50, 'pending');
INSERT INTO orders VALUES (NULL, 1, 250.75, 'completed');
INSERT INTO orders VALUES (NULL, 3, 50.00, 'pending');

-- Single JOIN
SELECT * FROM users JOIN orders ON users.id = orders.user_id;

-- JOIN with WHERE clause
SELECT users.name, orders.total, orders.status FROM users JOIN orders ON users.id = orders.user_id WHERE orders.status='pending';

-- Multiple JOINs (3 tables)
CREATE TABLE order_items (id INT PRIMARY KEY AUTO_INCREMENT, order_id INT, product_id INT, quantity INT);
INSERT INTO order_items VALUES (NULL, 1, 1, 2);
INSERT INTO order_items VALUES (NULL, 1, 2, 1);

-- Join users -> orders -> order_items
SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN order_items ON orders.id = order_items.order_id;
```

#### 6. Foreign Key Constraints

```sql
-- Foreign key with RESTRICT (default - prevents deletion if child records exist)
CREATE TABLE order_items_fk (id INT PRIMARY KEY AUTO_INCREMENT, order_id INT REFERENCES orders(id), product_id INT, quantity INT);
INSERT INTO order_items_fk VALUES (NULL, 1, 1, 2);

-- Error: Foreign key constraint violation
DELETE FROM orders WHERE id=1;
-- Expected error: "Cannot delete or update a parent row: a foreign key constraint fails (order_items_fk.order_id -> orders.id)"

-- Verify order still exists
SELECT * FROM orders WHERE id=1;

-- Must delete child records first
DELETE FROM order_items_fk WHERE order_id=1;
DELETE FROM orders WHERE id=1;

-- Foreign key with CASCADE (automatically deletes child records)
CREATE TABLE order_items_cascade (id INT PRIMARY KEY AUTO_INCREMENT, order_id INT REFERENCES orders(id) ON DELETE CASCADE, product_id INT, quantity INT);
INSERT INTO orders VALUES (NULL, 3, 75.00, 'pending');
INSERT INTO order_items_cascade VALUES (NULL, 2, 1, 3);

-- Success: Order and order_items_cascade both deleted automatically
DELETE FROM orders WHERE id=2;

-- Verify cascade delete worked
SELECT * FROM orders WHERE id=2;
SELECT * FROM order_items_cascade WHERE order_id=2;
-- Both should return empty results
```

#### 7. Indexing (Automatic)

Indexes are automatically created for PRIMARY KEY and UNIQUE columns. They speed up lookups:

```sql
-- These queries use indexes automatically (faster):
SELECT * FROM users WHERE id=1;  -- Uses primary key index
SELECT * FROM users WHERE email='john@example.com';  -- Uses unique index

-- Verify indexes exist (check data/users.json file structure)
-- Indexes are stored in the table's JSON file under "indexes" key
```

### Web App

1. Set up demo tables (recommended for testing endpoints):

```bash
python setup_demo_tables.py
```

2. Start the FastAPI web server (from the project root directory):

```bash
python -m uvicorn web.app:app --reload
```

Then visit:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **API Root**: http://localhost:8000/

#### Available Endpoints

All tables (users, products, orders, order-items) follow the same endpoint pattern. Replace `{table_name}` with the actual table name:

- `GET /{table_name}` - Get all records
- `GET /{table_name}/{id}` - Get record by ID
- `POST /{table_name}` - Create a new record
- `PUT /{table_name}/{id}` - Update record by ID
- `DELETE /{table_name}/{id}` - Delete record by ID

#### Example API Usage

```bash
# Get all records (replace 'users' with any table name)
curl http://localhost:8000/users

# Get record by ID
curl http://localhost:8000/users/1

# Create a new record (ID auto-incremented, omit from request)
curl -X POST http://localhost:8000/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice", "email": "alice@example.com", "age": 28}'

# Update record (partial update - only include fields to change)
curl -X PUT http://localhost:8000/users/1 \
  -H "Content-Type: application/json" \
  -d '{"age": 29}'

# Delete record
curl -X DELETE http://localhost:8000/users/1

# Example: Create a product
curl -X POST http://localhost:8000/products \
  -H "Content-Type: application/json" \
  -d '{"name": "Tablet", "price": 399.99, "description": "10-inch tablet", "in_stock": true}'

# Example: Get order items for an order
curl http://localhost:8000/order-items/order/1
```

## Supported Data Types

- **INT**: Integer numbers
- **TEXT**: String values
- **BOOL**: Boolean values (true/false, 1/0)
- **FLOAT**: Floating-point numbers

## Data Persistence

All table data is stored in JSON files in the `data/` directory. Each table has its own file (`table_name.json`) containing:

- Schema (columns, constraints, indexes)
- Data rows
- Indexes
