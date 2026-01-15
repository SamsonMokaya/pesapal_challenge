# Pesapal RDBMS Challenge

A simple relational database management system (RDBMS) implementation with SQL-like interface and interactive REPL.

## Features

- **Table Creation**: Create tables with column data types (INT, TEXT, BOOL, FLOAT)
- **CRUD Operations**: Create, Read, Update, Delete operations
- **Constraints**: Primary key and unique key support
- **Indexing**: Basic indexing for fast lookups (automatic on PRIMARY KEY and UNIQUE columns)
- **Joins**: INNER JOIN operations with multiple table support
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
├── web/               # Web app (Phase 7)
│   ├── app.py
│   ├── templates/
│   └── static/
├── data/              # Database files (auto-created)
├── main.py            # REPL entry point
├── requirements.txt
└── README.md
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

4. (Optional) Set up demo tables for web app:

```bash
python setup_demo_tables.py
```

## Usage

### Start the REPL

```bash
python main.py
```

### Example Commands (REPL)

```sql
-- Create table with primary key, auto-increment, and unique constraint
CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, email TEXT UNIQUE, age INT);

-- Insert records (ID auto-incremented when NULL)
INSERT INTO users VALUES (NULL, 'John Doe', 'john@example.com', 30);
INSERT INTO users VALUES (NULL, 'Jane Smith', 'jane@example.com', 25);

-- Select all records
SELECT * FROM users;

-- Select specific columns with WHERE clause
SELECT name, email FROM users WHERE id=1;

-- Update record
UPDATE users SET age=31 WHERE id=1;

-- Delete record
DELETE FROM users WHERE id=2;

-- Auto-increment: ID automatically generated
INSERT INTO users VALUES (NULL, 'Bob Johnson', 'bob@example.com', 35);

-- Error: Unique constraint violation (duplicate email)
INSERT INTO users VALUES (NULL, 'Duplicate', 'john@example.com', 28);

-- Error: Primary key violation (duplicate ID)
INSERT INTO users VALUES (1, 'Duplicate ID', 'duplicate@example.com', 30);

-- JOIN example (create orders table first)
CREATE TABLE orders (id INT PRIMARY KEY AUTO_INCREMENT, user_id INT, total FLOAT, status TEXT);
INSERT INTO orders VALUES (NULL, 1, 100.50, 'pending');

-- Join users and orders tables
SELECT * FROM users JOIN orders ON users.id = orders.user_id;
```

### Web App

Start the FastAPI web server:

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

**Special endpoint for order-items:**

- `GET /order-items/order/{order_id}` - Get all items for a specific order

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
