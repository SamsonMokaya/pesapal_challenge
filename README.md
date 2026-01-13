# Pesapal RDBMS Challenge

A simple relational database management system (RDBMS) implementation with SQL-like interface and interactive REPL.

## Features

- **Table Creation**: Create tables with column data types (INT, TEXT, BOOL, FLOAT)
- **CRUD Operations**: Create, Read, Update, Delete operations
- **Constraints**: Primary key and unique key support
- **Indexing**: Basic indexing for fast lookups (Phase 2+)
- **Joins**: Table joining capabilities (Phase 6+)
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

## Usage

### Start the REPL

```bash
python main.py
```

### Example Commands

```sql
mydb> CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT);
OK

mydb> INSERT INTO users VALUES (1, 'john@example.com', 'John Doe');
OK

mydb> INSERT INTO users VALUES (2, 'jane@example.com', 'Jane Smith');
OK

mydb> SELECT * FROM users;
id=1 email=john@example.com name=John Doe
id=2 email=jane@example.com name=Jane Smith

mydb> SELECT name, email FROM users WHERE id=1;
name=John Doe email=john@example.com

mydb> list tables
Tables:
  - users

mydb> exit
```

## Supported Data Types

- **INT**: Integer numbers
- **TEXT**: String values
- **BOOL**: Boolean values (true/false, 1/0)
- **FLOAT**: Floating-point numbers

## Current Status

**Phase 1: Core Engine** ✅

- [x] Storage layer (JSON file-based)
- [x] Table creation with schema
- [x] INSERT operation
- [x] SELECT operation with WHERE clause
- [x] Primary key and unique constraints
- [x] Type validation
- [x] REPL interface

**Phase 2-7**: Coming soon...

## Data Persistence

All table data is stored in JSON files in the `data/` directory. Each table has its own file (`table_name.json`) containing:

- Schema (columns, constraints, indexes)
- Data rows
- Indexes
