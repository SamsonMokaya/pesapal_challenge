"""
Core database engine logic.
Handles table creation, CRUD operations, and data validation.
"""

from typing import Dict, List, Any, Optional, Tuple
from .storage import Storage


class DatabaseEngine:
    """Core database engine for table operations and queries."""
    
    # Supported data types
    SUPPORTED_TYPES = {'INT', 'TEXT', 'BOOL', 'FLOAT'}
    
    def __init__(self, storage: Optional[Storage] = None):
        """
        Initialize the database engine.
        
        Args:
            storage: Storage instance (creates new one if not provided)
        """
        self.storage = storage or Storage()
    
    def create_table(self, table_name: str, columns: List[Dict[str, Any]]) -> None:
        """
        Create a new table with specified columns.
        
        Args:
            table_name: Name of the table
            columns: List of column definitions, each with:
                - name: column name
                - type: data type (INT, TEXT, BOOL, FLOAT)
                - primary_key: boolean
                - unique: boolean
                - nullable: boolean (default True, False for primary keys)
        
        Raises:
            ValueError: If table exists or schema is invalid
        """
        if self.storage.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' already exists")
        
        # Validate and normalize schema
        schema = self._validate_schema(columns)
        
        # Create table in storage
        self.storage.create_table(table_name, schema)
    
    def _validate_schema(self, columns: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate and normalize table schema.
        
        Args:
            columns: List of column definitions
            
        Returns:
            Normalized schema dictionary
        """
        if not columns:
            raise ValueError("Table must have at least one column")
        
        schema = {
            "columns": {},
            "primary_key": None,
            "unique_keys": [],
            "indexes": {}
        }
        
        column_names = set()
        primary_key_count = 0
        
        for col_def in columns:
            col_name = col_def.get("name")
            if not col_name:
                raise ValueError("Column must have a name")
            
            if col_name in column_names:
                raise ValueError(f"Duplicate column name: '{col_name}'")
            column_names.add(col_name)
            
            col_type = col_def.get("type", "").upper()
            if col_type not in self.SUPPORTED_TYPES:
                raise ValueError(f"Unsupported data type: '{col_type}'. Supported types: {self.SUPPORTED_TYPES}")
            
            is_primary = col_def.get("primary_key", False)
            is_unique = col_def.get("unique", False)
            nullable = col_def.get("nullable", True) if not is_primary else False
            
            if is_primary:
                primary_key_count += 1
                if primary_key_count > 1:
                    raise ValueError("Table can have only one primary key")
                schema["primary_key"] = col_name
                # Primary keys are automatically unique
                is_unique = True
            
            schema["columns"][col_name] = {
                "type": col_type,
                "nullable": nullable,
                "unique": is_unique or is_primary
            }
            
            # Create index for primary key automatically
            if is_primary:
                schema["indexes"][f"{col_name}_idx"] = {
                    "column": col_name,
                    "type": "hash"
                }
        
        return schema
    
    def _validate_value_type(self, value: Any, expected_type: str) -> Any:
        """
        Validate and convert a value to the expected type.
        
        Args:
            value: Value to validate
            expected_type: Expected data type
            
        Returns:
            Converted value
            
        Raises:
            ValueError: If value cannot be converted to expected type
        """
        if value is None:
            return None
        
        expected_type = expected_type.upper()
        
        if expected_type == "INT":
            try:
                return int(value)
            except (ValueError, TypeError):
                raise ValueError(f"Cannot convert '{value}' to INT")
        
        elif expected_type == "FLOAT":
            try:
                return float(value)
            except (ValueError, TypeError):
                raise ValueError(f"Cannot convert '{value}' to FLOAT")
        
        elif expected_type == "BOOL":
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, str)):
                value_str = str(value).lower()
                if value_str in ('true', '1', 'yes', 't'):
                    return True
                elif value_str in ('false', '0', 'no', 'f', ''):
                    return False
            raise ValueError(f"Cannot convert '{value}' to BOOL")
        
        elif expected_type == "TEXT":
            return str(value)
        
        else:
            raise ValueError(f"Unsupported type: {expected_type}")
    
    def insert(self, table_name: str, values: List[Any]) -> None:
        """
        Insert a new row into a table.
        
        Args:
            table_name: Name of the table
            values: List of values in column order
            
        Raises:
            ValueError: If table doesn't exist, wrong number of values, or constraint violation
        """
        if not self.storage.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")
        
        schema = self.storage.get_schema(table_name)
        columns = list(schema["columns"].keys())
        
        if len(values) != len(columns):
            raise ValueError(
                f"Column count mismatch: expected {len(columns)}, got {len(values)}"
            )
        
        # Build row dictionary and validate types
        row = {}
        for i, (col_name, col_def) in enumerate(schema["columns"].items()):
            value = values[i]
            
            # Check NULL constraint
            if value is None or (isinstance(value, str) and value.upper() == 'NULL'):
                if not col_def["nullable"]:
                    raise ValueError(f"Column '{col_name}' cannot be NULL")
                row[col_name] = None
            else:
                # Validate and convert type
                converted_value = self._validate_value_type(value, col_def["type"])
                row[col_name] = converted_value
        
        # Check primary key constraint
        if schema["primary_key"]:
            pk_col = schema["primary_key"]
            pk_value = row[pk_col]
            
            if pk_value is None:
                raise ValueError(f"Primary key column '{pk_col}' cannot be NULL")
            
            # Check for duplicate primary key
            existing_rows = self.storage.get_all_rows(table_name)
            for existing_row in existing_rows:
                if existing_row[pk_col] == pk_value:
                    raise ValueError(
                        f"Primary key violation: duplicate value '{pk_value}' in column '{pk_col}'"
                    )
        
        # Check unique constraints
        for col_name, col_def in schema["columns"].items():
            if col_def["unique"] and row[col_name] is not None:
                existing_rows = self.storage.get_all_rows(table_name)
                for existing_row in existing_rows:
                    if existing_row[col_name] == row[col_name]:
                        raise ValueError(
                            f"Unique constraint violation: duplicate value '{row[col_name]}' in column '{col_name}'"
                        )
        
        # Insert the row
        self.storage.insert_row(table_name, row)
    
    def select(self, table_name: str, columns: Optional[List[str]] = None, 
               where_clause: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Select rows from a table.
        
        Args:
            table_name: Name of the table
            columns: List of column names to select (None for all columns)
            where_clause: Dictionary of column: value filters
            
        Returns:
            List of matching row dictionaries
            
        Raises:
            ValueError: If table doesn't exist or column doesn't exist
        """
        if not self.storage.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")
        
        schema = self.storage.get_schema(table_name)
        all_rows = self.storage.get_all_rows(table_name)
        
        # Validate columns if specified
        if columns:
            for col in columns:
                if col not in schema["columns"]:
                    raise ValueError(f"Column '{col}' not found in table '{table_name}'")
        else:
            columns = list(schema["columns"].keys())
        
        # Apply WHERE clause filtering
        filtered_rows = all_rows
        if where_clause:
            filtered_rows = []
            for row in all_rows:
                match = True
                for col, expected_value in where_clause.items():
                    if col not in schema["columns"]:
                        raise ValueError(f"Column '{col}' not found in table '{table_name}'")
                    
                    # Convert expected_value to proper type for comparison
                    col_type = schema["columns"][col]["type"]
                    try:
                        if expected_value is None or (isinstance(expected_value, str) and expected_value.upper() == 'NULL'):
                            if row[col] is not None:
                                match = False
                                break
                        else:
                            converted_value = self._validate_value_type(expected_value, col_type)
                            if row[col] != converted_value:
                                match = False
                                break
                    except ValueError:
                        # Type conversion failed, no match
                        match = False
                        break
                
                if match:
                    filtered_rows.append(row)
        
        # Select only requested columns
        result = []
        for row in filtered_rows:
            result_row = {col: row[col] for col in columns}
            result.append(result_row)
        
        return result
    
    def list_tables(self) -> List[str]:
        """
        List all tables in the database.
        
        Returns:
            List of table names
        """
        data_dir = self.storage.data_dir
        tables = []
        for file_path in data_dir.glob("*.json"):
            tables.append(file_path.stem)
        return sorted(tables)
