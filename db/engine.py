"""
Core database engine logic.
Handles table creation, CRUD operations, and data validation.
"""

from typing import Dict, List, Any, Optional, Tuple
import re
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
        
        # Build initial indexes (empty since table is new)
        self._build_all_indexes(table_name)
    
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
            "indexes": {},
            "auto_increment": None,  # Column name with auto_increment
            "auto_increment_counter": 0,  # Current counter value
            "foreign_keys": []  # List of foreign key definitions
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
            is_auto_increment = col_def.get("auto_increment", False)
            nullable = col_def.get("nullable", True) if not is_primary else False
            
            # Validate AUTO_INCREMENT
            if is_auto_increment:
                if not is_primary:
                    raise ValueError(f"AUTO_INCREMENT can only be used with PRIMARY KEY. Column '{col_name}' is not a primary key")
                if col_type != "INT":
                    raise ValueError(f"AUTO_INCREMENT can only be used with INT type. Column '{col_name}' is {col_type}")
            
            if is_primary:
                primary_key_count += 1
                if primary_key_count > 1:
                    raise ValueError("Table can have only one primary key")
                schema["primary_key"] = col_name
                # Primary keys are automatically unique
                is_unique = True
                
                # Set auto_increment if specified
                if is_auto_increment:
                    schema["auto_increment"] = col_name
            
            # Handle foreign key
            foreign_key = col_def.get("foreign_key")
            if foreign_key:
                # Validate that referenced table exists (if it does)
                ref_table = foreign_key.get("references_table")
                ref_column = foreign_key.get("references_column")
                on_delete = foreign_key.get("on_delete", "RESTRICT")  # Default to RESTRICT
                if ref_table and ref_column:
                    schema["foreign_keys"].append({
                        "column": col_name,
                        "references_table": ref_table,
                        "references_column": ref_column,
                        "on_delete": on_delete
                    })
            
            schema["columns"][col_name] = {
                "type": col_type,
                "nullable": nullable,
                "unique": is_unique or is_primary,
                "foreign_key": foreign_key
            }
            
            # Create index for primary key automatically
            if is_primary:
                schema["indexes"][f"{col_name}_idx"] = {
                    "column": col_name,
                    "type": "hash"
                }
            # Create index for unique columns automatically
            elif is_unique:
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
        auto_increment_col = schema.get("auto_increment")
        
        # Handle auto-increment: if column exists and values list is shorter, auto-generate ID
        if auto_increment_col and len(values) == len(columns) - 1:
            # Insert auto-increment value at the beginning
            auto_increment_idx = columns.index(auto_increment_col)
            counter = schema.get("auto_increment_counter", 0)
            new_id = counter + 1
            values.insert(auto_increment_idx, new_id)
        
        if len(values) != len(columns):
            raise ValueError(
                f"Column count mismatch: expected {len(columns)}, got {len(values)}"
            )
        
        # Build row dictionary and validate types
        row = {}
        for i, (col_name, col_def) in enumerate(schema["columns"].items()):
            value = values[i]
            
            # Handle auto-increment: if NULL or missing, auto-generate
            if auto_increment_col == col_name:
                if value is None or (isinstance(value, str) and value.upper() == 'NULL'):
                    counter = schema.get("auto_increment_counter", 0)
                    value = counter + 1
            
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
        
        # Update auto_increment counter after insert
        if auto_increment_col:
            pk_value = row[auto_increment_col]
            schema = self.storage.get_schema(table_name)
            current_counter = schema.get("auto_increment_counter", 0)
            if pk_value > current_counter:
                schema["auto_increment_counter"] = pk_value
                # Save updated schema
                table_data = self.storage.get_table(table_name)
                table_data["schema"] = schema
                self.storage.save_table(table_name, table_data)
        
        # Update indexes
        all_rows = self.storage.get_all_rows(table_name)
        row_index = len(all_rows) - 1  # Index of the newly inserted row
        self._update_index_on_insert(table_name, row, row_index)
    
    def select(self, table_name: str, columns: Optional[List[str]] = None, 
               where_clause: Optional[Dict[str, Any]] = None,
               joins: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """
        Select rows from a table, optionally with joins.
        
        Args:
            table_name: Name of the table
            columns: List of column names to select (None for all columns)
            where_clause: Dictionary of column: value filters
            joins: List of join definitions (None for no joins)
            
        Returns:
            List of matching row dictionaries
            
        Raises:
            ValueError: If table doesn't exist or column doesn't exist
        """
        # Handle joins
        if joins:
            return self._select_with_joins(table_name, columns, where_clause, joins)
        
        # Regular select without joins
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
            
            # Check if we can use an index (single equality condition)
            if len(where_clause) == 1:
                col, expected_value = next(iter(where_clause.items()))
                if col in schema["columns"]:
                    # Try to use index
                    col_type = schema["columns"][col]["type"]
                    try:
                        if expected_value is not None and not (isinstance(expected_value, str) and expected_value.upper() == 'NULL'):
                            converted_value = self._validate_value_type(expected_value, col_type)
                            indexed_rows = self._get_indexed_rows(table_name, col, converted_value)
                            
                            if indexed_rows is not None:
                                # Use index for fast lookup
                                for row_idx in indexed_rows:
                                    filtered_rows.append(all_rows[row_idx])
                            else:
                                # Fall back to full table scan
                                for row in all_rows:
                                    # Case-insensitive comparison for TEXT columns
                                    if col_type == "TEXT":
                                        if isinstance(row[col], str) and isinstance(converted_value, str):
                                            if row[col].lower() == converted_value.lower():
                                                filtered_rows.append(row)
                                        elif row[col] == converted_value:
                                            filtered_rows.append(row)
                                    else:
                                        if row[col] == converted_value:
                                            filtered_rows.append(row)
                        else:
                            # NULL comparison - fall back to full scan
                            for row in all_rows:
                                if row[col] is None:
                                    filtered_rows.append(row)
                    except ValueError:
                        # Type conversion failed - fall back to full scan
                        pass
            
            # If index lookup didn't work or multiple conditions, use full scan
            if not filtered_rows and where_clause:
                for row in all_rows:
                    match = True
                    for col, expected_value in where_clause.items():
                        if col not in schema["columns"]:
                            raise ValueError(f"Column '{col}' not found in table '{table_name}'")
                        
                        col_type = schema["columns"][col]["type"]
                        
                        # Check if this is a LIKE operator
                        if isinstance(expected_value, dict) and expected_value.get("operator") == "LIKE":
                            pattern = expected_value.get("value")
                            if not isinstance(pattern, str):
                                match = False
                                break
                            if not self._match_like_pattern(row[col], pattern):
                                match = False
                                break
                        else:
                            # Regular equality comparison
                            try:
                                if expected_value is None or (isinstance(expected_value, str) and expected_value.upper() == 'NULL'):
                                    if row[col] is not None:
                                        match = False
                                        break
                                else:
                                    converted_value = self._validate_value_type(expected_value, col_type)
                                    # Case-insensitive comparison for TEXT columns
                                    if col_type == "TEXT":
                                        if isinstance(row[col], str) and isinstance(converted_value, str):
                                            if row[col].lower() != converted_value.lower():
                                                match = False
                                                break
                                        elif row[col] != converted_value:
                                            match = False
                                            break
                                    else:
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
    
    def _select_with_joins(self, table_name: str, columns: Optional[List[str]], 
                          where_clause: Optional[Dict[str, Any]],
                          joins: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Select rows with JOIN operations using nested loop join.
        
        Args:
            table_name: Name of the first table
            columns: List of column names to select (None for all columns)
            where_clause: Dictionary of column: value filters
            joins: List of join definitions
            
        Returns:
            List of joined row dictionaries
        """
        # Get first table data
        if not self.storage.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")
        
        schema1 = self.storage.get_schema(table_name)
        rows1 = self.storage.get_all_rows(table_name)
        
        # Process each join
        current_rows = rows1
        current_schemas = {table_name: schema1}
        current_table_names = [table_name]
        
        for join_def in joins:
            join_table = join_def["table"]
            on_clause = join_def["on"]
            
            if not self.storage.table_exists(join_table):
                raise ValueError(f"Table '{join_table}' does not exist")
            
            schema2 = self.storage.get_schema(join_table)
            rows2 = self.storage.get_all_rows(join_table)
            current_schemas[join_table] = schema2
            current_table_names.append(join_table)
            
            # Perform nested loop join
            joined_rows = []
            first_table = current_table_names[0]
            
            for row1 in current_rows:
                for row2 in rows2:
                    # Check join condition
                    left_table = on_clause["left_table"]
                    left_col = on_clause["left_column"]
                    right_table = on_clause["right_table"]
                    right_col = on_clause["right_column"]
                    
                    # Get left value - check if row1 has table prefix or not
                    left_value = None
                    if isinstance(row1, dict):
                        # Always try table.column format first (works for both prefixed and non-prefixed rows)
                        prefixed_key = f"{left_table}.{left_col}"
                        if prefixed_key in row1:
                            left_value = row1[prefixed_key]
                        else:
                            # Check if row1 has prefixes (from previous joins)
                            has_prefixes = any('.' in key for key in row1.keys())
                            if has_prefixes:
                                # Row has prefixes, so we must use the prefixed key
                                # If it's not found, the column might not exist in this row
                                left_value = None
                            else:
                                # No prefixes yet, column is from first table
                                left_value = row1.get(left_col)
                    
                    # Get right value from join table
                    right_value = None
                    if isinstance(row2, dict):
                        # Right value should always be from the join table
                        right_value = row2.get(right_col)
                    
                    # Perform join if condition matches
                    if left_value == right_value and left_value is not None:
                        # Combine rows with table prefix to avoid column name conflicts
                        combined_row = {}
                        
                        # Add columns from first table(s) - row1 might be from previous join
                        if isinstance(row1, dict):
                            # Check if row1 already has table prefixes (from previous join)
                            has_prefixes = any('.' in key for key in row1.keys())
                            if has_prefixes:
                                # Already has prefixes, copy as-is
                                for key, value in row1.items():
                                    combined_row[key] = value
                            else:
                                # No prefixes, add them from first table
                                for col_name in schema1["columns"].keys():
                                    combined_row[f"{first_table}.{col_name}"] = row1.get(col_name)
                        else:
                            # row1 is not a dict (shouldn't happen, but handle it)
                            for col_name in schema1["columns"].keys():
                                combined_row[f"{first_table}.{col_name}"] = None
                        
                        # Add columns from joined table with table prefix (always prefix)
                        for col_name in schema2["columns"].keys():
                            combined_row[f"{join_table}.{col_name}"] = row2.get(col_name) if isinstance(row2, dict) else None
                        
                        joined_rows.append(combined_row)
            
            current_rows = joined_rows
            table_name = join_table  # For next iteration
        
        # Apply WHERE clause if present
        if where_clause:
            filtered_rows = []
            for row in current_rows:
                match = True
                for col, expected_value in where_clause.items():
                    # Handle table.column notation
                    if '.' in col:
                        table_col = col
                    else:
                        # Try to find column in any table
                        table_col = None
                        for tbl_name in current_table_names:
                            if tbl_name in current_schemas and col in current_schemas[tbl_name]["columns"]:
                                table_col = f"{tbl_name}.{col}"
                                break
                        
                        if not table_col:
                            # Try without prefix
                            table_col = col
                    
                    row_value = row.get(table_col) if table_col in row else row.get(col)
                    
                    if row_value != expected_value:
                        match = False
                        break
                
                if match:
                    filtered_rows.append(row)
            current_rows = filtered_rows
        
        # Select only requested columns
        result = []
        for row in current_rows:
            if columns is None:
                # Return all columns, but simplify names if possible
                simplified_row = {}
                for key, value in row.items():
                    # Remove table prefix if column name is unique
                    if '.' in key:
                        col_name = key.split('.', 1)[1]
                        # Check if this column name appears in multiple tables
                        count = sum(1 for k in row.keys() if k.endswith(f".{col_name}"))
                        if count == 1:
                            simplified_row[col_name] = value
                        else:
                            simplified_row[key] = value
                    else:
                        simplified_row[key] = value
                result.append(simplified_row)
            else:
                result_row = {}
                for col in columns:
                    # Handle table.column notation
                    if '.' in col:
                        if col in row:
                            result_row[col] = row[col]
                    else:
                        # Try to find column
                        found = False
                        for tbl_name in current_table_names:
                            table_col = f"{tbl_name}.{col}"
                            if table_col in row:
                                result_row[col] = row[table_col]
                                found = True
                                break
                        
                        if not found and col in row:
                            result_row[col] = row[col]
                
                result.append(result_row)
        
        return result
    
    def update(self, table_name: str, assignments: Dict[str, Any], 
               where_clause: Optional[Dict[str, Any]] = None) -> int:
        """
        Update rows in a table.
        
        Args:
            table_name: Name of the table
            assignments: Dictionary of column: new_value pairs
            where_clause: Dictionary of column: value filters (None to update all rows)
            
        Returns:
            Number of rows updated
            
        Raises:
            ValueError: If table doesn't exist, column doesn't exist, or constraint violation
        """
        if not self.storage.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")
        
        schema = self.storage.get_schema(table_name)
        all_rows = self.storage.get_all_rows(table_name)
        
        # Validate that all assignment columns exist
        for col_name in assignments.keys():
            if col_name not in schema["columns"]:
                raise ValueError(f"Column '{col_name}' not found in table '{table_name}'")
        
        # Find rows to update
        rows_to_update_indices = []
        for i, row in enumerate(all_rows):
            if where_clause is None:
                # Update all rows
                rows_to_update_indices.append(i)
            else:
                # Check if row matches WHERE clause
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
                            # Case-insensitive comparison for TEXT columns
                            if col_type == "TEXT":
                                if isinstance(row[col], str) and isinstance(converted_value, str):
                                    if row[col].lower() != converted_value.lower():
                                        match = False
                                        break
                                elif row[col] != converted_value:
                                    match = False
                                    break
                            else:
                                if row[col] != converted_value:
                                    match = False
                                    break
                    except ValueError:
                        match = False
                        break
                
                if match:
                    rows_to_update_indices.append(i)
        
        if not rows_to_update_indices:
            return 0  # No rows matched
        
        # Create updated rows
        updated_rows = []
        for i, row in enumerate(all_rows):
            if i in rows_to_update_indices:
                # Create a copy of the row
                updated_row = row.copy()
                
                # Apply assignments
                for col_name, new_value in assignments.items():
                    col_def = schema["columns"][col_name]
                    
                    # Handle NULL
                    if new_value is None or (isinstance(new_value, str) and new_value.upper() == 'NULL'):
                        if not col_def["nullable"]:
                            raise ValueError(f"Column '{col_name}' cannot be NULL")
                        updated_row[col_name] = None
                    else:
                        # Validate and convert type
                        converted_value = self._validate_value_type(new_value, col_def["type"])
                        updated_row[col_name] = converted_value
                
                # Check constraints before updating
                self._validate_row_constraints(table_name, updated_row, schema, exclude_indices=[i])
                
                updated_rows.append(updated_row)
            else:
                updated_rows.append(row)
        
        # Save updated rows
        self.storage.update_table_data(table_name, updated_rows)
        
        # Update indexes for changed rows
        for i in rows_to_update_indices:
            old_row = all_rows[i]
            new_row = updated_rows[i]
            self._update_index_on_update(table_name, old_row, new_row, i)
        
        return len(rows_to_update_indices)
    
    def delete(self, table_name: str, where_clause: Optional[Dict[str, Any]] = None) -> int:
        """
        Delete rows from a table.
        
        Args:
            table_name: Name of the table
            where_clause: Dictionary of column: value filters (None to delete all rows)
            
        Returns:
            Number of rows deleted
            
        Raises:
            ValueError: If table doesn't exist or column doesn't exist
        """
        if not self.storage.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")
        
        schema = self.storage.get_schema(table_name)
        all_rows = self.storage.get_all_rows(table_name)
        
        # Find rows to delete first (before checking foreign keys)
        rows_to_delete = []
        rows_to_keep = []
        
        for row in all_rows:
            if where_clause is None:
                # Delete all rows (don't add to keep list)
                rows_to_delete.append(row)
            else:
                # Check if row matches WHERE clause
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
                            # Case-insensitive comparison for TEXT columns
                            if col_type == "TEXT":
                                if isinstance(row[col], str) and isinstance(converted_value, str):
                                    if row[col].lower() != converted_value.lower():
                                        match = False
                                        break
                                elif row[col] != converted_value:
                                    match = False
                                    break
                            else:
                                if row[col] != converted_value:
                                    match = False
                                    break
                    except ValueError:
                        match = False
                        break
                
                if match:
                    rows_to_delete.append(row)
                else:
                    rows_to_keep.append(row)
        
        # Check foreign key constraints and handle CASCADE deletes
        if rows_to_delete:
            self._handle_foreign_key_constraints(table_name, rows_to_delete, schema)
        
        # Save remaining rows
        self.storage.update_table_data(table_name, rows_to_keep)
        
        # Rebuild indexes after delete (simpler than tracking deletions)
        deleted_count = len(rows_to_delete)
        if deleted_count > 0:
            self._rebuild_indexes_after_delete(table_name)
        
        return deleted_count
    
    def _validate_row_constraints(self, table_name: str, row: Dict[str, Any], 
                                   schema: Dict[str, Any], exclude_indices: List[int] = None) -> None:
        """
        Validate constraints for a row (used in UPDATE).
        
        Args:
            table_name: Name of the table
            row: Row dictionary to validate
            schema: Table schema
            exclude_indices: List of row indices to exclude from duplicate checks (for UPDATE)
            
        Raises:
            ValueError: If constraint violation detected
        """
        exclude_indices = exclude_indices or []
        
        # Check primary key constraint
        if schema["primary_key"]:
            pk_col = schema["primary_key"]
            pk_value = row[pk_col]
            
            if pk_value is None:
                raise ValueError(f"Primary key column '{pk_col}' cannot be NULL")
            
            # Check for duplicate primary key (excluding current row in UPDATE)
            existing_rows = self.storage.get_all_rows(table_name)
            for idx, existing_row in enumerate(existing_rows):
                if idx not in exclude_indices and existing_row[pk_col] == pk_value:
                    raise ValueError(
                        f"Primary key violation: duplicate value '{pk_value}' in column '{pk_col}'"
                    )
        
        # Check unique constraints
        for col_name, col_def in schema["columns"].items():
            if col_def["unique"] and row[col_name] is not None:
                existing_rows = self.storage.get_all_rows(table_name)
                for idx, existing_row in enumerate(existing_rows):
                    if idx not in exclude_indices and existing_row[col_name] == row[col_name]:
                        raise ValueError(
                            f"Unique constraint violation: duplicate value '{row[col_name]}' in column '{col_name}'"
                        )
    
    def _build_all_indexes(self, table_name: str) -> None:
        """
        Build all indexes for a table from existing data.
        
        Args:
            table_name: Name of the table
        """
        schema = self.storage.get_schema(table_name)
        all_rows = self.storage.get_all_rows(table_name)
        indexes = {}
        
        # Build each index defined in schema
        for index_name, index_def in schema.get("indexes", {}).items():
            col_name = index_def["column"]
            index_data = {}
            
            # Build hash map: value -> [row_indices]
            for row_idx, row in enumerate(all_rows):
                value = row.get(col_name)
                if value is not None:
                    # Convert value to hashable type for dictionary key
                    if isinstance(value, (list, dict)):
                        value = str(value)  # Convert complex types to string
                    if value not in index_data:
                        index_data[value] = []
                    index_data[value].append(row_idx)
            
            indexes[index_name] = index_data
        
        # Save indexes
        self.storage.update_indexes(table_name, indexes)
    
    def _get_indexed_rows(self, table_name: str, column: str, value: Any) -> Optional[List[int]]:
        """
        Get row indices using an index if available.
        
        Args:
            table_name: Name of the table
            column: Column name to look up
            value: Value to search for
            
        Returns:
            List of row indices if index exists and value found, None otherwise
        """
        schema = self.storage.get_schema(table_name)
        indexes = self.storage.get_indexes(table_name)
        
        # Find index for this column
        for index_name, index_def in schema.get("indexes", {}).items():
            if index_def["column"] == column:
                # Use index for lookup
                index_data = indexes.get(index_name, {})
                # Convert value to match index key type
                if isinstance(value, (list, dict)):
                    value = str(value)
                return index_data.get(value)
        
        return None
    
    def _update_index_on_insert(self, table_name: str, row: Dict[str, Any], row_index: int) -> None:
        """
        Update indexes after inserting a row.
        
        Args:
            table_name: Name of the table
            row: The inserted row
            row_index: Index of the inserted row
        """
        schema = self.storage.get_schema(table_name)
        indexes = self.storage.get_indexes(table_name)
        
        # Update each index
        for index_name, index_def in schema.get("indexes", {}).items():
            col_name = index_def["column"]
            value = row.get(col_name)
            
            if value is not None:
                # Convert value to hashable type
                if isinstance(value, (list, dict)):
                    value = str(value)
                
                if index_name not in indexes:
                    indexes[index_name] = {}
                
                if value not in indexes[index_name]:
                    indexes[index_name][value] = []
                
                indexes[index_name][value].append(row_index)
        
        # Save updated indexes
        self.storage.update_indexes(table_name, indexes)
    
    def _update_index_on_update(self, table_name: str, old_row: Dict[str, Any], 
                                new_row: Dict[str, Any], row_index: int) -> None:
        """
        Update indexes after updating a row.
        
        Args:
            table_name: Name of the table
            old_row: The row before update
            new_row: The row after update
            row_index: Index of the updated row
        """
        schema = self.storage.get_schema(table_name)
        indexes = self.storage.get_indexes(table_name)
        
        # Update each index
        for index_name, index_def in schema.get("indexes", {}).items():
            col_name = index_def["column"]
            old_value = old_row.get(col_name)
            new_value = new_row.get(col_name)
            
            # If indexed column changed, update index
            if old_value != new_value:
                # Remove old value from index
                if old_value is not None:
                    if isinstance(old_value, (list, dict)):
                        old_value = str(old_value)
                    if index_name in indexes and old_value in indexes[index_name]:
                        if row_index in indexes[index_name][old_value]:
                            indexes[index_name][old_value].remove(row_index)
                        if not indexes[index_name][old_value]:
                            del indexes[index_name][old_value]
                
                # Add new value to index
                if new_value is not None:
                    if isinstance(new_value, (list, dict)):
                        new_value = str(new_value)
                    if index_name not in indexes:
                        indexes[index_name] = {}
                    if new_value not in indexes[index_name]:
                        indexes[index_name][new_value] = []
                    indexes[index_name][new_value].append(row_index)
        
        # Save updated indexes
        self.storage.update_indexes(table_name, indexes)
    
    def _rebuild_indexes_after_delete(self, table_name: str) -> None:
        """
        Rebuild all indexes after delete operation (simpler than tracking deletions).
        
        Args:
            table_name: Name of the table
        """
        self._build_all_indexes(table_name)
    
    def _handle_foreign_key_constraints(self, table_name: str, rows_to_delete: List[Dict[str, Any]], 
                                        schema: Dict[str, Any]) -> None:
        """
        Handle foreign key constraints before deletion.
        - RESTRICT: Prevents deletion if child records exist
        - CASCADE: Automatically deletes child records
        
        Args:
            table_name: Name of the table being deleted from
            rows_to_delete: List of rows to be deleted
            schema: Schema of the table being deleted from
            
        Raises:
            ValueError: If foreign key constraint violation detected (RESTRICT mode)
        """
        # Get primary key column of the table being deleted from
        pk_column = schema.get("primary_key")
        if not pk_column:
            # If no primary key, we can't enforce foreign keys reliably
            return
        
        # Get all tables to check for foreign keys
        all_tables = self.list_tables()
        
        # Collect primary key values being deleted
        pk_values_to_delete = {row[pk_column] for row in rows_to_delete if pk_column in row}
        
        if not pk_values_to_delete:
            return
        
        # Check each table for foreign keys pointing to this table
        for child_table_name in all_tables:
            if child_table_name == table_name:
                continue
            
            try:
                child_schema = self.storage.get_schema(child_table_name)
                child_foreign_keys = child_schema.get("foreign_keys", [])
                
                # Find foreign keys that reference the table being deleted from
                for fk in child_foreign_keys:
                    if fk.get("references_table") == table_name and fk.get("references_column") == pk_column:
                        # This child table has a foreign key pointing to the table being deleted
                        fk_column = fk.get("column")
                        on_delete = fk.get("on_delete", "RESTRICT")
                        
                        if not fk_column:
                            continue
                        
                        # Check if any child records reference the rows being deleted
                        child_rows = self.storage.get_all_rows(child_table_name)
                        referencing_rows = [row for row in child_rows if row.get(fk_column) in pk_values_to_delete]
                        
                        if referencing_rows:
                            if on_delete == "CASCADE":
                                # Automatically delete child records
                                self._cascade_delete(child_table_name, fk_column, pk_values_to_delete)
                            else:
                                # RESTRICT: Prevent deletion
                                raise ValueError(
                                    f"Cannot delete or update a parent row: a foreign key constraint fails "
                                    f"({child_table_name}.{fk_column} -> {table_name}.{pk_column})"
                                )
            except ValueError as e:
                # Re-raise foreign key constraint errors
                error_msg = str(e)
                if "Foreign key constraint violation" in error_msg or "Cannot delete" in error_msg:
                    raise
                # Table might not exist or other error, skip it
                continue
    
    def _cascade_delete(self, child_table_name: str, fk_column: str, pk_values_to_delete: set) -> None:
        """
        Cascade delete: Delete child records that reference the deleted parent records.
        
        Args:
            child_table_name: Name of the child table
            fk_column: Foreign key column name in child table
            pk_values_to_delete: Set of primary key values being deleted from parent
        """
        all_child_rows = self.storage.get_all_rows(child_table_name)
        rows_to_keep = [row for row in all_child_rows if row.get(fk_column) not in pk_values_to_delete]
        
        # Update child table data
        self.storage.update_table_data(child_table_name, rows_to_keep)
        
        # Rebuild indexes for child table
        self._rebuild_indexes_after_delete(child_table_name)
        
        # Recursively check if this child table has its own children with CASCADE
        child_schema = self.storage.get_schema(child_table_name)
        child_pk = child_schema.get("primary_key")
        if child_pk:
            deleted_child_pk_values = {
                row[child_pk] for row in all_child_rows 
                if row.get(fk_column) in pk_values_to_delete and child_pk in row
            }
            if deleted_child_pk_values:
                self._handle_foreign_key_constraints(child_table_name, 
                    [row for row in all_child_rows if row.get(child_pk) in deleted_child_pk_values],
                    child_schema)
    
    @staticmethod
    def _match_like_pattern(text: str, pattern: str) -> bool:
        """
        Match text against a LIKE pattern with % and _ wildcards.
        
        Args:
            text: Text to match
            pattern: LIKE pattern with % (any sequence) and _ (single character)
            
        Returns:
            True if text matches pattern, False otherwise
        """
        if not isinstance(text, str):
            return False
        
        # Convert SQL LIKE pattern to regex
        regex_pattern = '^'
        i = 0
        while i < len(pattern):
            if pattern[i] == '%':
                regex_pattern += '.*'
            elif pattern[i] == '_':
                regex_pattern += '.'
            else:
                regex_pattern += re.escape(pattern[i])
            i += 1
        regex_pattern += '$'
        
        try:
            return bool(re.match(regex_pattern, text, re.IGNORECASE))
        except re.error:
            return False
    
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
