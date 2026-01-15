"""
SQL-like command parser.
Parses CREATE TABLE, INSERT, SELECT, UPDATE, DELETE commands.
"""

import re
from typing import Dict, List, Any, Optional, Tuple


class SQLParser:
    """Parser for SQL-like commands."""
    
    def parse(self, command: str) -> Dict[str, Any]:
        """
        Parse a SQL-like command.
        
        Args:
            command: SQL command string
            
        Returns:
            Dictionary with 'type' and command-specific data
            
        Raises:
            ValueError: If command syntax is invalid
        """
        command = command.strip()
        if not command:
            raise ValueError("Empty command")
        
        # Remove trailing semicolon if present
        if command.endswith(';'):
            command = command[:-1].strip()
        
        # Normalize whitespace (collapse multiple spaces to single space)
        # But preserve spaces inside quoted strings
        normalized = []
        in_string = False
        string_char = None
        i = 0
        
        while i < len(command):
            char = command[i]
            
            if not in_string and char in ("'", '"'):
                in_string = True
                string_char = char
                normalized.append(char)
            elif in_string and char == string_char:
                # Check if escaped
                if i > 0 and command[i-1] == '\\':
                    normalized.append(char)
                else:
                    in_string = False
                    string_char = None
                    normalized.append(char)
            elif not in_string and char.isspace():
                # Collapse whitespace
                if normalized and normalized[-1] != ' ':
                    normalized.append(' ')
            else:
                normalized.append(char)
            
            i += 1
        
        command = ''.join(normalized).strip()
        command_upper = command.upper()
        
        # Parse CREATE TABLE
        if command_upper.startswith('CREATE TABLE'):
            return self._parse_create_table(command)
        
        # Parse INSERT INTO
        elif command_upper.startswith('INSERT INTO'):
            return self._parse_insert(command)
        
        # Parse SELECT
        elif command_upper.startswith('SELECT'):
            return self._parse_select(command)
        
        # Parse UPDATE
        elif command_upper.startswith('UPDATE'):
            return self._parse_update(command)
        
        # Parse DELETE
        elif command_upper.startswith('DELETE'):
            return self._parse_delete(command)
        
        else:
            raise ValueError(f"Unknown command: {command.split()[0] if command.split() else 'empty'}")
    
    def _parse_create_table(self, command: str) -> Dict[str, Any]:
        """
        Parse CREATE TABLE command.
        
        Format: CREATE TABLE table_name (column_name TYPE [PRIMARY KEY] [UNIQUE], ...)
        """
        # Match: CREATE TABLE table_name (columns...)
        pattern = r'CREATE\s+TABLE\s+(\w+)\s*\((.*)\)'
        match = re.match(pattern, command, re.IGNORECASE)
        
        if not match:
            raise ValueError("Invalid CREATE TABLE syntax. Expected: CREATE TABLE table_name (column_name TYPE [PRIMARY KEY] [UNIQUE], ...)")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        
        # Parse columns
        columns = []
        # Split by comma, but be careful with parentheses
        column_defs = self._split_column_definitions(columns_str)
        
        for col_def in column_defs:
            col_def = col_def.strip()
            if not col_def:
                continue
            
            # Parse: column_name TYPE [PRIMARY KEY] [AUTO_INCREMENT] [UNIQUE]
            parts = col_def.split()
            if len(parts) < 2:
                raise ValueError(f"Invalid column definition: '{col_def}'. Expected: column_name TYPE [PRIMARY KEY] [AUTO_INCREMENT] [UNIQUE]")
            
            col_name = parts[0]
            col_type = parts[1].upper()
            
            # Check for PRIMARY KEY, AUTO_INCREMENT, and UNIQUE keywords
            is_primary = 'PRIMARY' in col_def.upper() and 'KEY' in col_def.upper()
            is_auto_increment = 'AUTO_INCREMENT' in col_def.upper() or 'AUTOINCREMENT' in col_def.upper()
            is_unique = 'UNIQUE' in col_def.upper() and not is_primary
            
            columns.append({
                "name": col_name,
                "type": col_type,
                "primary_key": is_primary,
                "unique": is_unique,
                "auto_increment": is_auto_increment
            })
        
        return {
            "type": "CREATE_TABLE",
            "table_name": table_name,
            "columns": columns
        }
    
    def _split_column_definitions(self, columns_str: str) -> List[str]:
        """Split column definitions by comma, handling edge cases."""
        columns = []
        current = ""
        paren_depth = 0
        
        for char in columns_str:
            if char == '(':
                paren_depth += 1
                current += char
            elif char == ')':
                paren_depth -= 1
                current += char
            elif char == ',' and paren_depth == 0:
                if current.strip():
                    columns.append(current.strip())
                current = ""
            else:
                current += char
        
        if current.strip():
            columns.append(current.strip())
        
        return columns
    
    def _parse_insert(self, command: str) -> Dict[str, Any]:
        """
        Parse INSERT INTO command.
        
        Format: INSERT INTO table_name VALUES (value1, value2, ...)
        """
        # Match: INSERT INTO table_name VALUES (values...)
        pattern = r'INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.*)\)'
        match = re.match(pattern, command, re.IGNORECASE)
        
        if not match:
            raise ValueError("Invalid INSERT syntax. Expected: INSERT INTO table_name VALUES (value1, value2, ...)")
        
        table_name = match.group(1)
        values_str = match.group(2)
        
        # Parse values
        values = self._parse_values(values_str)
        
        return {
            "type": "INSERT",
            "table_name": table_name,
            "values": values
        }
    
    def _parse_values(self, values_str: str) -> List[Any]:
        """Parse comma-separated values, handling strings with commas."""
        values = []
        current = ""
        in_string = False
        string_char = None
        
        i = 0
        while i < len(values_str):
            char = values_str[i]
            
            if not in_string and char in ("'", '"'):
                in_string = True
                string_char = char
                current += char
            elif in_string and char == string_char:
                # Check if escaped
                if i > 0 and values_str[i-1] == '\\':
                    current += char
                else:
                    in_string = False
                    string_char = None
                    current += char
            elif not in_string and char == ',':
                if current.strip():
                    values.append(self._parse_single_value(current.strip()))
                current = ""
            else:
                current += char
            
            i += 1
        
        if current.strip():
            values.append(self._parse_single_value(current.strip()))
        
        return values
    
    def _parse_single_value(self, value_str: str) -> Any:
        """Parse a single value, handling strings, numbers, NULL, booleans."""
        value_str = value_str.strip()
        
        # NULL
        if value_str.upper() == 'NULL':
            return None
        
        # String (quoted)
        if (value_str.startswith("'") and value_str.endswith("'")) or \
           (value_str.startswith('"') and value_str.endswith('"')):
            return value_str[1:-1]
        
        # Boolean (true/false)
        if value_str.upper() in ('TRUE', 'FALSE'):
            return value_str.upper() == 'TRUE'
        
        # Number (int or float)
        try:
            if '.' in value_str:
                return float(value_str)
            else:
                return int(value_str)
        except ValueError:
            # Return as string if not a number
            return value_str
    
    def _parse_select(self, command: str) -> Dict[str, Any]:
        """
        Parse SELECT command.
        
        Format: SELECT * FROM table_name [WHERE column=value]
        Format: SELECT col1, col2 FROM table_name [WHERE column=value]
        Format: SELECT * FROM table1 JOIN table2 ON table1.id = table2.foreign_id [WHERE ...]
        """
        # Check for JOIN syntax
        if 'JOIN' in command.upper():
            return self._parse_select_with_join(command)
        
        # Match: SELECT columns FROM table_name [WHERE conditions]
        pattern = r'SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$'
        match = re.match(pattern, command, re.IGNORECASE)
        
        if not match:
            raise ValueError("Invalid SELECT syntax. Expected: SELECT * FROM table_name [WHERE column=value]")
        
        columns_str = match.group(1).strip()
        table_name = match.group(2)
        where_clause_str = match.group(3)
        
        # Parse columns
        if columns_str == '*':
            columns = None  # All columns
        else:
            columns = [col.strip() for col in columns_str.split(',')]
        
        # Parse WHERE clause
        where_clause = None
        if where_clause_str:
            where_clause = self._parse_where_clause(where_clause_str)
        
        return {
            "type": "SELECT",
            "table_name": table_name,
            "columns": columns,
            "where_clause": where_clause,
            "joins": None
        }
    
    def _parse_select_with_join(self, command: str) -> Dict[str, Any]:
        """
        Parse SELECT command with JOIN(s).
        
        Format: SELECT * FROM table1 JOIN table2 ON table1.col = table2.col [JOIN table3 ON ...] [WHERE ...]
        """
        # Extract SELECT columns and FROM clause
        select_match = re.match(r'SELECT\s+(.+?)\s+FROM\s+(.+?)(?:\s+WHERE\s+(.+))?$', command, re.IGNORECASE)
        if not select_match:
            raise ValueError("Invalid JOIN syntax. Expected: SELECT * FROM table1 JOIN table2 ON table1.col = table2.col [WHERE ...]")
        
        columns_str = select_match.group(1).strip()
        from_clause = select_match.group(2).strip()
        where_clause_str = select_match.group(3)
        
        # Parse columns
        if columns_str == '*':
            columns = None  # All columns
        else:
            columns = [col.strip() for col in columns_str.split(',')]
        
        # Parse FROM clause: table1 JOIN table2 ON ... [JOIN table3 ON ...]
        # Split by JOIN (case insensitive) but preserve the ON clauses
        parts = re.split(r'\s+JOIN\s+', from_clause, flags=re.IGNORECASE)
        if len(parts) < 2:
            raise ValueError("Invalid JOIN syntax. Expected at least one JOIN")
        
        table1_name = parts[0].strip()
        joins = []
        
        # Parse each JOIN
        for i in range(1, len(parts)):
            join_part = parts[i]
            # Find the ON clause - it should be followed by either another JOIN or WHERE or end
            on_match = re.match(r'(\w+)\s+ON\s+(.+?)(?:\s+JOIN\s+|\s+WHERE\s+|$)', join_part, re.IGNORECASE)
            if not on_match:
                raise ValueError(f"Invalid JOIN syntax in: {join_part}")
            
            table2_name = on_match.group(1).strip()
            on_clause_str = on_match.group(2).strip()
            
            # Parse ON clause
            on_clause = self._parse_on_clause(on_clause_str)
            
            joins.append({
                "type": "INNER",  # Default to INNER JOIN
                "table": table2_name,
                "on": on_clause
            })
        
        # Parse WHERE clause if present
        where_clause = None
        if where_clause_str:
            where_clause = self._parse_where_clause(where_clause_str)
        
        return {
            "type": "SELECT",
            "table_name": table1_name,
            "columns": columns,
            "where_clause": where_clause,
            "joins": joins
        }
    
    def _parse_on_clause(self, on_str: str) -> Dict[str, str]:
        """
        Parse ON clause for JOIN.
        
        Format: table1.column = table2.column
        Returns: {"left_table": "table1", "left_column": "column", 
                  "right_table": "table2", "right_column": "column"}
        """
        on_str = on_str.strip()
        
        # Match: table1.col = table2.col
        if '=' not in on_str:
            raise ValueError(f"Invalid ON clause: '{on_str}'. Expected: table1.column = table2.column")
        
        parts = on_str.split('=', 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid ON clause: '{on_str}'. Expected: table1.column = table2.column")
        
        left_expr = parts[0].strip()
        right_expr = parts[1].strip()
        
        # Parse table.column format
        def parse_table_column(expr: str) -> tuple:
            if '.' not in expr:
                raise ValueError(f"Invalid expression in ON clause: '{expr}'. Expected: table.column")
            parts = expr.split('.', 1)
            return parts[0].strip(), parts[1].strip()
        
        left_table, left_column = parse_table_column(left_expr)
        right_table, right_column = parse_table_column(right_expr)
        
        return {
            "left_table": left_table,
            "left_column": left_column,
            "right_table": right_table,
            "right_column": right_column
        }
    
    def _parse_where_clause(self, where_str: str) -> Dict[str, Any]:
        """
        Parse WHERE clause.
        
        Format: column=value or column LIKE 'pattern' or column!=value, etc.
        Supports equality (=) and LIKE operator with wildcards (% and _)
        """
        where_str = where_str.strip()
        
        # LIKE operator (case-insensitive)
        if ' LIKE ' in where_str.upper():
            parts = re.split(r'\s+LIKE\s+', where_str, flags=re.IGNORECASE)
            if len(parts) == 2:
                col = parts[0].strip()
                pattern = self._parse_single_value(parts[1].strip())
                return {col: {"operator": "LIKE", "value": pattern}}
        
        # Simple equality: column=value
        if '=' in where_str:
            parts = where_str.split('=', 1)
            if len(parts) == 2:
                col = parts[0].strip()
                value = self._parse_single_value(parts[1].strip())
                return {col: value}
        
        raise ValueError(f"Unsupported WHERE clause format: '{where_str}'. Use: column=value or column LIKE 'pattern'")
    
    def _parse_update(self, command: str) -> Dict[str, Any]:
        """
        Parse UPDATE command.
        
        Format: UPDATE table_name SET column=value [, column=value ...] [WHERE column=value]
        """
        # Match: UPDATE table_name SET assignments [WHERE conditions]
        pattern = r'UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$'
        match = re.match(pattern, command, re.IGNORECASE)
        
        if not match:
            raise ValueError("Invalid UPDATE syntax. Expected: UPDATE table_name SET column=value [WHERE column=value]")
        
        table_name = match.group(1)
        set_clause = match.group(2).strip()
        where_clause_str = match.group(3)
        
        # Parse SET clause: column=value, column=value, ...
        assignments = {}
        # Split by comma, but be careful with quoted strings
        set_parts = self._split_by_comma_outside_quotes(set_clause)
        
        for assignment in set_parts:
            assignment = assignment.strip()
            if '=' not in assignment:
                raise ValueError(f"Invalid assignment: '{assignment}'. Expected: column=value")
            
            parts = assignment.split('=', 1)
            if len(parts) != 2:
                raise ValueError(f"Invalid assignment: '{assignment}'. Expected: column=value")
            
            col_name = parts[0].strip()
            value_str = parts[1].strip()
            value = self._parse_single_value(value_str)
            assignments[col_name] = value
        
        if not assignments:
            raise ValueError("UPDATE must specify at least one column to update")
        
        # Parse WHERE clause if present
        where_clause = None
        if where_clause_str:
            where_clause = self._parse_where_clause(where_clause_str)
        
        return {
            "type": "UPDATE",
            "table_name": table_name,
            "assignments": assignments,
            "where_clause": where_clause
        }
    
    def _split_by_comma_outside_quotes(self, text: str) -> List[str]:
        """Split text by comma, but preserve commas inside quoted strings."""
        parts = []
        current = ""
        in_string = False
        string_char = None
        
        i = 0
        while i < len(text):
            char = text[i]
            
            if not in_string and char in ("'", '"'):
                in_string = True
                string_char = char
                current += char
            elif in_string and char == string_char:
                # Check if escaped
                if i > 0 and text[i-1] == '\\':
                    current += char
                else:
                    in_string = False
                    string_char = None
                    current += char
            elif not in_string and char == ',':
                if current.strip():
                    parts.append(current.strip())
                current = ""
            else:
                current += char
            
            i += 1
        
        if current.strip():
            parts.append(current.strip())
        
        return parts
    
    def _parse_delete(self, command: str) -> Dict[str, Any]:
        """
        Parse DELETE command.
        
        Format: DELETE FROM table_name [WHERE column=value]
        """
        # Match: DELETE FROM table_name [WHERE conditions]
        pattern = r'DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$'
        match = re.match(pattern, command, re.IGNORECASE)
        
        if not match:
            raise ValueError("Invalid DELETE syntax. Expected: DELETE FROM table_name [WHERE column=value]")
        
        table_name = match.group(1)
        where_clause_str = match.group(2)
        
        # Parse WHERE clause if present
        where_clause = None
        if where_clause_str:
            where_clause = self._parse_where_clause(where_clause_str)
        
        return {
            "type": "DELETE",
            "table_name": table_name,
            "where_clause": where_clause
        }
