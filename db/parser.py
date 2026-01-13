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
            
            # Parse: column_name TYPE [PRIMARY KEY] [UNIQUE]
            parts = col_def.split()
            if len(parts) < 2:
                raise ValueError(f"Invalid column definition: '{col_def}'. Expected: column_name TYPE [PRIMARY KEY] [UNIQUE]")
            
            col_name = parts[0]
            col_type = parts[1].upper()
            
            # Check for PRIMARY KEY and UNIQUE keywords
            is_primary = 'PRIMARY' in col_def.upper() and 'KEY' in col_def.upper()
            is_unique = 'UNIQUE' in col_def.upper() and not is_primary
            
            columns.append({
                "name": col_name,
                "type": col_type,
                "primary_key": is_primary,
                "unique": is_unique
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
        """
        # Match: SELECT columns FROM table_name [WHERE conditions]
        pattern = r'SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?'
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
            "where_clause": where_clause
        }
    
    def _parse_where_clause(self, where_str: str) -> Dict[str, Any]:
        """
        Parse WHERE clause.
        
        Format: column=value or column!=value or column>value, etc.
        For Phase 1, we'll support simple equality: column=value
        """
        where_str = where_str.strip()
        
        # Simple equality: column=value
        if '=' in where_str:
            parts = where_str.split('=', 1)
            if len(parts) == 2:
                col = parts[0].strip()
                value = self._parse_single_value(parts[1].strip())
                return {col: value}
        
        # For Phase 1, we'll keep it simple with just equality
        # More operators will be added in later phases
        raise ValueError(f"Unsupported WHERE clause format: '{where_str}'. Use: column=value")
    
    def _parse_update(self, command: str) -> Dict[str, Any]:
        """
        Parse UPDATE command.
        
        Format: UPDATE table_name SET column=value WHERE column=value
        """
        # This will be implemented in Phase 3
        raise ValueError("UPDATE command not yet implemented")
    
    def _parse_delete(self, command: str) -> Dict[str, Any]:
        """
        Parse DELETE command.
        
        Format: DELETE FROM table_name WHERE column=value
        """
        # This will be implemented in Phase 3
        raise ValueError("DELETE command not yet implemented")
