"""
REPL (Read-Eval-Print Loop) for interactive database commands.
"""

from typing import Optional, Dict, Any
from .engine import DatabaseEngine
from .parser import SQLParser


class REPL:
    """Interactive REPL for executing SQL-like commands."""
    
    def __init__(self, engine: Optional[DatabaseEngine] = None):
        """
        Initialize the REPL.
        
        Args:
            engine: DatabaseEngine instance (creates new one if not provided)
        """
        self.engine = engine or DatabaseEngine()
        self.parser = SQLParser()
        self.prompt = "mydb> "
    
    def run(self) -> None:
        """Start the interactive REPL loop."""
        print("Welcome to MyDB RDBMS!")
        print("Type 'exit' or 'quit' to exit.")
        print("Type 'help' for available commands.\n")
        
        while True:
            try:
                command = input(self.prompt).strip()
                
                if not command:
                    continue
                
                # Handle exit commands
                if command.lower() in ('exit', 'quit', 'q'):
                    print("Goodbye!")
                    break
                
                # Handle help
                if command.lower() == 'help':
                    self._print_help()
                    continue
                
                # Handle list tables
                if command.lower() == 'list tables' or command.lower() == '\\dt':
                    self._list_tables()
                    continue
                
                # Execute SQL command
                result = self._execute_command(command)
                self._display_result(result)
                
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")
    
    def _execute_command(self, command: str) -> Dict[str, Any]:
        """
        Execute a SQL command.
        
        Args:
            command: SQL command string
            
        Returns:
            Result dictionary with 'status' and optional 'data'
        """
        parsed = self.parser.parse(command)
        cmd_type = parsed["type"]
        
        if cmd_type == "CREATE_TABLE":
            self.engine.create_table(
                parsed["table_name"],
                parsed["columns"]
            )
            return {"status": "OK", "message": f"Table '{parsed['table_name']}' created"}
        
        elif cmd_type == "INSERT":
            self.engine.insert(
                parsed["table_name"],
                parsed["values"]
            )
            return {"status": "OK", "message": "Row inserted"}
        
        elif cmd_type == "SELECT":
            rows = self.engine.select(
                parsed["table_name"],
                parsed["columns"],
                parsed["where_clause"]
            )
            return {"status": "OK", "data": rows}
        
        else:
            raise ValueError(f"Unsupported command type: {cmd_type}")
    
    def _display_result(self, result: Dict[str, Any]) -> None:
        """Display command execution result."""
        if result["status"] == "OK":
            if "message" in result:
                print(result["message"])
            elif "data" in result:
                rows = result["data"]
                if not rows:
                    print("(0 rows)")
                else:
                    for row in rows:
                        # Format: col1=value1 col2=value2 ...
                        row_str = " ".join(f"{k}={v}" for k, v in row.items())
                        print(row_str)
    
    def _print_help(self) -> None:
        """Print help information."""
        help_text = """
Available commands:
  CREATE TABLE table_name (column_name TYPE [PRIMARY KEY] [UNIQUE], ...)
  INSERT INTO table_name VALUES (value1, value2, ...)
  SELECT * FROM table_name [WHERE column=value]
  SELECT col1, col2 FROM table_name [WHERE column=value]
  list tables  (or \\dt) - List all tables
  help         - Show this help message
  exit/quit    - Exit the REPL

Supported data types: INT, TEXT, BOOL, FLOAT
        """
        print(help_text)
    
    def _list_tables(self) -> None:
        """List all tables."""
        tables = self.engine.list_tables()
        if not tables:
            print("No tables found.")
        else:
            print("Tables:")
            for table in tables:
                print(f"  - {table}")
