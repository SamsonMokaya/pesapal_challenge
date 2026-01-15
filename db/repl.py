"""
REPL (Read-Eval-Print Loop) for interactive database commands.
"""

from typing import Optional, Dict, Any
from .engine import DatabaseEngine
from .parser import SQLParser

# Try to import readline for better input handling (arrow keys, history)
try:
    import readline
    READLINE_AVAILABLE = True
except ImportError:
    READLINE_AVAILABLE = False


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
        self.history_file = None
        
        # Set up readline for better input handling
        if READLINE_AVAILABLE:
            # Configure readline
            readline.set_completer_delims(' \t\n;')
            readline.parse_and_bind("tab: complete")
            
            # Try to load history from file
            try:
                import os
                history_path = os.path.expanduser("~/.mydb_history")
                if os.path.exists(history_path):
                    readline.read_history_file(history_path)
                self.history_file = history_path
            except Exception:
                pass  # History file not critical
    
    def run(self) -> None:
        """Start the interactive REPL loop."""
        print("Welcome to MyDB RDBMS!")
        print("Type 'exit' or 'quit' to exit.")
        print("Type 'help' for available commands.\n")
        
        while True:
            try:
                # Use input() - readline will handle arrow keys automatically if available
                command = input(self.prompt).strip()
                
                if not command:
                    continue
                
                # Handle exit commands
                if command.lower() in ('exit', 'quit', 'q'):
                    # Save history before exiting
                    if READLINE_AVAILABLE and self.history_file:
                        try:
                            readline.write_history_file(self.history_file)
                        except Exception:
                            pass
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
                parsed["where_clause"],
                parsed.get("joins")
            )
            return {"status": "OK", "data": rows}
        
        elif cmd_type == "UPDATE":
            rows_affected = self.engine.update(
                parsed["table_name"],
                parsed["assignments"],
                parsed["where_clause"]
            )
            return {"status": "OK", "message": f"{rows_affected} row(s) updated"}
        
        elif cmd_type == "DELETE":
            rows_deleted = self.engine.delete(
                parsed["table_name"],
                parsed["where_clause"]
            )
            return {"status": "OK", "message": f"{rows_deleted} row(s) deleted"}
        
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
  UPDATE table_name SET column=value [, column=value ...] [WHERE column=value]
  DELETE FROM table_name [WHERE column=value]
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
