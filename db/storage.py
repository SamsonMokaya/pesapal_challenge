"""
Storage layer for file-based JSON storage.
Handles reading and writing table data, metadata, and indexes.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional


class Storage:
    """Manages file-based JSON storage for tables."""
    
    def __init__(self, data_dir: str = "data"):
        """
        Initialize storage with a data directory.
        
        Args:
            data_dir: Directory where table files will be stored
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
    
    def _get_table_path(self, table_name: str) -> Path:
        """Get the file path for a table."""
        return self.data_dir / f"{table_name}.json"
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table file exists."""
        return self._get_table_path(table_name).exists()
    
    def create_table(self, table_name: str, schema: Dict[str, Any]) -> None:
        """
        Create a new table file with schema and empty data.
        
        Args:
            table_name: Name of the table
            schema: Table schema containing columns, constraints, indexes
        """
        if self.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' already exists")
        
        table_data = {
            "schema": schema,
            "data": [],
            "indexes": {}
        }
        
        self._write_table(table_name, table_data)
    
    def get_table(self, table_name: str) -> Dict[str, Any]:
        """
        Load a table's data from file.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing schema, data, and indexes
        """
        if not self.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")
        
        table_path = self._get_table_path(table_name)
        with open(table_path, 'r') as f:
            return json.load(f)
    
    def _write_table(self, table_name: str, table_data: Dict[str, Any]) -> None:
        """Write table data to file."""
        table_path = self._get_table_path(table_name)
        with open(table_path, 'w') as f:
            json.dump(table_data, f, indent=2)
    
    def save_table(self, table_name: str, table_data: Dict[str, Any]) -> None:
        """
        Save table data to file.
        
        Args:
            table_name: Name of the table
            table_data: Complete table data dictionary
        """
        self._write_table(table_name, table_data)
    
    def insert_row(self, table_name: str, row: Dict[str, Any]) -> None:
        """
        Insert a new row into a table.
        
        Args:
            table_name: Name of the table
            row: Dictionary of column_name: value pairs
        """
        table_data = self.get_table(table_name)
        table_data["data"].append(row)
        self.save_table(table_name, table_data)
    
    def get_all_rows(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get all rows from a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of row dictionaries
        """
        table_data = self.get_table(table_name)
        return table_data["data"]
    
    def update_table_data(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """
        Update all data rows for a table.
        
        Args:
            table_name: Name of the table
            data: List of row dictionaries
        """
        table_data = self.get_table(table_name)
        table_data["data"] = data
        self.save_table(table_name, table_data)
    
    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get the schema for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Schema dictionary
        """
        table_data = self.get_table(table_name)
        return table_data["schema"]
    
    def update_indexes(self, table_name: str, indexes: Dict[str, Any]) -> None:
        """
        Update indexes for a table.
        
        Args:
            table_name: Name of the table
            indexes: Dictionary of index_name: index_data
        """
        table_data = self.get_table(table_name)
        table_data["indexes"] = indexes
        self.save_table(table_name, table_data)
    
    def get_indexes(self, table_name: str) -> Dict[str, Any]:
        """
        Get all indexes for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary of indexes
        """
        table_data = self.get_table(table_name)
        return table_data.get("indexes", {})
    
    def drop_table(self, table_name: str) -> None:
        """
        Delete a table file.
        
        Args:
            table_name: Name of the table
        """
        if not self.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")
        
        table_path = self._get_table_path(table_name)
        table_path.unlink()
