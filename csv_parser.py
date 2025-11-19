import os
from typing import List, Dict, Optional, Callable, Generator


class CSVParser:
    """
    A custom CSV parser with DataFrame-like functionality.
    Built from scratch without using csv, pandas, or similar libraries.
    
    Features:
    - Type inference (int, float, bool, string)
    - Schema detection
    - Filtering with lambda conditions
    - Column projection
    - Aggregation with grouping
    - Inner join operations
    - Chunked parsing for large files
    """
    
    def __init__(self, file_path: str, delimiter: str = ','):
        """
        Initialize CSVParser with a file path.
        
        Args:
            file_path: Path to the CSV file
            delimiter: Column delimiter (default: ',')
        
        Raises:
            FileNotFoundError: If the CSV file doesn't exist
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        self.file_path = file_path
        self.delimiter = delimiter
        self.data: List[Dict] = []
        self.schema: Dict[str, str] = {}

    # ----------------------------
    # CSV Parsing (Custom Implementation)
    # ----------------------------
    def _parse_csv_line(self, line: str) -> List[str]:
        """
        Parse a single CSV line, handling quoted fields.
        Custom implementation without using csv library.
        
        Args:
            line: A line from the CSV file
            
        Returns:
            List of field values
        """
        fields = []
        current_field = ""
        in_quotes = False
        i = 0
        
        while i < len(line):
            char = line[i]
            
            if char == '"':
                if in_quotes and i + 1 < len(line) and line[i + 1] == '"':
                    # Escaped quote
                    current_field += '"'
                    i += 1
                else:
                    # Toggle quote state
                    in_quotes = not in_quotes
            elif char == self.delimiter and not in_quotes:
                # End of field
                fields.append(current_field)
                current_field = ""
            else:
                current_field += char
            
            i += 1
        
        # Add last field
        fields.append(current_field)
        return fields
    
    def _read_csv_file(self) -> tuple[List[str], List[List[str]]]:
        """
        Read CSV file and return header and rows.
        Custom implementation without using csv library.
        
        Returns:
            Tuple of (header_list, rows_list)
        """
        with open(self.file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        if not lines:
            return [], []
        
        # Parse header
        header = self._parse_csv_line(lines[0].rstrip('\n\r'))
        
        # Parse data rows
        rows = []
        for line in lines[1:]:
            line = line.rstrip('\n\r')
            if line:  # Skip empty lines
                rows.append(self._parse_csv_line(line))
        
        return header, rows

    # ----------------------------
    # Type inference
    # ----------------------------
    def _infer_value_type(self, value: str):
        """
        Infer the type of a value from its string representation.
        
        Args:
            value: String value to infer type from
            
        Returns:
            Converted value (int, float, bool, string, or None)
        """
        if value is None or value.strip() == "":
            return None
        value = value.strip()
        
        # Try integer
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try float
        try:
            return float(value)
        except ValueError:
            pass
        
        # Try boolean
        if value.lower() in ("true", "false"):
            return value.lower() == "true"
        
        # Default to string
        return value

    def _infer_schema_all_rows(self):
        """
        Infer the schema by examining all rows in the dataset.
        Promotes types when conflicts are found (e.g., int + float -> float).
        """
        if not self.data:
            return
            
        col_types = {col: None for col in self.data[0].keys()}
        
        for row in self.data:
            for col, val in row.items():
                t = "string"
                if isinstance(val, int):
                    t = "int"
                elif isinstance(val, float):
                    t = "float"
                elif isinstance(val, bool):
                    t = "bool"
                
                # Promote types when necessary
                if col_types[col] is None:
                    col_types[col] = t
                elif col_types[col] != t:
                    # Simple promotion: any type mismatch becomes string
                    col_types[col] = "string"
        
        self.schema = col_types

    # ----------------------------
    # Parsing
    # ----------------------------
    def parse(self, type_inference: bool = True, chunk_size: Optional[int] = None) -> Optional[Generator[List[Dict], None, None]]:
        """
        Parse the CSV file into memory or yield chunks.
        Custom implementation without using csv library.
        
        Args:
            type_inference: Whether to infer types (default: True)
            chunk_size: If None, parse entire file. If int, yield chunks of this size
            
        Returns:
            List of dictionaries if chunk_size is None, otherwise a generator
        """
        if chunk_size is None:
            # Parse entire file into memory
            header, rows = self._read_csv_file()
            
            for row in rows:
                # Handle rows with different number of columns
                if len(row) != len(header):
                    # Pad with empty strings or truncate
                    if len(row) < len(header):
                        row.extend([''] * (len(header) - len(row)))
                    else:
                        row = row[:len(header)]
                
                # Create dictionary for this row
                parsed_row = {}
                for col_name, value in zip(header, row):
                    if type_inference:
                        parsed_row[col_name] = self._infer_value_type(value)
                    else:
                        parsed_row[col_name] = value.strip()
                
                self.data.append(parsed_row)
            
            if self.data:
                self._infer_schema_all_rows()
            
            print(f"[CSVParser] Parsed {len(self.data)} rows from {self.file_path}")
            return self.data
        else:
            # Return generator for chunked parsing
            def generator():
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    # Read and parse header
                    header_line = f.readline().rstrip('\n\r')
                    header = self._parse_csv_line(header_line)
                    
                    chunk = []
                    row_count = 0
                    
                    for line in f:
                        line = line.rstrip('\n\r')
                        if not line:  # Skip empty lines
                            continue
                        
                        row = self._parse_csv_line(line)
                        
                        # Handle rows with different number of columns
                        if len(row) != len(header):
                            if len(row) < len(header):
                                row.extend([''] * (len(header) - len(row)))
                            else:
                                row = row[:len(header)]
                        
                        # Create dictionary for this row
                        parsed_row = {}
                        for col_name, value in zip(header, row):
                            if type_inference:
                                parsed_row[col_name] = self._infer_value_type(value)
                            else:
                                parsed_row[col_name] = value.strip()
                        
                        chunk.append(parsed_row)
                        row_count += 1
                        
                        if row_count % chunk_size == 0:
                            yield chunk
                            chunk = []
                    
                    # Yield remaining rows
                    if chunk:
                        yield chunk
            
            return generator()

    # ----------------------------
    # DataFrame-like interface
    # ----------------------------
    def __getitem__(self, col: str):
        """
        Retrieve a column as a list using bracket notation.
        
        Example:
            parser['PTS']  # Returns list of all PTS values
            
        Args:
            col: Column name
            
        Returns:
            List of values for the specified column
            
        Raises:
            KeyError: If column doesn't exist
        """
        if col not in self.schema:
            raise KeyError(f"Column {col} not found")
        return [row[col] for row in self.data]

    def filter_rows(self, condition: Callable[[Dict], bool]) -> List[Dict]:
        """
        Filter rows based on a lambda condition.
        
        Example:
            parser.filter_rows(lambda row: row['PTS'] > 30)
            
        Args:
            condition: Lambda function that takes a row dict and returns bool
            
        Returns:
            List of rows that satisfy the condition
        """
        return [row for row in self.data if condition(row)]

    def filter_columns(self, columns: List[str]) -> List[Dict]:
        """
        Project specific columns from the dataset.
        
        Example:
            parser.filter_columns(['Player', 'PTS', 'TRB'])
            
        Args:
            columns: List of column names to keep
            
        Returns:
            List of dictionaries with only the specified columns
            
        Raises:
            ValueError: If any column doesn't exist in schema
        """
        bad_cols = [c for c in columns if c not in self.schema]
        if bad_cols:
            raise ValueError(f"Columns not found in schema: {bad_cols}")
        return [{col: row[col] for col in columns} for row in self.data]

    def aggregate(self, group_by: Optional[str], target_col: str, func: str):
        """
        Perform aggregation on a column, optionally grouped by another column.
        
        Examples:
            # Global aggregation
            parser.aggregate(None, 'PTS', 'avg')
            
            # Grouped aggregation
            parser.aggregate('Tm', 'PTS', 'avg')
            
        Args:
            group_by: Column to group by (None for global aggregation)
            target_col: Column to aggregate
            func: Aggregation function ('sum', 'max', 'min', 'avg', 'count')
            
        Returns:
            Dictionary {group_value: aggregated_value} if group_by is specified,
            otherwise a single aggregated value
            
        Raises:
            ValueError: If columns don't exist or function is unsupported
        """
        if target_col not in self.schema:
            raise ValueError(f"Column {target_col} not found")
        if group_by and group_by not in self.schema:
            raise ValueError(f"Group by column {group_by} not found")

        if group_by:
            # Grouped aggregation
            result = {}
            for row in self.data:
                key = row[group_by]
                val = row[target_col]
                if key not in result:
                    result[key] = []
                if val is not None:
                    result[key].append(val)
            
            # Apply aggregation function
            for k, vals in result.items():
                if func == "sum":
                    result[k] = sum(vals)
                elif func == "max":
                    result[k] = max(vals) if vals else None
                elif func == "min":
                    result[k] = min(vals) if vals else None
                elif func == "avg":
                    result[k] = sum(vals) / len(vals) if vals else None
                elif func == "count":
                    result[k] = len(vals)
                else:
                    raise ValueError(f"Unsupported aggregation function: {func}")
            return result
        else:
            # Global aggregation
            vals = [row[target_col] for row in self.data if row[target_col] is not None]
            if func == "sum":
                return sum(vals)
            elif func == "max":
                return max(vals) if vals else None
            elif func == "min":
                return min(vals) if vals else None
            elif func == "avg":
                return sum(vals) / len(vals) if vals else None
            elif func == "count":
                return len(vals)
            else:
                raise ValueError(f"Unsupported aggregation function: {func}")

    def join(self, other_data: List[Dict], left_on: str, right_on: str) -> List[Dict]:
        """
        Perform an inner join with another dataset.
        
        Example:
            team_data = other_parser.get_data()
            joined = parser.join(team_data, left_on='Tm', right_on='TeamCode')
            
        Args:
            other_data: List of dictionaries to join with
            left_on: Column name in this dataset to join on
            right_on: Column name in other dataset to join on
            
        Returns:
            List of merged dictionaries (inner join)
        """
        # Build index for right dataset
        right_index = {}
        for row in other_data:
            key = row[right_on]
            if key not in right_index:
                right_index[key] = []
            right_index[key].append(row)
        
        # Perform join
        joined = []
        for row in self.data:
            key = row[left_on]
            if key in right_index:
                for o_row in right_index[key]:
                    merged = row.copy()
                    merged.update(o_row)
                    joined.append(merged)
        
        return joined

    # ----------------------------
    # Utilities
    # ----------------------------
    def get_data(self) -> List[Dict]:
        """
        Get the raw data.
        
        Returns:
            List of dictionaries representing rows
        """
        return self.data

    def get_schema(self) -> Dict[str, str]:
        """
        Get the inferred schema.
        
        Returns:
            Dictionary mapping column names to types
        """
        return self.schema

    def summary(self) -> Dict:
        """
        Get a summary of the dataset.
        
        Returns:
            Dictionary with file path, row count, columns, schema, and sample row
        """
        return {
            "file": self.file_path,
            "rows": len(self.data),
            "columns": list(self.schema.keys()),
            "schema": self.schema,
            "sample_row": self.data[0] if self.data else None
        }
    
    def sort_data(self, column: str, reverse: bool = False) -> List[Dict]:
        """
        Sort data by a specific column.
        
        Args:
            column: Column name to sort by
            reverse: If True, sort in descending order
            
        Returns:
            Sorted list of dictionaries
        """
        if column not in self.schema:
            raise ValueError(f"Column {column} not found")
        
        return sorted(
            self.data, 
            key=lambda x: x[column] if x[column] is not None else float('-inf'),
            reverse=reverse
        )