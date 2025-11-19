import os
from typing import List, Dict, Optional, Callable, Generator


class CSVParser:
    
    def __init__(self, file_path: str, delimiter: str = ','):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        self.file_path = file_path
        self.delimiter = delimiter
        self.data: List[Dict] = []
        self.schema: Dict[str, str] = {}

    def _parse_csv_line(self, line: str) -> List[str]:
        fields = []
        current_field = ""
        in_quotes = False
        i = 0
        
        while i < len(line):
            char = line[i]
            
            if char == '"':
                if in_quotes and i + 1 < len(line) and line[i + 1] == '"':
                    current_field += '"'
                    i += 1
                else:
                    in_quotes = not in_quotes
            elif char == self.delimiter and not in_quotes:
                fields.append(current_field)
                current_field = ""
            else:
                current_field += char
            
            i += 1
        
        fields.append(current_field)
        return fields
    
    def _read_csv_file(self) -> tuple[List[str], List[List[str]]]:
        with open(self.file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        if not lines:
            return [], []
        
        header = self._parse_csv_line(lines[0].rstrip('\n\r'))
        
        rows = []
        for line in lines[1:]:
            line = line.rstrip('\n\r')
            if line:
                rows.append(self._parse_csv_line(line))
        
        return header, rows

    def _infer_value_type(self, value: str):
        if value is None or value.strip() == "":
            return None
        value = value.strip()
        
        try:
            return int(value)
        except ValueError:
            pass
        
        try:
            return float(value)
        except ValueError:
            pass
        
        if value.lower() in ("true", "false"):
            return value.lower() == "true"
        
        return value

    def _infer_schema_all_rows(self):
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
                
                if col_types[col] is None:
                    col_types[col] = t
                elif col_types[col] != t:
                    col_types[col] = "string"
        
        self.schema = col_types

    def parse(self, type_inference: bool = True, chunk_size: Optional[int] = None) -> Optional[Generator[List[Dict], None, None]]:
        if chunk_size is None:
            header, rows = self._read_csv_file()
            
            for row in rows:
                if len(row) != len(header):
                    if len(row) < len(header):
                        row.extend([''] * (len(header) - len(row)))
                    else:
                        row = row[:len(header)]
                
                parsed_row = {}
                for col_name, value in zip(header, row):
                    if type_inference:
                        parsed_row[col_name] = self._infer_value_type(value)
                    else:
                        parsed_row[col_name] = value.strip()
                
                self.data.append(parsed_row)
            
            if self.data:
                self._infer_schema_all_rows()
            
            return self.data
        else:
            def generator():
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    header_line = f.readline().rstrip('\n\r')
                    header = self._parse_csv_line(header_line)
                    
                    chunk = []
                    row_count = 0
                    
                    for line in f:
                        line = line.rstrip('\n\r')
                        if not line:
                            continue
                        
                        row = self._parse_csv_line(line)
                        
                        if len(row) != len(header):
                            if len(row) < len(header):
                                row.extend([''] * (len(header) - len(row)))
                            else:
                                row = row[:len(header)]
                        
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
                    
                    if chunk:
                        yield chunk
            
            return generator()

    def __getitem__(self, col: str):
        if col not in self.schema:
            raise KeyError(f"Column {col} not found")
        return [row[col] for row in self.data]

    def filter_rows(self, condition: Callable[[Dict], bool]) -> List[Dict]:
        return [row for row in self.data if condition(row)]

    def filter_columns(self, columns: List[str]) -> List[Dict]:
        bad_cols = [c for c in columns if c not in self.schema]
        if bad_cols:
            raise ValueError(f"Columns not found in schema: {bad_cols}")
        return [{col: row[col] for col in columns} for row in self.data]

    def aggregate(self, group_by: Optional[str], target_col: str, func: str):
        if target_col not in self.schema:
            raise ValueError(f"Column {target_col} not found")
        if group_by and group_by not in self.schema:
            raise ValueError(f"Group by column {group_by} not found")

        if group_by:
            result = {}
            for row in self.data:
                key = row[group_by]
                val = row[target_col]
                if key not in result:
                    result[key] = []
                if val is not None:
                    result[key].append(val)
            
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
        if not other_data or not self.data:
            return []

        right_index: Dict[object, List[Dict]] = {}
        for row in other_data:
            if right_on not in row:
                continue
            key = row[right_on]
            if key not in right_index:
                right_index[key] = []
            right_index[key].append(row)

        joined: List[Dict] = []
        for row in self.data:
            if left_on not in row:
                continue
            key = row[left_on]
            if key in right_index:
                for o_row in right_index[key]:
                    merged = row.copy()
                    merged.update(o_row)
                    joined.append(merged)

        return joined

    def get_data(self) -> List[Dict]:
        return self.data

    def get_schema(self) -> Dict[str, str]:
        return self.schema

    def summary(self) -> Dict:
        return {
            "file": self.file_path,
            "rows": len(self.data),
            "columns": list(self.schema.keys()),
            "schema": self.schema,
            "sample_row": self.data[0] if self.data else None
        }
    
    def sort_data(self, column: str, reverse: bool = False) -> List[Dict]:
        if column not in self.schema:
            raise ValueError(f"Column {column} not found")
        
        return sorted(
            self.data, 
            key=lambda x: x[column] if x[column] is not None else float('-inf'),
            reverse=reverse
        )