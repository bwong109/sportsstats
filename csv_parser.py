import csv
import os
from typing import List, Dict, Optional, Callable, Generator


class CSVParser:
    def __init__(self, file_path: str):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        self.file_path = file_path
        self.data: List[Dict] = []
        self.schema: Dict[str, str] = {}

    # ----------------------------
    # Type inference
    # ----------------------------
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
                # Promote types
                if col_types[col] is None:
                    col_types[col] = t
                elif col_types[col] != t:
                    col_types[col] = "string"  # simple promotion
        self.schema = col_types

    # ----------------------------
    # Parsing
    # ----------------------------
    def parse(self, type_inference: bool = True, chunk_size: Optional[int] = None) -> Optional[Generator[List[Dict], None, None]]:
        """
        If chunk_size is None: parse entire CSV into memory
        If chunk_size is an int: yield chunks of rows
        """
        if chunk_size is None:
            with open(self.file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    parsed_row = {col: self._infer_value_type(val) if type_inference else val.strip()
                                  for col, val in row.items()}
                    self.data.append(parsed_row)
            if self.data:
                self._infer_schema_all_rows()
            print(f"[CSVParser] Parsed {len(self.data)} rows from {self.file_path}")
            return self.data
        else:
            def generator():
                with open(self.file_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    chunk = []
                    for i, row in enumerate(reader, start=1):
                        parsed_row = {col: self._infer_value_type(val) if type_inference else val.strip()
                                      for col, val in row.items()}
                        chunk.append(parsed_row)
                        if i % chunk_size == 0:
                            yield chunk
                            chunk = []
                    if chunk:
                        yield chunk
            return generator()

    # ----------------------------
    # DataFrame-like interface
    # ----------------------------
    def __getitem__(self, col: str):
        if col not in self.schema:
            raise KeyError(f"Column {col} not found")
        return [row[col] for row in self.data]

    def filter_rows(self, condition: Callable[[Dict], bool]):
        """Return rows satisfying the lambda condition"""
        return [row for row in self.data if condition(row)]

    def filter_columns(self, columns: List[str]):
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
                    result[k] = max(vals)
                elif func == "min":
                    result[k] = min(vals)
                elif func == "avg":
                    result[k] = sum(vals)/len(vals) if vals else None
                elif func == "count":
                    result[k] = len(vals)
                else:
                    raise ValueError(f"Unsupported aggregation function {func}")
            return result
        else:
            vals = [row[target_col] for row in self.data if row[target_col] is not None]
            if func == "sum":
                return sum(vals)
            elif func == "max":
                return max(vals)
            elif func == "min":
                return min(vals)
            elif func == "avg":
                return sum(vals)/len(vals) if vals else None
            elif func == "count":
                return len(vals)
            else:
                raise ValueError(f"Unsupported aggregation function {func}")

    def join(self, other_data: List[Dict], left_on: str, right_on: str):
        """Inner join with another dataset"""
        right_index = {}
        for row in other_data:
            key = row[right_on]
            if key not in right_index:
                right_index[key] = []
            right_index[key].append(row)
        joined = []
        for row in self.data:
            key = row[left_on]
            if key in right_index:
                for o_row in right_index[key]:
                    merged = row.copy()
                    merged.update(o_row)
                    joined.append(merged)
        return joined

    # --------------x`--------------
    # Utilities
    # ----------------------------
    def get_data(self):
        return self.data

    def get_schema(self):
        return self.schema

    def summary(self):
        return {
            "file": self.file_path,
            "rows": len(self.data),
            "columns": list(self.schema.keys()),
            "schema": self.schema,
            "sample_row": self.data[0] if self.data else None
        }
