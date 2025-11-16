import csv
from typing import List, Dict, Optional
import os


class CSVParser:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.data = []
        self.schema = {}

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")

    # ----------------------------
    # Type inference utilities
    # ----------------------------
    def _infer_value_type(self, value: str):
        """Infer type of a single value."""
        if value is None or value.strip() == "":
            return None

        value = value.strip()

        # int
        try:
            return int(value)
        except ValueError:
            pass

        # float
        try:
            return float(value)
        except ValueError:
            pass

        # boolean
        lower = value.lower()
        if lower in ("true", "false"):
            return lower == "true"

        # string
        return value

    def _promote_type(self, t1: str, t2: str):
        """Promote column types when conflicts occur."""
        order = ["int", "float", "bool", "string"]

        # if either is string → string
        if t1 == "string" or t2 == "string":
            return "string"

        # bool + number → string (avoid ambiguous casting)
        if (t1 in ("int", "float") and t2 == "bool") or (t2 in ("int", "float") and t1 == "bool"):
            return "string"

        # numeric promotion
        return order[max(order.index(t1), order.index(t2))]

    def _infer_schema_all_rows(self):
        """Infer schema using every row in the dataset."""
        col_types = {col: None for col in self.data[0].keys()}

        for row in self.data:
            for col, value in row.items():
                if value is None:
                    continue

                if isinstance(value, int):
                    t = "int"
                elif isinstance(value, float):
                    t = "float"
                elif isinstance(value, bool):
                    t = "bool"
                else:
                    t = "string"

                if col_types[col] is None:
                    col_types[col] = t
                else:
                    col_types[col] = self._promote_type(col_types[col], t)

        # fallback to string
        for col in col_types:
            if col_types[col] is None:
                col_types[col] = "string"

        self.schema = col_types

    # ----------------------------
    # Parsing
    # ----------------------------
    def parse(self, type_inference: bool = True) -> List[Dict]:
        with open(self.file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            if not reader.fieldnames:
                raise ValueError("CSV file has no header row.")

            for row in reader:
                parsed_row = {}
                for col, value in row.items():
                    if type_inference:
                        parsed_row[col] = self._infer_value_type(value)
                    else:
                        parsed_row[col] = value.strip() if value else None

                self.data.append(parsed_row)

        # Infer schema from all rows
        if type_inference and self.data:
            self._infer_schema_all_rows()

        print(f"[CSVParser] Parsed {len(self.data)} rows from {self.file_path}")
        return self.data

    # ----------------------------
    # Utility functions
    # ----------------------------
    def get_data(self):
        return self.data

    def get_schema(self):
        return self.schema

    def filter_columns(self, columns: List[str]) -> List[Dict]:
        bad_cols = [c for c in columns if c not in self.schema]
        if bad_cols:
            raise ValueError(f"Columns not found in schema: {bad_cols}")

        return [{col: row[col] for col in columns} for row in self.data]

    def summary(self) -> Dict:
        return {
            "file": self.file_path,
            "rows": len(self.data),
            "columns": list(self.schema.keys()),
            "schema": self.schema,
            "sample_row": self.data[0] if self.data else None
        }
