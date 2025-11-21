import os

# GLOBAL chunk size for scalable filtering + aggregation
CHUNK_PROCESS_SIZE = 5000  


def iter_chunks(data, chunk_size=CHUNK_PROCESS_SIZE):
    """Yield list slices in consistent fixed-size chunks."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


class CSVParser:
    
    def __init__(self, file_path, delimiter=','):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        self.file_path = file_path
        self.delimiter = delimiter
        self.data = []
        self.schema = {}

    # ----------------------------------------------------------------------
    # PARSER HELPERS
    # ----------------------------------------------------------------------

    def _parse_csv_line(self, line):
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

    def _read_csv_file(self):
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

    # ----------------------------------------------------------------------
    # TYPE INFERENCE
    # ----------------------------------------------------------------------

    def _infer_value_type(self, value):
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
                if isinstance(val, int):
                    t = "int"
                elif isinstance(val, float):
                    t = "float"
                elif isinstance(val, bool):
                    t = "bool"
                else:
                    t = "string"
                
                if col_types[col] is None:
                    col_types[col] = t
                elif col_types[col] != t:
                    col_types[col] = "string"
        
        self.schema = col_types

    # ----------------------------------------------------------------------
    # PARSE CSV (FULL + CHUNKED)
    # ----------------------------------------------------------------------

    def parse(self, type_inference=True, chunk_size=None):
        """Parse whole file or chunk generator depending on chunk_size."""
        
        # -------- FULL PARSE --------
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
                    parsed_row[col_name] = (
                        self._infer_value_type(value) if type_inference else value.strip()
                    )
                
                self.data.append(parsed_row)
            
            if self.data:
                self._infer_schema_all_rows()
            
            return self.data
        
        # -------- CHUNKED PARSE --------
        else:
            def generator():
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    header = self._parse_csv_line(f.readline().rstrip('\n\r'))
                    
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
                            parsed_row[col_name] = (
                                self._infer_value_type(value) if type_inference else value.strip()
                            )
                        
                        chunk.append(parsed_row)
                        row_count += 1
                        
                        if row_count % chunk_size == 0:
                            yield chunk
                            chunk = []
                    
                    if chunk:
                        yield chunk
            
            return generator()

    # ----------------------------------------------------------------------
    # ACCESSORS
    # ----------------------------------------------------------------------

    def __getitem__(self, col):
        if col not in self.schema:
            raise KeyError(f"Column {col} not found")
        return [row[col] for row in self.data]

    def filter_rows(self, condition):
        return [row for row in self.data if condition(row)]

    def filter_columns(self, columns):
        missing = [c for c in columns if c not in self.schema]
        if missing:
            raise ValueError(f"Columns not found: {missing}")
        return [{col: row[col] for col in columns} for row in self.data]

    # ----------------------------------------------------------------------
    # AGGREGATION (non-chunked – chunk logic handled in Flask)
    # ----------------------------------------------------------------------

    def aggregate(self, group_by, target_col, func):
        if target_col not in self.schema and target_col is not None:
            raise ValueError(f"Column {target_col} not found")

        if group_by and group_by not in self.schema:
            raise ValueError(f"Group by column {group_by} not found")

        # ---- GROUP BY MODE ----
        if group_by:
            result = {}
            
            for row in self.data:
                g = row[group_by]
                v = row[target_col]
                if g not in result:
                    result[g] = []
                if v is not None:
                    result[g].append(v)

            # Reduce
            for key, vals in result.items():
                if func == "sum":
                    result[key] = sum(vals)
                elif func == "max":
                    result[key] = max(vals)
                elif func == "min":
                    result[key] = min(vals)
                elif func == "avg":
                    result[key] = sum(vals) / len(vals)
                elif func == "count":
                    result[key] = len(vals)
                else:
                    raise ValueError(f"Unsupported function: {func}")

            return result

        # ---- GLOBAL AGGREGATION ----
        vals = [row[target_col] for row in self.data if row[target_col] is not None]

        if func == "sum":
            return sum(vals)
        if func == "max":
            return max(vals)
        if func == "min":
            return min(vals)
        if func == "avg":
            return sum(vals) / len(vals)
        if func == "count":
            return len(vals)

        raise ValueError(f"Unsupported function: {func}")

    # ----------------------------------------------------------------------
    # JOIN
    # ----------------------------------------------------------------------

    def join(self, other_data, left_on, right_on):
        if not other_data or not self.data:
            return []

        # Build right side index
        index = {}
        for row in other_data:
            if right_on not in row:
                continue
            key = row[right_on]
            index.setdefault(key, []).append(row)

        # Join
        joined = []
        for row in self.data:
            if left_on not in row:
                continue
            key = row[left_on]
            if key in index:
                for match in index[key]:
                    merged = row.copy()
                    merged.update(match)
                    joined.append(merged)

        return joined

    # ----------------------------------------------------------------------

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