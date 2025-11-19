from flask import Flask, request, render_template_string, session, jsonify
from csv_parser import CSVParser
import time
import os
import glob
import threading

APP = Flask(__name__)
APP.secret_key = 'csv-parser-secret-key-2024'

DATA_FOLDER = "data"
parsers = {}
chunk_stats = {}
loading_progress = {}
active_dataset = None


def get_chunk_size(file_size_mb):
    if file_size_mb < 1:
        return None
    elif file_size_mb < 10:
        return 1000
    elif file_size_mb < 50:
        return 500
    elif file_size_mb < 100:
        return 200
    else:
        return 100


def load_dataset_with_progress(filepath, dataset_name):
    global parsers, chunk_stats, loading_progress
    
    start_time = time.time()
    parser = CSVParser(filepath)
    
    file_size = os.path.getsize(filepath)
    file_size_mb = file_size / (1024 * 1024)
    chunk_size = get_chunk_size(file_size_mb)
    
    if chunk_size is None:
        parser.parse(type_inference=True)
        chunk_stats[dataset_name] = {
            'strategy': 'full',
            'chunks_processed': 1,
            'total_rows': len(parser.data),
            'load_time': time.time() - start_time,
            'file_size_mb': file_size_mb,
            'chunk_size': 'N/A'
        }
    else:
        chunk_stats[dataset_name] = {
            'strategy': 'chunked',
            'chunks_processed': 0,
            'total_rows': 0,
            'file_size_mb': file_size_mb,
            'chunk_size': chunk_size
        }
        
        chunk_generator = parser.parse(type_inference=True, chunk_size=chunk_size)
        for chunk in chunk_generator:
            chunk_stats[dataset_name]['chunks_processed'] += 1
            chunk_stats[dataset_name]['total_rows'] += len(chunk)
            parser.data.extend(chunk)
            
        if parser.data:
            parser._infer_schema_all_rows()
        
        chunk_stats[dataset_name]['load_time'] = time.time() - start_time
    
    parsers[dataset_name] = parser


def get_available_datasets():
    csv_files = glob.glob(os.path.join(DATA_FOLDER, "*.csv"))
    return [os.path.basename(f) for f in csv_files]


def get_query_state():
    if 'query_state' not in session:
        session['query_state'] = {
            'filters': [],
            'selected_columns': [],
            'sort_column': '',
            'sort_order': 'desc',
            'show_all_columns': True,
            'join_dataset': '',
            'join_left_col': '',
            'join_right_col': '',
            'aggregation_column': '',
            'aggregation_function': '',
            'aggregation_group_by': '',
            'limit': 50,
            'use_limit': True
        }
    
    state = session['query_state']
    defaults = {
        'filters': [],
        'selected_columns': [],
        'sort_column': '',
        'sort_order': 'desc',
        'show_all_columns': True,
        'join_dataset': '',
        'join_left_col': '',
        'join_right_col': '',
        'aggregation_column': '',
        'aggregation_function': '',
        'aggregation_group_by': '',
        'limit': 50,
        'use_limit': True
    }
    
    for key, default_value in defaults.items():
        if key not in state:
            state[key] = default_value
    
    session['query_state'] = state
    return state


def apply_filters(data, filters, schema):
    if not filters:
        return data
    
    filtered_data = data
    for f in filters:
        if not f.get('column') or not f.get('value'):
            continue
            
        col = f['column']
        op = f['op']
        val = f['value']
        
        try:
            val = int(val)
        except ValueError:
            try:
                val = float(val)
            except ValueError:
                pass
        
        if op == ">":
            condition = lambda row, c=col, v=val: row.get(c) is not None and row.get(c) > v
        elif op == ">=":
            condition = lambda row, c=col, v=val: row.get(c) is not None and row.get(c) >= v
        elif op == "<":
            condition = lambda row, c=col, v=val: row.get(c) is not None and row.get(c) < v
        elif op == "<=":
            condition = lambda row, c=col, v=val: row.get(c) is not None and row.get(c) <= v
        elif op == "==":
            condition = lambda row, c=col, v=val: row.get(c) == v
        elif op == "!=":
            condition = lambda row, c=col, v=val: row.get(c) != v
        else:
            continue
        
        temp_parser = CSVParser.__new__(CSVParser)
        temp_parser.data = filtered_data
        temp_parser.schema = schema
        filtered_data = temp_parser.filter_rows(condition)
    
    return filtered_data


def apply_aggregation(data, aggregation_column, aggregation_function, group_by_column):
    # If only group_by is specified without aggregation, do GROUP BY only
    if group_by_column and (not aggregation_column or not aggregation_function):
        if not data:
            return data, None
        
        # Create a dictionary to track unique combinations
        seen = {}
        grouped_data = []
        
        for row in data:
            group_val = row.get(group_by_column)
            if group_val not in seen:
                seen[group_val] = True
                grouped_data.append(row.copy())
        
        return grouped_data, f"Grouped by {group_by_column}"
    
    # If no aggregation column or function, return data as-is
    if not aggregation_column or not aggregation_function:
        return data, None
    
    temp_parser = CSVParser.__new__(CSVParser)
    temp_parser.data = data
    
    if data:
        schema = {}
        for col in data[0].keys():
            schema[col] = "string"
        temp_parser.schema = schema
    
    try:
        if group_by_column:
            result = temp_parser.aggregate(group_by_column, aggregation_column, aggregation_function)
            aggregated_data = []
            for group_val, agg_val in result.items():
                aggregated_data.append({
                    group_by_column: group_val,
                    f"{aggregation_function}({aggregation_column})": agg_val
                })
            return aggregated_data, f"{aggregation_function.upper()} of {aggregation_column} grouped by {group_by_column}"
        else:
            result = temp_parser.aggregate(None, aggregation_column, aggregation_function)
            aggregated_data = [{f"{aggregation_function}({aggregation_column})": result}]
            return aggregated_data, f"{aggregation_function.upper()} of {aggregation_column}"
    except Exception as e:
        return data, f"Aggregation error: {e}"


def execute_query(p, state):
    base_data = p.data
    working_data = base_data
    aggregation_info = None
    working_schema = p.schema

    join_ds = state.get('join_dataset')
    join_left = state.get('join_left_col')
    join_right = state.get('join_right_col')

    if join_ds and join_left and join_right and join_ds in parsers:
        other_parser = parsers[join_ds]

        temp_join_parser = CSVParser.__new__(CSVParser)
        temp_join_parser.data = base_data
        temp_join_parser.schema = p.schema

        working_data = temp_join_parser.join(other_parser.data, left_on=join_left, right_on=join_right)

        temp_schema_parser = CSVParser.__new__(CSVParser)
        temp_schema_parser.data = working_data
        if working_data:
            temp_schema_parser._infer_schema_all_rows()
            working_schema = temp_schema_parser.schema
        else:
            working_schema = p.schema
    else:
        working_data = base_data
        working_schema = p.schema

    columns = list(working_schema.keys())

    if state.get('filters'):
        working_data = apply_filters(working_data, state['filters'], working_schema)

    if not state.get('show_all_columns', True) and state.get('selected_columns'):
        temp_parser = CSVParser.__new__(CSVParser)
        temp_parser.data = working_data
        temp_parser.schema = working_schema
        working_data = temp_parser.filter_columns(state['selected_columns'])
        columns = state['selected_columns']

    # Check if we have aggregation or just GROUP BY
    if state.get('aggregation_column') and state.get('aggregation_function'):
        # Full aggregation with function
        working_data, aggregation_info = apply_aggregation(
            working_data,
            state['aggregation_column'],
            state['aggregation_function'],
            state.get('aggregation_group_by', '')
        )
        if working_data:
            columns = list(working_data[0].keys())
    elif state.get('aggregation_group_by'):
        # GROUP BY only, no aggregation function
        working_data, aggregation_info = apply_aggregation(
            working_data,
            None,
            None,
            state.get('aggregation_group_by', '')
        )
        if working_data:
            columns = list(working_data[0].keys())

    if state.get('sort_column') and not aggregation_info:
        temp_parser = CSVParser.__new__(CSVParser)
        temp_parser.data = working_data

        temp_schema_parser = CSVParser.__new__(CSVParser)
        temp_schema_parser.data = working_data
        if working_data:
            temp_schema_parser._infer_schema_all_rows()
            temp_parser.schema = temp_schema_parser.schema
        else:
            temp_parser.schema = working_schema

        working_data = temp_parser.sort_data(
            state['sort_column'],
            reverse=(state.get('sort_order', 'desc') == 'desc')
        )
    elif state.get('sort_column') and aggregation_info:
        # Allow sorting even with aggregation/grouping
        temp_parser = CSVParser.__new__(CSVParser)
        temp_parser.data = working_data

        temp_schema_parser = CSVParser.__new__(CSVParser)
        temp_schema_parser.data = working_data
        if working_data:
            temp_schema_parser._infer_schema_all_rows()
            temp_parser.schema = temp_schema_parser.schema
        else:
            temp_parser.schema = working_schema

        # Check if sort column exists in the current data
        if working_data and state['sort_column'] in working_data[0]:
            working_data = temp_parser.sort_data(
                state['sort_column'],
                reverse=(state.get('sort_order', 'desc') == 'desc')
            )

    # Apply limit if enabled
    total_rows = len(working_data)
    if state.get('use_limit', True) and state.get('limit'):
        limit = int(state['limit'])
        working_data = working_data[:limit]
    
    return working_data, columns, aggregation_info, working_schema, total_rows


@APP.route("/api/loading_progress/<dataset_name>")
def get_loading_progress(dataset_name):
    if dataset_name in loading_progress:
        return jsonify(loading_progress[dataset_name])
    return jsonify({'status': 'not_found'})


@APP.route("/api/dataset_columns/<dataset_name>")
def get_dataset_columns(dataset_name):
    if dataset_name in parsers:
        columns = list(parsers[dataset_name].schema.keys())
        return jsonify({'columns': columns})
    
    try:
        filepath = os.path.join(DATA_FOLDER, dataset_name)
        if os.path.exists(filepath):
            temp_parser = CSVParser(filepath)
            temp_parser.parse(type_inference=True)
            columns = list(temp_parser.get_schema().keys())
            return jsonify({'columns': columns})
    except Exception:
        pass
    
    return jsonify({'columns': []})


@APP.route("/api/load_dataset", methods=["POST"])
def load_dataset():
    data = request.get_json()
    dataset_name = data.get('dataset')
    
    if not dataset_name:
        return jsonify({'error': 'No dataset specified'}), 400
    
    filepath = os.path.join(DATA_FOLDER, dataset_name)
    if not os.path.exists(filepath):
        return jsonify({'error': 'Dataset not found'}), 404
    
    thread = threading.Thread(target=load_dataset_with_progress, args=(filepath, dataset_name))
    thread.start()
    thread.join()  # Wait for loading to complete
    
    return jsonify({'status': 'complete', 'dataset': dataset_name})


PAGE_TEMPLATE = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Sports Statistics</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f5f5f5;
      color: #333;
      line-height: 1.6;
    }

    .header {
      background: white;
      border-bottom: 2px solid #e0e0e0;
      padding: 20px;
      margin-bottom: 20px;
    }

    .header h1 {
      font-size: 24px;
      font-weight: 600;
      margin-bottom: 15px;
    }

    .dataset-selector {
      display: flex;
      gap: 10px;
      align-items: center;
    }

    .container {
      max-width: 1400px;
      margin: 0 auto;
      padding: 20px;
    }

    .grid {
      display: grid;
      grid-template-columns: 2fr 1fr;
      gap: 20px;
    }

    @media (max-width: 1024px) {
      .grid {
        grid-template-columns: 1fr;
      }
    }

    .card {
      background: white;
      border: 1px solid #e0e0e0;
      border-radius: 8px;
      padding: 20px;
    }

    .card-title {
      font-size: 18px;
      font-weight: 600;
      margin-bottom: 15px;
      padding-bottom: 10px;
      border-bottom: 1px solid #e0e0e0;
    }

    .form-group {
      margin-bottom: 15px;
    }

    .form-label {
      display: block;
      font-size: 13px;
      font-weight: 500;
      margin-bottom: 5px;
    }

    .form-input, .form-select {
      width: 100%;
      padding: 8px 10px;
      border: 1px solid #ddd;
      border-radius: 4px;
      font-size: 14px;
    }

    .form-input:focus, .form-select:focus {
      outline: none;
      border-color: #4a90e2;
    }

    .btn {
      padding: 8px 16px;
      border: none;
      border-radius: 4px;
      font-size: 14px;
      cursor: pointer;
      transition: background 0.2s;
    }

    .btn-primary {
      background: #4a90e2;
      color: white;
    }

    .btn-primary:hover {
      background: #357abd;
    }

    .btn-secondary {
      background: #f0f0f0;
      color: #333;
    }

    .btn-secondary:hover {
      background: #e0e0e0;
    }

    .btn-success {
      background: #5cb85c;
      color: white;
    }

    .btn-success:hover {
      background: #4cae4c;
    }

    .btn-danger {
      background: #d9534f;
      color: white;
    }

    .btn-danger:hover {
      background: #c9302c;
    }

    .tabs {
      display: flex;
      gap: 5px;
      border-bottom: 1px solid #e0e0e0;
      margin-bottom: 20px;
    }

    .tab-btn {
      padding: 10px 15px;
      border: none;
      background: transparent;
      cursor: pointer;
      font-size: 14px;
      color: #666;
      border-bottom: 2px solid transparent;
      margin-bottom: -1px;
    }

    .tab-btn.active {
      color: #4a90e2;
      border-bottom-color: #4a90e2;
    }

    .tab-content {
      display: none;
    }

    .tab-content.active {
      display: block;
    }

    .form-row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }

    .form-row-3 {
      display: grid;
      grid-template-columns: 2fr 1fr 2fr;
      gap: 10px;
    }

    .filter-list {
      border: 1px solid #e0e0e0;
      border-radius: 4px;
      padding: 10px;
      background: #fafafa;
      margin-bottom: 15px;
    }

    .filter-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 8px;
      background: white;
      border: 1px solid #e0e0e0;
      border-radius: 4px;
      margin-bottom: 8px;
      font-size: 13px;
    }

    .filter-item:last-child {
      margin-bottom: 0;
    }

    .remove-btn {
      background: #f0f0f0;
      color: #666;
      border: none;
      padding: 4px 8px;
      border-radius: 3px;
      font-size: 12px;
      cursor: pointer;
    }

    .button-group {
      display: flex;
      gap: 10px;
      margin-top: 15px;
    }

    .alert {
      padding: 12px;
      border-radius: 4px;
      margin-top: 15px;
      font-size: 14px;
    }

    .alert-success {
      background: #d4edda;
      color: #155724;
      border: 1px solid #c3e6cb;
    }

    .alert-error {
      background: #f8d7da;
      color: #721c24;
      border: 1px solid #f5c6cb;
    }

    .alert-info {
      background: #d1ecf1;
      color: #0c5460;
      border: 1px solid #bee5eb;
    }

    .results-table-wrapper {
      max-height: 500px;
      overflow: auto;
      margin-top: 15px;
      border: 1px solid #e0e0e0;
      border-radius: 4px;
    }

    .results-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }

    .results-table th,
    .results-table td {
      padding: 10px;
      text-align: left;
      border-bottom: 1px solid #e0e0e0;
    }

    .results-table th {
      background: #fafafa;
      font-weight: 600;
      position: sticky;
      top: 0;
    }

    .results-table tbody tr:hover {
      background: #f9f9f9;
    }

    .info-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 15px;
      margin-bottom: 20px;
    }

    .info-item {
      padding: 15px;
      background: #fafafa;
      border: 1px solid #e0e0e0;
      border-radius: 4px;
    }

    .info-label {
      font-size: 12px;
      color: #666;
      margin-bottom: 5px;
    }

    .info-value {
      font-size: 20px;
      font-weight: 600;
    }

    .schema-table {
      width: 100%;
      font-size: 13px;
      border-collapse: collapse;
    }

    .schema-table th,
    .schema-table td {
      padding: 8px;
      text-align: left;
      border-bottom: 1px solid #e0e0e0;
    }

    .schema-table th {
      background: #fafafa;
      font-weight: 600;
    }

    .checkbox-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
      gap: 8px;
      max-height: 200px;
      overflow-y: auto;
      border: 1px solid #e0e0e0;
      border-radius: 4px;
      padding: 10px;
      background: #fafafa;
    }

    .checkbox-label {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 13px;
      cursor: pointer;
    }

    .query-summary {
      background: #f0f8ff;
      border: 1px solid #d0e8f7;
      border-radius: 4px;
      padding: 12px;
      margin-bottom: 15px;
      font-size: 13px;
    }

    .query-summary-title {
      font-weight: 600;
      margin-bottom: 8px;
      color: #4a90e2;
    }

    .empty-state {
      text-align: center;
      padding: 40px;
      color: #999;
    }
  </style>
</head>
<body>
  <div class="header">
    <h1>NBA Statistics</h1>
    
    <div class="dataset-selector">
      <label class="form-label" style="margin: 0;">Dataset:</label>
      <select id="datasetSelect" class="form-select" style="width: auto;">
        <option value="">Choose Dataset</option>
        {% for ds in available_datasets %}
          <option value="{{ ds }}" {% if ds == current_dataset %}selected{% endif %}>{{ ds }}</option>
        {% endfor %}
      </select>
      <button onclick="loadSelectedDataset()" class="btn btn-primary">Load</button>
    </div>
  </div>

  <div class="container">
    {% if not current_dataset %}
    <div class="card">
      <div class="empty-state">
        <h2>Select a dataset to begin</h2>
        <p>Choose a CSV file from the dropdown above</p>
      </div>
    </div>
    {% else %}
    <div class="grid">
      <div class="card">
        <h2 class="card-title">Query Builder</h2>

        {% if query_state.filters or query_state.sort_column or (not query_state.show_all_columns and query_state.selected_columns) or query_state.join_dataset or query_state.aggregation_column %}
        <div class="query-summary">
          <div class="query-summary-title">Active Query Settings</div>
          {% if query_state.filters %}
          <div>Filters: {{ query_state.filters|length }} active</div>
          {% endif %}
          {% if not query_state.show_all_columns and query_state.selected_columns %}
          <div>Columns: {{ query_state.selected_columns|length }} selected</div>
          {% endif %}
          {% if query_state.sort_column %}
          <div>Sort: {{ query_state.sort_column }} ({{ 'DESC' if query_state.sort_order == 'desc' else 'ASC' }})</div>
          {% endif %}
          {% if query_state.join_dataset %}
          <div>Join: {{ query_state.join_dataset }}</div>
          {% endif %}
          {% if query_state.aggregation_column and query_state.aggregation_function %}
          <div>Aggregation: {{ query_state.aggregation_function.upper() }}({{ query_state.aggregation_column }})</div>
          {% elif query_state.aggregation_group_by %}
          <div>Group By: {{ query_state.aggregation_group_by }}</div>
          {% endif %}
          {% if query_state.use_limit %}
          <div>Limit: {{ query_state.limit }} rows</div>
          {% endif %}
        </div>
        {% endif %}

        <div class="tabs">
          <button class="tab-btn active" onclick="switchTab('filter')">Filter</button>
          <button class="tab-btn" onclick="switchTab('columns')">Columns</button>
          <button class="tab-btn" onclick="switchTab('sort')">Sort</button>
          <button class="tab-btn" onclick="switchTab('aggregate')">Aggregate</button>
          <button class="tab-btn" onclick="switchTab('join')">Join</button>
          <button class="tab-btn" onclick="switchTab('limit')">Limit</button>
        </div>

        <div id="tab-filter" class="tab-content active">
          {% if query_state.filters %}
          <div class="filter-list">
            {% for filter in query_state.filters %}
            <div class="filter-item">
              <span>{{ filter.column }} {{ filter.op }} {{ filter.value }}</span>
              <form method="post" action="/?action=remove_filter" style="display: inline;">
                <input type="hidden" name="filter_index" value="{{ loop.index0 }}">
                <button type="submit" class="remove-btn">Remove</button>
              </form>
            </div>
            {% endfor %}
          </div>
          {% else %}
          <div style="padding: 10px; background: #fafafa; border-radius: 4px; margin-bottom: 15px; text-align: center; color: #999;">
            No filters applied
          </div>
          {% endif %}

          <form method="post" action="/?action=add_filter">
            <div class="form-row-3">
              <div class="form-group">
                <label class="form-label">Column</label>
                <select name="filter_column" class="form-select">
                  {% for col in columns %}
                    <option value="{{ col }}">{{ col }}</option>
                  {% endfor %}
                </select>
              </div>
              <div class="form-group">
                <label class="form-label">Operator</label>
                <select name="filter_op" class="form-select">
                  <option value=">">></option>
                  <option value=">=">≥</option>
                  <option value="<"><</option>
                  <option value="<=">≤</option>
                  <option value="==">==</option>
                  <option value="!=">≠</option>
                </select>
              </div>
              <div class="form-group">
                <label class="form-label">Value</label>
                <input type="text" name="filter_value" class="form-input" placeholder="e.g., 30">
              </div>
            </div>
            <div class="button-group">
              <button type="submit" class="btn btn-success">Add Filter</button>
              {% if query_state.filters %}
              <button type="submit" formaction="/?action=clear_filters" class="btn btn-secondary">Clear All</button>
              {% endif %}
            </div>
          </form>
        </div>

        <div id="tab-columns" class="tab-content">
          <form method="post" action="/?action=update_columns">
            <div class="form-group">
              <label class="checkbox-label">
                <input type="checkbox" name="show_all_columns" id="show_all_columns" 
                       {% if query_state.show_all_columns %}checked{% endif %} onchange="toggleColumnSelection()">
                Show All Columns
              </label>
            </div>
            
            <div id="columnSelection" {% if query_state.show_all_columns %}style="display:none"{% endif %}>
              <div class="form-group">
                <label class="form-label">Select Columns</label>
                <div class="checkbox-grid">
                  {% for col in columns %}
                    <label class="checkbox-label">
                      <input type="checkbox" name="selected_columns" value="{{ col }}" 
                             {% if col in query_state.selected_columns %}checked{% endif %}>
                      {{ col }}
                    </label>
                  {% endfor %}
                </div>
              </div>
            </div>
            
            <div class="button-group">
              <button type="submit" class="btn btn-primary">Apply</button>
            </div>
          </form>
        </div>

        <div id="tab-sort" class="tab-content">
          <form method="post" action="/?action=update_sort">
            <div class="form-row">
              <div class="form-group">
                <label class="form-label">Sort By</label>
                <select name="sort_column" class="form-select">
                  <option value="">No Sorting</option>
                  {% for col in columns %}
                    <option value="{{ col }}" {% if query_state.sort_column == col %}selected{% endif %}>{{ col }}</option>
                  {% endfor %}
                </select>
              </div>
              <div class="form-group">
                <label class="form-label">Order</label>
                <select name="sort_order" class="form-select">
                  <option value="desc" {% if query_state.sort_order == 'desc' %}selected{% endif %}>Descending</option>
                  <option value="asc" {% if query_state.sort_order == 'asc' %}selected{% endif %}>Ascending</option>
                </select>
              </div>
            </div>
            <div class="button-group">
              <button type="submit" class="btn btn-primary">Apply</button>
              <button type="submit" formaction="/?action=clear_sort" class="btn btn-secondary">Clear</button>
            </div>
          </form>
        </div>

        <div id="tab-aggregate" class="tab-content">
          <form method="post" action="/?action=update_aggregation">
            <div class="form-group">
              <label class="form-label">Group By</label>
              <select name="aggregation_group_by" class="form-select">
                <option value="">No Grouping</option>
                {% for col in columns %}
                  <option value="{{ col }}" {% if query_state.aggregation_group_by == col %}selected{% endif %}>{{ col }}</option>
                {% endfor %}
              </select>
            </div>
            <div class="form-group">
              <label class="form-label">Function (Optional)</label>
              <select name="aggregation_function" class="form-select">
                <option value="">No Aggregation</option>
                <option value="sum" {% if query_state.aggregation_function == 'sum' %}selected{% endif %}>SUM</option>
                <option value="avg" {% if query_state.aggregation_function == 'avg' %}selected{% endif %}>AVERAGE</option>
                <option value="max" {% if query_state.aggregation_function == 'max' %}selected{% endif %}>MAXIMUM</option>
                <option value="min" {% if query_state.aggregation_function == 'min' %}selected{% endif %}>MINIMUM</option>
                <option value="count" {% if query_state.aggregation_function == 'count' %}selected{% endif %}>COUNT</option>
              </select>
            </div>
            <div class="form-group">
              <label class="form-label">Column (Optional)</label>
              <select name="aggregation_column" class="form-select">
                <option value="">Select Column</option>
                {% for col in columns %}
                  <option value="{{ col }}" {% if query_state.aggregation_column == col %}selected{% endif %}>{{ col }}</option>
                {% endfor %}
              </select>
            </div>
            <div class="button-group">
              <button type="submit" class="btn btn-success">Apply</button>
              {% if query_state.aggregation_column or query_state.aggregation_group_by %}
              <button type="submit" formaction="/?action=clear_aggregation" class="btn btn-secondary">Clear</button>
              {% endif %}
            </div>
          </form>
        </div>

        <div id="tab-join" class="tab-content">
          <form method="post" action="/?action=join_dataset">
            <div class="form-group">
              <label class="form-label">Join With</label>
              <select name="join_dataset" id="joinDatasetSelect" class="form-select" onchange="loadJoinColumns()">
                <option value="">Select Dataset</option>
                {% for ds in available_datasets %}
                  {% if ds != current_dataset %}
                    <option value="{{ ds }}" {% if query_state.join_dataset == ds %}selected{% endif %}>{{ ds }}</option>
                  {% endif %}
                {% endfor %}
              </select>
            </div>
            <div class="form-row">
              <div class="form-group">
                <label class="form-label">Left Column</label>
                <select name="join_left_col" class="form-select">
                  {% for col in schema.keys() %}
                    <option value="{{ col }}" {% if query_state.join_left_col == col %}selected{% endif %}>{{ col }}</option>
                  {% endfor %}
                </select>
              </div>
              <div class="form-group">
                <label class="form-label">Right Column</label>
                <select name="join_right_col" id="joinRightCol" class="form-select">
                  <option value="">Select Column</option>
                </select>
              </div>
            </div>
            <div class="button-group">
              <button type="submit" class="btn btn-success">Apply Join</button>
              {% if query_state.join_dataset %}
              <button type="submit" formaction="/?action=clear_join" class="btn btn-secondary">Clear</button>
              {% endif %}
            </div>
          </form>
        </div>

        <div id="tab-limit" class="tab-content">
          <form method="post" action="/?action=update_limit">
            <div class="form-group">
              <label class="checkbox-label">
                <input type="checkbox" name="use_limit" id="use_limit" 
                       {% if query_state.use_limit %}checked{% endif %} onchange="toggleLimitInput()">
                Enable Row Limit
              </label>
            </div>
            
            <div id="limitInput" {% if not query_state.use_limit %}style="display:none"{% endif %}>
              <div class="form-group">
                <label class="form-label">Number of Rows</label>
                <input type="number" name="limit" class="form-input" 
                       value="{{ query_state.limit }}" min="1" placeholder="50">
              </div>
            </div>
            
            <div class="button-group">
              <button type="submit" class="btn btn-primary">Apply</button>
            </div>
          </form>
        </div>

        <div class="button-group" style="margin-top: 20px; padding-top: 20px; border-top: 1px solid #e0e0e0;">
          <form method="post" action="/?action=execute_query" style="display: inline;">
            <button type="submit" class="btn btn-primary">Run Query</button>
          </form>
          <form method="post" action="/?action=clear_all" style="display: inline;">
            <button type="submit" class="btn btn-danger">Clear All</button>
          </form>
        </div>

        {% if error %}
          <div class="alert alert-error">{{ error }}</div>
        {% endif %}

        {% if success %}
          <div class="alert alert-success">{{ success }}</div>
        {% endif %}

        {% if aggregation_info %}
          <div class="alert alert-info">{{ aggregation_info }}</div>
        {% endif %}

        {% if results %}
          <div class="results-table-wrapper">
            <table class="results-table">
              <thead>
                <tr>
                  {% for col in result_columns %}
                    <th>{{ col }}</th>
                  {% endfor %}
                </tr>
              </thead>
              <tbody>
                {% for row in results %}
                  <tr>
                    {% for col in result_columns %}
                      <td>
                        {% if row[col] is number %}
                          {{ "%.2f"|format(row[col]) if row[col] is float else row[col] }}
                        {% else %}
                          {{ row[col] }}
                        {% endif %}
                      </td>
                    {% endfor %}
                  </tr>
                {% endfor %}
              </tbody>
            </table>
          </div>
          <div style="margin-top: 10px; font-size: 13px; color: #666;">
            Showing {{ results|length }} of {{ total_rows }} row(s) with {{ result_columns|length }} column(s)
          </div>
        {% endif %}
      </div>

      <div class="card">
        <h2 class="card-title">Dataset Info</h2>
        
        <div class="info-grid">
          <div class="info-item">
            <div class="info-label">Rows</div>
            <div class="info-value">{{ row_count }}</div>
          </div>
          <div class="info-item">
            <div class="info-label">Columns</div>
            <div class="info-value">{{ schema|length }}</div>
          </div>
          <div class="info-item">
            <div class="info-label">Types</div>
            <div class="info-value">{{ unique_types }}</div>
          </div>
        </div>

        <h3 style="font-size: 16px; margin-bottom: 10px;">Schema</h3>
        <div style="max-height: 400px; overflow-y: auto;">
          <table class="schema-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Column</th>
                <th>Type</th>
              </tr>
            </thead>
            <tbody>
              {% for col_name, col_type in schema.items() %}
                <tr>
                  <td>{{ loop.index }}</td>
                  <td>{{ col_name }}</td>
                  <td>{{ col_type }}</td>
                </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>

        {% if current_dataset and current_dataset in chunk_stats %}
        <h3 style="font-size: 16px; margin: 20px 0 10px;">Loading Stats</h3>
        <div style="font-size: 13px; color: #666; line-height: 1.8;">
          <div><strong>Strategy:</strong> {{ chunk_stats[current_dataset].get('strategy', 'N/A').upper() }}</div>
          <div><strong>Load Time:</strong> {{ "%.2f"|format(chunk_stats[current_dataset].get('load_time', 0)) }}s</div>
          <div><strong>File Size:</strong> {{ "%.2f"|format(chunk_stats[current_dataset].get('file_size_mb', 0)) }}MB</div>
        </div>
        {% endif %}
      </div>
    </div>
    {% endif %}
  </div>

  <script>
    function switchTab(tabName) {
      document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
      document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
      document.getElementById('tab-' + tabName).classList.add('active');
      event.target.classList.add('active');
    }

    function toggleColumnSelection() {
      const showAll = document.getElementById('show_all_columns').checked;
      document.getElementById('columnSelection').style.display = showAll ? 'none' : 'block';
    }

    function toggleLimitInput() {
      const useLimit = document.getElementById('use_limit').checked;
      document.getElementById('limitInput').style.display = useLimit ? 'block' : 'none';
    }

    function loadJoinColumns() {
      const dataset = document.getElementById('joinDatasetSelect').value;
      const rightColSelect = document.getElementById('joinRightCol');
      
      rightColSelect.innerHTML = '<option value="">Loading...</option>';
      
      if (!dataset) {
        rightColSelect.innerHTML = '<option value="">Select Column</option>';
        return;
      }
      
      fetch('/api/dataset_columns/' + dataset)
        .then(response => response.json())
        .then(data => {
          rightColSelect.innerHTML = '<option value="">Select Column</option>';
          data.columns.forEach(col => {
            const option = document.createElement('option');
            option.value = col;
            option.textContent = col;
            {% if query_state.join_right_col %}
            if (col === '{{ query_state.join_right_col }}') {
              option.selected = true;
            }
            {% endif %}
            rightColSelect.appendChild(option);
          });
        });
    }

    function loadSelectedDataset() {
      const select = document.getElementById('datasetSelect');
      const dataset = select.value;
      
      if (!dataset) {
        alert('Please select a dataset');
        return;
      }

      // Show loading message
      const btn = event.target;
      btn.disabled = true;
      btn.textContent = 'Loading...';

      fetch('/api/load_dataset', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({dataset: dataset})
      })
      .then(response => response.json())
      .then(data => {
        window.location.href = '/?dataset=' + dataset;
      })
      .catch(error => {
        btn.disabled = false;
        btn.textContent = 'Load';
        alert('Error loading dataset: ' + error);
      });
    }

    document.addEventListener('DOMContentLoaded', function() {
      {% if query_state.join_dataset %}
      loadJoinColumns();
      {% endif %}
    });
  </script>
</body>
</html>
"""


@APP.route("/", methods=["GET", "POST"])
def index():
    global active_dataset
    
    new_dataset = request.args.get('dataset')
    if new_dataset:
        previous_dataset = session.get('active_dataset')
        active_dataset = new_dataset
        session['active_dataset'] = active_dataset

        if previous_dataset is None or previous_dataset != active_dataset:
            session['query_state'] = {
                'filters': [],
                'selected_columns': [],
                'sort_column': '',
                'sort_order': 'desc',
                'show_all_columns': True,
                'join_dataset': '',
                'join_left_col': '',
                'join_right_col': '',
                'aggregation_column': '',
                'aggregation_function': '',
                'aggregation_group_by': '',
                'limit': 50,
                'use_limit': True
            }
            session.modified = True
    elif 'active_dataset' in session:
        active_dataset = session['active_dataset']
    else:
        active_dataset = None
    
    available_datasets = get_available_datasets()
    
    if not active_dataset or active_dataset not in parsers:
        empty_query_state = {
            'filters': [],
            'selected_columns': [],
            'sort_column': '',
            'sort_order': 'desc',
            'show_all_columns': True,
            'join_dataset': '',
            'join_left_col': '',
            'join_right_col': '',
            'aggregation_column': '',
            'aggregation_function': '',
            'aggregation_group_by': '',
            'limit': 50,
            'use_limit': True
        }
        return render_template_string(
            PAGE_TEMPLATE,
            available_datasets=available_datasets,
            current_dataset=None,
            chunk_stats={},
            query_state=empty_query_state,
            error=None,
            success=None,
            aggregation_info=None,
            results=[],
            result_columns=[],
            columns=[],
            schema={},
            row_count=0,
            unique_types=0,
            total_rows=0
        )
    
    p = parsers[active_dataset]
    row_count = len(p.data)
    schema = p.get_schema()
    unique_types = len(set(schema.values()))
    
    error = None
    success = None
    aggregation_info = None
    results = []
    result_columns = []
    working_schema = schema
    total_rows = 0
    
    query_state = get_query_state()
    
    if request.method == "POST":
        action = request.args.get("action", "")
        
        try:
            if action == "add_filter":
                filter_col = request.form.get("filter_column")
                filter_op = request.form.get("filter_op")
                filter_val = request.form.get("filter_value")
                
                if filter_col and filter_val:
                    query_state['filters'].append({
                        'column': filter_col,
                        'op': filter_op,
                        'value': filter_val
                    })
                    session.modified = True
                    success = f"Filter added: {filter_col} {filter_op} {filter_val}"
                
            elif action == "remove_filter":
                filter_index = int(request.form.get("filter_index"))
                removed = query_state['filters'].pop(filter_index)
                session.modified = True
                success = f"Filter removed"
                
            elif action == "clear_filters":
                query_state['filters'] = []
                session.modified = True
                success = "All filters cleared"
                
            elif action == "update_columns":
                query_state['show_all_columns'] = 'show_all_columns' in request.form
                if not query_state['show_all_columns']:
                    query_state['selected_columns'] = request.form.getlist("selected_columns")
                session.modified = True
                success = "Column selection updated"
                
            elif action == "update_sort":
                query_state['sort_column'] = request.form.get("sort_column", "")
                query_state['sort_order'] = request.form.get("sort_order", "desc")
                session.modified = True
                success = "Sorting updated"
                
            elif action == "clear_sort":
                query_state['sort_column'] = ""
                session.modified = True
                success = "Sorting cleared"
                
            elif action == "update_aggregation":
                query_state['aggregation_function'] = request.form.get("aggregation_function", "")
                query_state['aggregation_column'] = request.form.get("aggregation_column", "")
                query_state['aggregation_group_by'] = request.form.get("aggregation_group_by", "")
                session.modified = True
                success = "Aggregation updated"
                
            elif action == "clear_aggregation":
                query_state['aggregation_function'] = ""
                query_state['aggregation_column'] = ""
                query_state['aggregation_group_by'] = ""
                session.modified = True
                success = "Aggregation cleared"
                
            elif action == "update_limit":
                query_state['use_limit'] = 'use_limit' in request.form
                if query_state['use_limit']:
                    limit_val = request.form.get("limit", "50")
                    try:
                        query_state['limit'] = int(limit_val)
                    except ValueError:
                        query_state['limit'] = 50
                session.modified = True
                success = "Limit settings updated"
                
            elif action == "join_dataset":
                join_ds = request.form.get("join_dataset")
                join_left = request.form.get("join_left_col")
                join_right = request.form.get("join_right_col")
                
                if join_ds and join_left and join_right:
                    if join_ds not in parsers:
                        filepath = os.path.join(DATA_FOLDER, join_ds)
                        load_dataset_with_progress(filepath, join_ds)
                    
                    query_state['join_dataset'] = join_ds
                    query_state['join_left_col'] = join_left
                    query_state['join_right_col'] = join_right
                    session.modified = True
                    success = f"Join configured with {join_ds}"
                
            elif action == "clear_join":
                query_state['join_dataset'] = ''
                query_state['join_left_col'] = ''
                query_state['join_right_col'] = ''
                session.modified = True
                success = "Join cleared"
                
            elif action == "execute_query":
                success = "Query executed"
            
            elif action == "clear_all":
                session['query_state'] = {
                    'filters': [],
                    'selected_columns': [],
                    'sort_column': '',
                    'sort_order': 'desc',
                    'show_all_columns': True,
                    'join_dataset': '',
                    'join_left_col': '',
                    'join_right_col': '',
                    'aggregation_column': '',
                    'aggregation_function': '',
                    'aggregation_group_by': '',
                    'limit': 50,
                    'use_limit': True
                }
                session.modified = True
                query_state = get_query_state()
                success = "All settings cleared"
            
        except Exception as e:
            error = f"Error: {str(e)}"
    
    if not error:
        results, result_columns, aggregation_info, working_schema, total_rows = execute_query(p, query_state)
    else:
        results = []
        result_columns = []
        aggregation_info = None
        working_schema = schema
        total_rows = 0
    
    columns = list(working_schema.keys()) if working_schema else list(schema.keys())
    
    return render_template_string(
        PAGE_TEMPLATE,
        available_datasets=available_datasets,
        current_dataset=active_dataset,
        chunk_stats=chunk_stats,
        query_state=query_state,
        error=error,
        success=success,
        aggregation_info=aggregation_info,
        results=results,
        result_columns=result_columns,
        columns=columns,
        schema=schema,
        row_count=row_count,
        unique_types=unique_types,
        total_rows=total_rows
    )


if __name__ == "__main__":
    print("Starting CSV Query Tool")
    print(f"Data folder: {DATA_FOLDER}")
    APP.run(debug=True, threaded=True)