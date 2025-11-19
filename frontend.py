"""
Enhanced Flask Web Application for CSVParser
Features:
- Multiple dataset loading with live chunk progress
- Multiple filters support  
- Dataset join functionality
- Performance optimizations with caching
- Dynamic chunk size based on file size
"""

from flask import Flask, request, render_template_string, session, jsonify
from csv_parser import CSVParser
import time
import os
import glob
import threading

APP = Flask(__name__)
APP.secret_key = 'csv-parser-secret-key-2024'

# Configuration
DATA_FOLDER = "data"
parsers = {}  # Cache multiple parsers {dataset_name: parser}
chunk_stats = {}  # Loading stats {dataset_name: stats}
loading_progress = {}  # Real-time loading progress {dataset_name: progress}
active_dataset = None


def get_chunk_size(file_size_mb):
    """Dynamically determine chunk size based on file size"""
    if file_size_mb < 1:
        return None  # Load entire file
    elif file_size_mb < 10:
        return 1000
    elif file_size_mb < 50:
        return 500
    elif file_size_mb < 100:
        return 200
    else:
        return 100


def load_dataset_with_progress(filepath, dataset_name):
    """Load dataset with real-time progress tracking"""
    global parsers, chunk_stats, loading_progress
    
    start_time = time.time()
    parser = CSVParser(filepath)
    
    file_size = os.path.getsize(filepath)
    file_size_mb = file_size / (1024 * 1024)
    chunk_size = get_chunk_size(file_size_mb)
    
    loading_progress[dataset_name] = {
        'status': 'loading',
        'chunks_processed': 0,
        'total_rows': 0,
        'percent': 0
    }
    
    if chunk_size is None:
        # Small file - load all at once
        parser.parse(type_inference=True)
        chunk_stats[dataset_name] = {
            'strategy': 'full',
            'chunks_processed': 1,
            'total_rows': len(parser.data),
            'load_time': time.time() - start_time,
            'file_size_mb': file_size_mb,
            'chunk_size': 'N/A'
        }
        loading_progress[dataset_name]['status'] = 'complete'
    else:
        # Large file - chunked loading
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
            
            # Update progress
            loading_progress[dataset_name]['chunks_processed'] = chunk_stats[dataset_name]['chunks_processed']
            loading_progress[dataset_name]['total_rows'] = chunk_stats[dataset_name]['total_rows']
            
        # Infer schema after all chunks loaded
        if parser.data:
            parser._infer_schema_all_rows()
        
        chunk_stats[dataset_name]['load_time'] = time.time() - start_time
        loading_progress[dataset_name]['status'] = 'complete'
        loading_progress[dataset_name]['percent'] = 100
    
    parsers[dataset_name] = parser
    print(f"[{dataset_name}] Loaded {chunk_stats[dataset_name]['total_rows']} rows in {chunk_stats[dataset_name]['load_time']:.2f}s")


def get_available_datasets():
    """Get list of CSV files in data folder"""
    csv_files = glob.glob(os.path.join(DATA_FOLDER, "*.csv"))
    return [os.path.basename(f) for f in csv_files]


def get_query_state():
    """Get current query state from session with proper defaults"""
    if 'query_state' not in session:
        session['query_state'] = {
            'filters': [],
            'selected_columns': [],
            'sort_column': '',
            'sort_order': 'desc',
            'show_all_columns': True,
            'join_dataset': '',
            'join_left_col': '',
            'join_right_col': ''
        }
    
    # Ensure all keys exist (for backwards compatibility)
    state = session['query_state']
    defaults = {
        'filters': [],
        'selected_columns': [],
        'sort_column': '',
        'sort_order': 'desc',
        'show_all_columns': True,
        'join_dataset': '',
        'join_left_col': '',
        'join_right_col': ''
    }
    
    for key, default_value in defaults.items():
        if key not in state:
            state[key] = default_value
    
    session['query_state'] = state
    return state


def apply_filters(data, filters, schema):
    """Apply multiple filters to data"""
    if not filters:
        return data
    
    filtered_data = data
    for f in filters:
        if not f.get('column') or not f.get('value'):
            continue
            
        col = f['column']
        op = f['op']
        val = f['value']
        
        # Type inference
        try:
            val = int(val)
        except ValueError:
            try:
                val = float(val)
            except ValueError:
                pass
        
        # Build condition
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
        
        # Filter data
        temp_parser = CSVParser.__new__(CSVParser)
        temp_parser.data = filtered_data
        temp_parser.schema = schema
        filtered_data = temp_parser.filter_rows(condition)
    
    return filtered_data


def execute_query(p, state):
    """Execute the combined query with optimizations"""
    working_data = p.data
    columns = list(p.schema.keys())
    
    # Ensure state has all required keys
    if not isinstance(state, dict):
        state = {}
    
    # Step 1: Apply multiple filters
    if state.get('filters'):
        working_data = apply_filters(working_data, state['filters'], p.schema)
    
    # Step 2: Apply column selection if not "show all"
    if not state.get('show_all_columns', True) and state.get('selected_columns'):
        temp_parser = CSVParser.__new__(CSVParser)
        temp_parser.data = working_data
        temp_parser.schema = p.schema
        working_data = temp_parser.filter_columns(state['selected_columns'])
        columns = state['selected_columns']
    
    # Step 3: Apply sorting if exists
    if state.get('sort_column'):
        temp_parser = CSVParser.__new__(CSVParser)
        temp_parser.data = working_data
        temp_parser.schema = {k: v for k, v in p.schema.items() if k in columns}
        working_data = temp_parser.sort_data(state['sort_column'], reverse=(state.get('sort_order', 'desc') == 'desc'))
    
    return working_data, columns


@APP.route("/api/loading_progress/<dataset_name>")
def get_loading_progress(dataset_name):
    """API endpoint for real-time loading progress"""
    if dataset_name in loading_progress:
        return jsonify(loading_progress[dataset_name])
    return jsonify({'status': 'not_found'})


@APP.route("/api/load_dataset", methods=["POST"])
def load_dataset():
    """API endpoint to load a dataset"""
    data = request.get_json()
    dataset_name = data.get('dataset')
    
    if not dataset_name:
        return jsonify({'error': 'No dataset specified'}), 400
    
    filepath = os.path.join(DATA_FOLDER, dataset_name)
    if not os.path.exists(filepath):
        return jsonify({'error': 'Dataset not found'}), 404
    
    # Load in background thread
    thread = threading.Thread(target=load_dataset_with_progress, args=(filepath, dataset_name))
    thread.start()
    
    return jsonify({'status': 'loading_started', 'dataset': dataset_name})


# Main HTML Template
PAGE_TEMPLATE = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Basketball Stats Query Tool</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root {
      --primary: #2563eb;
      --primary-soft: rgba(37,99,235,0.1);
      --success: #16a34a;
      --danger: #dc2626;
      --bg: #f8fafc;
      --card: #ffffff;
      --text: #1e293b;
      --text-muted: #64748b;
      --border: #e2e8f0;
    }

    * {
      box-sizing: border-box;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }

    body {
      margin: 0;
      padding: 0;
      background: var(--bg);
      color: var(--text);
    }

    .container {
      max-width: 1400px;
      margin: 0 auto;
      padding: 24px;
    }

    .header {
      background: white;
      border-bottom: 1px solid var(--border);
      padding: 20px 24px;
      margin-bottom: 24px;
    }

    .header h1 {
      margin: 0 0 8px 0;
      font-size: 28px;
      color: var(--text);
    }

    .dataset-selector {
      display: flex;
      gap: 12px;
      align-items: center;
      margin-top: 16px;
    }

    .form-select {
      padding: 10px 12px;
      border: 1px solid var(--border);
      border-radius: 6px;
      font-size: 14px;
      min-width: 200px;
    }

    .btn {
      padding: 10px 20px;
      border: none;
      border-radius: 6px;
      font-size: 14px;
      font-weight: 500;
      cursor: pointer;
      transition: all 0.2s;
    }

    .btn-primary {
      background: var(--primary);
      color: white;
    }

    .btn-primary:hover {
      background: #1d4ed8;
    }

    .btn-secondary {
      background: #f1f5f9;
      color: var(--text);
      border: 1px solid var(--border);
    }

    .btn-success {
      background: var(--success);
      color: white;
    }

    .btn-danger {
      background: #fee2e2;
      color: var(--danger);
      border: 1px solid #fecaca;
    }

    .loading-card {
      background: white;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 16px;
      margin-top: 12px;
    }

    .progress-bar {
      width: 100%;
      height: 24px;
      background: #f1f5f9;
      border-radius: 12px;
      overflow: hidden;
      position: relative;
    }

    .progress-fill {
      height: 100%;
      background: linear-gradient(90deg, var(--success), #22c55e);
      transition: width 0.3s;
      display: flex;
      align-items: center;
      justify-content: center;
      color: white;
      font-size: 12px;
      font-weight: 600;
    }

    .stats-bar {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 12px;
    }

    .stat-badge {
      padding: 6px 12px;
      background: var(--primary-soft);
      color: var(--primary);
      border-radius: 6px;
      font-size: 12px;
      font-weight: 500;
    }

    .grid {
      display: grid;
      grid-template-columns: 2fr 1fr;
      gap: 24px;
    }

    @media (max-width: 1024px) {
      .grid {
        grid-template-columns: 1fr;
      }
    }

    .card {
      background: white;
      border-radius: 12px;
      border: 1px solid var(--border);
      padding: 20px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }

    .card-title {
      font-size: 18px;
      font-weight: 600;
      margin: 0 0 16px 0;
      color: var(--text);
    }

    .tabs {
      display: flex;
      gap: 8px;
      border-bottom: 2px solid var(--border);
      margin-bottom: 20px;
    }

    .tab-btn {
      padding: 12px 20px;
      border: none;
      background: transparent;
      color: var(--text-muted);
      cursor: pointer;
      font-size: 14px;
      font-weight: 500;
      border-bottom: 2px solid transparent;
      margin-bottom: -2px;
    }

    .tab-btn.active {
      color: var(--primary);
      border-bottom-color: var(--primary);
    }

    .tab-content {
      display: none;
    }

    .tab-content.active {
      display: block;
    }

    .form-group {
      margin-bottom: 16px;
    }

    .form-label {
      display: block;
      font-size: 13px;
      font-weight: 500;
      color: var(--text);
      margin-bottom: 6px;
    }

    .form-input {
      width: 100%;
      padding: 10px 12px;
      border: 1px solid var(--border);
      border-radius: 6px;
      font-size: 14px;
      color: var(--text);
    }

    .form-input:focus, .form-select:focus {
      outline: none;
      border-color: var(--primary);
      box-shadow: 0 0 0 3px var(--primary-soft);
    }

    .form-row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
    }

    .form-row-3 {
      display: grid;
      grid-template-columns: 2fr 1fr 2fr;
      gap: 12px;
    }

    .filter-list {
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 12px;
      background: #f8fafc;
      margin-bottom: 12px;
    }

    .filter-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 8px 12px;
      background: white;
      border: 1px solid var(--border);
      border-radius: 4px;
      margin-bottom: 8px;
    }

    .filter-item:last-child {
      margin-bottom: 0;
    }

    .remove-filter-btn {
      background: #fee2e2;
      color: var(--danger);
      border: none;
      padding: 4px 8px;
      border-radius: 4px;
      font-size: 12px;
      cursor: pointer;
    }

    .checkbox-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
      gap: 8px;
      max-height: 200px;
      overflow-y: auto;
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 12px;
      background: #f8fafc;
    }

    .checkbox-label {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 13px;
      cursor: pointer;
    }

    .button-group {
      display: flex;
      gap: 12px;
      margin-top: 20px;
      flex-wrap: wrap;
    }

    .alert {
      padding: 12px 16px;
      border-radius: 6px;
      margin-top: 16px;
      font-size: 14px;
    }

    .alert-error {
      background: #fee2e2;
      color: #991b1b;
      border: 1px solid #fecaca;
    }

    .alert-success {
      background: #dcfce7;
      color: #166534;
      border: 1px solid #bbf7d0;
    }

    .results-card {
      margin-top: 20px;
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
      background: white;
    }

    .results-header {
      padding: 12px 16px;
      background: #f8fafc;
      border-bottom: 1px solid var(--border);
      font-size: 13px;
      color: var(--text-muted);
    }

    .results-table-wrapper {
      max-height: 500px;
      overflow: auto;
    }

    .results-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }

    .results-table th,
    .results-table td {
      padding: 10px 12px;
      text-align: left;
      border-bottom: 1px solid var(--border);
    }

    .results-table th {
      background: #f8fafc;
      font-weight: 600;
      color: var(--text);
      position: sticky;
      top: 0;
      z-index: 1;
    }

    .results-table tbody tr:hover {
      background: #f8fafc;
    }

    .current-query-box {
      background: #eff6ff;
      border: 1px solid #bfdbfe;
      border-radius: 8px;
      padding: 12px;
      margin-bottom: 16px;
    }

    .current-query-title {
      font-size: 12px;
      font-weight: 600;
      color: var(--primary);
      margin-bottom: 8px;
    }

    .current-query-item {
      font-size: 13px;
      color: var(--text);
      margin-bottom: 4px;
    }

    .show-all-toggle {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 12px;
      background: #f8fafc;
      border-radius: 6px;
      margin-bottom: 12px;
    }

    .info-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 16px;
      margin-bottom: 20px;
    }

    .info-item {
      padding: 16px;
      background: #f8fafc;
      border-radius: 8px;
      border: 1px solid var(--border);
    }

    .info-label {
      font-size: 12px;
      color: var(--text-muted);
      margin-bottom: 4px;
    }

    .info-value {
      font-size: 20px;
      font-weight: 600;
      color: var(--text);
    }

    .schema-table {
      width: 100%;
      font-size: 13px;
      border-collapse: collapse;
    }

    .schema-table th,
    .schema-table td {
      padding: 8px 12px;
      text-align: left;
      border-bottom: 1px solid var(--border);
    }

    .schema-table th {
      background: #f8fafc;
      font-weight: 600;
      color: var(--text);
    }

    .join-section {
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 12px;
      background: #f8fafc;
      margin-bottom: 12px;
    }
  </style>
</head>
<body>
  <div class="header">
    <h1>🏀 Basketball Stats Query Tool</h1>
    
    <div class="dataset-selector">
      <label style="font-size: 14px; font-weight: 500;">Select Dataset:</label>
      <select id="datasetSelect" class="form-select">
        <option value="">-- Choose Dataset --</option>
        {% for ds in available_datasets %}
          <option value="{{ ds }}" {% if ds == current_dataset %}selected{% endif %}>{{ ds }}</option>
        {% endfor %}
      </select>
      <button onclick="loadSelectedDataset()" class="btn btn-primary">📂 Load Dataset</button>
    </div>

    <div id="loadingSection" style="display: none;">
      <div class="loading-card">
        <div style="font-size: 14px; font-weight: 600; margin-bottom: 8px;">Loading: <span id="loadingDatasetName"></span></div>
        <div class="progress-bar">
          <div id="progressFill" class="progress-fill" style="width: 0%">0%</div>
        </div>
        <div style="margin-top: 8px; font-size: 13px; color: var(--text-muted);">
          <span id="loadingStats">Initializing...</span>
        </div>
      </div>
    </div>

    {% if current_dataset and current_dataset in chunk_stats %}
    <div class="stats-bar">
      <span class="stat-badge">{{ chunk_stats[current_dataset].get('strategy', 'N/A').upper() }} Loading</span>
      <span class="stat-badge">{{ chunk_stats[current_dataset].get('total_rows', 0) }} Total Rows</span>
      <span class="stat-badge">{{ "%.2f"|format(chunk_stats[current_dataset].get('load_time', 0)) }}s Load Time</span>
      <span class="stat-badge">{{ "%.2f"|format(chunk_stats[current_dataset].get('file_size_mb', 0)) }}MB File Size</span>
      {% if chunk_stats[current_dataset].get('chunk_size') != 'N/A' %}
      <span class="stat-badge">Chunk Size: {{ chunk_stats[current_dataset].get('chunk_size', 0) }} rows</span>
      {% endif %}
    </div>
    {% endif %}
  </div>

  <div class="container">
    {% if not current_dataset %}
    <div class="card">
      <h2 style="text-align: center; color: var(--text-muted);">👆 Please select and load a dataset to begin</h2>
    </div>
    {% else %}
    <div class="grid">
      <!-- LEFT: Query Builder -->
      <div class="card">
        <h2 class="card-title">Build Your Query</h2>

        <!-- Current Query Summary -->
        {% if query_state.filters or query_state.sort_column or (not query_state.show_all_columns and query_state.selected_columns) or query_state.join_dataset %}
        <div class="current-query-box">
          <div class="current-query-title">🔍 Active Query Settings:</div>
          {% if query_state.filters %}
          <div class="current-query-item">• {{ query_state.filters|length }} filter(s) applied</div>
          {% endif %}
          {% if not query_state.show_all_columns and query_state.selected_columns %}
          <div class="current-query-item">• Showing {{ query_state.selected_columns|length }} column(s)</div>
          {% endif %}
          {% if query_state.sort_column %}
          <div class="current-query-item">• Sorted by: {{ query_state.sort_column }} ({{ 'Highest to Lowest' if query_state.sort_order == 'desc' else 'Lowest to Highest' }})</div>
          {% endif %}
          {% if query_state.join_dataset %}
          <div class="current-query-item">• Joined with: {{ query_state.join_dataset }}</div>
          {% endif %}
        </div>
        {% endif %}

        <div class="tabs">
          <button class="tab-btn active" onclick="switchTab('filter')">📊 Filter Data</button>
          <button class="tab-btn" onclick="switchTab('columns')">📋 Select Columns</button>
          <button class="tab-btn" onclick="switchTab('sort')">⬆️ Sort Results</button>
          <button class="tab-btn" onclick="switchTab('join')">🔗 Join Data</button>
        </div>

        <!-- Filter Tab -->
        <div id="tab-filter" class="tab-content active">
          <h3 style="font-size: 15px; margin-bottom: 12px;">Active Filters</h3>
          
          {% if query_state.filters %}
          <div class="filter-list">
            {% for filter in query_state.filters %}
            <div class="filter-item">
              <span>{{ filter.column }} {{ filter.op }} {{ filter.value }}</span>
              <form method="post" action="/?action=remove_filter" style="display: inline;">
                <input type="hidden" name="filter_index" value="{{ loop.index0 }}">
                <button type="submit" class="remove-filter-btn">✕ Remove</button>
              </form>
            </div>
            {% endfor %}
          </div>
          {% else %}
          <div style="padding: 12px; background: #f8fafc; border-radius: 6px; margin-bottom: 12px; text-align: center; color: var(--text-muted);">
            No filters applied
          </div>
          {% endif %}

          <h3 style="font-size: 15px; margin: 16px 0 12px;">Add New Filter</h3>
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
                <label class="form-label">Condition</label>
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
              <button type="submit" class="btn btn-success">➕ Add Filter</button>
              {% if query_state.filters %}
              <button type="submit" formaction="/?action=clear_filters" class="btn btn-danger">🗑️ Clear All Filters</button>
              {% endif %}
            </div>
          </form>
        </div>

        <!-- Columns Tab -->
        <div id="tab-columns" class="tab-content">
          <form method="post" action="/?action=update_columns">
            <div class="show-all-toggle">
              <input type="checkbox" name="show_all_columns" id="show_all_columns" 
                     {% if query_state.show_all_columns %}checked{% endif %} onchange="toggleColumnSelection()">
              <label for="show_all_columns" style="cursor: pointer;">Show All Columns</label>
            </div>
            
            <div id="columnSelection" {% if query_state.show_all_columns %}style="display:none"{% endif %}>
              <div class="form-group">
                <label class="form-label">Select Which Columns to Show</label>
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
              <button type="submit" class="btn btn-primary">✓ Apply Column Selection</button>
            </div>
          </form>
        </div>

        <!-- Sort Tab -->
        <div id="tab-sort" class="tab-content">
          <form method="post" action="/?action=update_sort">
            <div class="form-row">
              <div class="form-group">
                <label class="form-label">Sort By Column</label>
                <select name="sort_column" class="form-select">
                  <option value="">-- No Sorting --</option>
                  {% for col in columns %}
                    <option value="{{ col }}" {% if query_state.sort_column == col %}selected{% endif %}>{{ col }}</option>
                  {% endfor %}
                </select>
              </div>
              <div class="form-group">
                <label class="form-label">Sort Order</label>
                <select name="sort_order" class="form-select">
                  <option value="desc" {% if query_state.sort_order == 'desc' %}selected{% endif %}>Highest to Lowest</option>
                  <option value="asc" {% if query_state.sort_order == 'asc' %}selected{% endif %}>Lowest to Highest</option>
                </select>
              </div>
            </div>
            <div class="button-group">
              <button type="submit" class="btn btn-primary">✓ Apply Sorting</button>
              <button type="submit" formaction="/?action=clear_sort" class="btn btn-secondary">✕ Remove Sorting</button>
            </div>
          </form>
        </div>

        <!-- Join Tab -->
        <div id="tab-join" class="tab-content">
          <form method="post" action="/?action=join_dataset">
            <div class="form-group">
              <label class="form-label">Dataset to Join</label>
              <select name="join_dataset" class="form-select">
                <option value="">-- Select Dataset --</option>
                {% for ds in available_datasets %}
                  {% if ds != current_dataset %}
                    <option value="{{ ds }}" {% if query_state.join_dataset == ds %}selected{% endif %}>{{ ds }}</option>
                  {% endif %}
                {% endfor %}
              </select>
            </div>
            <div class="form-row">
              <div class="form-group">
                <label class="form-label">Join On (Current Dataset)</label>
                <select name="join_left_col" class="form-select">
                  {% for col in columns %}
                    <option value="{{ col }}" {% if query_state.join_left_col == col %}selected{% endif %}>{{ col }}</option>
                  {% endfor %}
                </select>
              </div>
              <div class="form-group">
                <label class="form-label">Join On (Other Dataset)</label>
                <select name="join_right_col" class="form-select">
                  <option value="">-- Select Column --</option>
                </select>
              </div>
            </div>
            <div class="button-group">
              <button type="submit" class="btn btn-success">🔗 Apply Join</button>
              {% if query_state.join_dataset %}
              <button type="submit" formaction="/?action=clear_join" class="btn btn-secondary">✕ Remove Join</button>
              {% endif %}
            </div>
          </form>
        </div>

        <div class="button-group" style="margin-top: 24px; padding-top: 24px; border-top: 1px solid var(--border);">
          <form method="post" action="/?action=execute_query" style="display: inline;">
            <button type="submit" class="btn btn-primary">▶ Run Query</button>
          </form>
          <form method="post" action="/?action=clear_all" style="display: inline;">
            <button type="submit" class="btn btn-danger">🗑️ Clear All Settings</button>
          </form>
        </div>

        {% if error %}
          <div class="alert alert-error">{{ error }}</div>
        {% endif %}

        {% if success %}
          <div class="alert alert-success">{{ success }}</div>
        {% endif %}

        {% if results %}
          <div class="results-card">
            <div class="results-header">
              Showing {{ results|length }} result(s) with {{ result_columns|length }} column(s)
            </div>
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
          </div>
        {% endif %}
      </div>

      <!-- RIGHT: Dataset Info -->
      <div class="card">
        <h2 class="card-title">Dataset Information</h2>
        
        <div class="info-grid">
          <div class="info-item">
            <div class="info-label">Total Rows</div>
            <div class="info-value">{{ row_count }}</div>
          </div>
          <div class="info-item">
            <div class="info-label">Columns</div>
            <div class="info-value">{{ schema|length }}</div>
          </div>
          <div class="info-item">
            <div class="info-label">Data Types</div>
            <div class="info-value">{{ unique_types }}</div>
          </div>
        </div>

        <h3 style="font-size: 16px; margin-bottom: 12px;">Column Details</h3>
        <div style="max-height: 400px; overflow-y: auto;">
          <table class="schema-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Column Name</th>
                <th>Data Type</th>
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

        <h3 style="font-size: 16px; margin: 20px 0 12px;">Loading Performance</h3>
        <div style="font-size: 13px; line-height: 1.8; color: var(--text-muted);">
          <div><strong>Strategy:</strong> {{ chunk_stats[current_dataset].get('strategy', 'N/A').upper() }}</div>
          <div><strong>Chunks Processed:</strong> {{ chunk_stats[current_dataset].get('chunks_processed', 0) }}</div>
          <div><strong>Load Time:</strong> {{ "%.2f"|format(chunk_stats[current_dataset].get('load_time', 0)) }}s</div>
          <div><strong>File Size:</strong> {{ "%.2f"|format(chunk_stats[current_dataset].get('file_size_mb', 0)) }}MB</div>
          {% if chunk_stats[current_dataset].get('chunk_size') != 'N/A' %}
          <div><strong>Chunk Size:</strong> {{ chunk_stats[current_dataset].get('chunk_size', 0) }} rows/chunk</div>
          <div style="margin-top: 8px; padding: 8px; background: #eff6ff; border-radius: 4px; font-size: 12px;">
            💡 <strong>Scaling Strategy:</strong> Larger files use smaller chunks for memory efficiency
          </div>
          {% endif %}
        </div>
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

    function loadSelectedDataset() {
      const select = document.getElementById('datasetSelect');
      const dataset = select.value;
      
      if (!dataset) {
        alert('Please select a dataset');
        return;
      }

      // Show loading section
      document.getElementById('loadingSection').style.display = 'block';
      document.getElementById('loadingDatasetName').textContent = dataset;

      // Start loading
      fetch('/api/load_dataset', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({dataset: dataset})
      })
      .then(response => response.json())
      .then(data => {
        if (data.error) {
          alert('Error: ' + data.error);
          return;
        }

        // Poll for progress
        const interval = setInterval(() => {
          fetch('/api/loading_progress/' + dataset)
            .then(r => r.json())
            .then(progress => {
              if (progress.status === 'loading') {
                const percent = progress.chunks_processed * 10; // Approximate
                document.getElementById('progressFill').style.width = Math.min(percent, 99) + '%';
                document.getElementById('progressFill').textContent = Math.min(percent, 99) + '%';
                document.getElementById('loadingStats').textContent = 
                  `Processed ${progress.chunks_processed} chunk(s), ${progress.total_rows} rows loaded...`;
              } else if (progress.status === 'complete') {
                clearInterval(interval);
                document.getElementById('progressFill').style.width = '100%';
                document.getElementById('progressFill').textContent = '100%';
                document.getElementById('loadingStats').textContent = 'Complete! Reloading page...';
                
                // Redirect to show loaded dataset
                setTimeout(() => {
                  window.location.href = '/?dataset=' + dataset;
                }, 1000);
              }
            });
        }, 500);
      });
    }
  </script>
</body>
</html>
"""


@APP.route("/", methods=["GET", "POST"])
def index():
    """Main route with multi-dataset support and multiple filters"""
    global active_dataset
    
    # Get or set active dataset
    if request.args.get('dataset'):
        active_dataset = request.args.get('dataset')
        session['active_dataset'] = active_dataset
    elif 'active_dataset' in session:
        active_dataset = session['active_dataset']
    
    # Get available datasets
    available_datasets = get_available_datasets()
    
    # If no dataset loaded, show selection screen
    if not active_dataset or active_dataset not in parsers:
        empty_query_state = {
            'filters': [],
            'selected_columns': [],
            'sort_column': '',
            'sort_order': 'desc',
            'show_all_columns': True,
            'join_dataset': '',
            'join_left_col': '',
            'join_right_col': ''
        }
        return render_template_string(
            PAGE_TEMPLATE,
            available_datasets=available_datasets,
            current_dataset=None,
            chunk_stats={},
            query_state=empty_query_state,
            error=None,
            success=None,
            results=[],
            result_columns=[],
            columns=[],
            schema={},
            row_count=0,
            unique_types=0
        )
    
    p = parsers[active_dataset]
    row_count = len(p.data)
    schema = p.get_schema()
    columns = list(schema.keys())
    unique_types = len(set(schema.values()))
    
    error = None
    success = None
    results = []
    result_columns = []
    
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
                success = f"Filter removed: {removed['column']} {removed['op']} {removed['value']}"
                
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
                success = "Sorting removed"
                
            elif action == "join_dataset":
                join_ds = request.form.get("join_dataset")
                join_left = request.form.get("join_left_col")
                join_right = request.form.get("join_right_col")
                
                if join_ds and join_left and join_right:
                    # Load join dataset if not already loaded
                    if join_ds not in parsers:
                        filepath = os.path.join(DATA_FOLDER, join_ds)
                        load_dataset_with_progress(filepath, join_ds)
                    
                    # Perform join
                    other_data = parsers[join_ds].data
                    joined_data = p.join(other_data, join_left, join_right)
                    
                    # Store join params
                    query_state['join_dataset'] = join_ds
                    query_state['join_left_col'] = join_left
                    query_state['join_right_col'] = join_right
                    session.modified = True
                    
                    results = joined_data
                    result_columns = list(joined_data[0].keys()) if joined_data else []
                    success = f"Joined with {join_ds} on {join_left} = {join_right}"
                
            elif action == "clear_join":
                query_state['join_dataset'] = ''
                query_state['join_left_col'] = ''
                query_state['join_right_col'] = ''
                session.modified = True
                success = "Join removed"
                
            elif action == "execute_query" or action == "clear_all":
                if action == "clear_all":
                    session['query_state'] = {
                        'filters': [],
                        'selected_columns': [],
                        'sort_column': '',
                        'sort_order': 'desc',
                        'show_all_columns': True,
                        'join_dataset': '',
                        'join_left_col': '',
                        'join_right_col': ''
                    }
                    session.modified = True
                    query_state = get_query_state()
                    success = "All settings cleared"
            
            # Execute query
            if action != "join_dataset":
                results, result_columns = execute_query(p, query_state)
                
        except Exception as e:
            error = f"Error: {str(e)}"
    
    # Default: show results with current query state
    if not results and not error:
        results, result_columns = execute_query(p, query_state)
    
    return render_template_string(
        PAGE_TEMPLATE,
        available_datasets=available_datasets,
        current_dataset=active_dataset,
        chunk_stats=chunk_stats,
        query_state=query_state,
        error=error,
        success=success,
        results=results,
        result_columns=result_columns,
        columns=columns,
        schema=schema,
        row_count=row_count,
        unique_types=unique_types
    )


if __name__ == "__main__":
    print("\n" + "="*60)
    print("Starting Enhanced Basketball Stats Query Tool")
    print("="*60)
    print(f"Data folder: {DATA_FOLDER}")
    print("="*60 + "\n")
    APP.run(debug=True, threaded=True)