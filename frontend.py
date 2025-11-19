from flask import Flask, request, render_template, session, jsonify
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
    if group_by_column and (not aggregation_column or not aggregation_function):
        if not data:
            return data, None
        
        seen = {}
        grouped_data = []
        
        for row in data:
            group_val = row.get(group_by_column)
            if group_val not in seen:
                seen[group_val] = True
                grouped_data.append(row.copy())
        
        return grouped_data, f"Grouped by {group_by_column}"
    
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

    state['selected_columns'] = [
        c for c in state['selected_columns']
        if c in working_schema
      ] 
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

    if state.get('aggregation_column') and state.get('aggregation_function'):
        working_data, aggregation_info = apply_aggregation(
            working_data,
            state['aggregation_column'],
            state['aggregation_function'],
            state.get('aggregation_group_by', '')
        )
        if working_data:
            columns = list(working_data[0].keys())
    elif state.get('aggregation_group_by'):
        working_data, aggregation_info = apply_aggregation(
            working_data,
            None,
            None,
            state.get('aggregation_group_by', '')
        )
        if working_data:
            columns = list(working_data[0].keys())

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
    
    load_dataset_with_progress(filepath, dataset_name)
    
    return jsonify({'status': 'complete', 'dataset': dataset_name})


@APP.route("/", methods=["GET", "POST"])
def index():
    global active_dataset
    error = None
    success = None
    
    new_dataset = request.args.get('dataset')
    if new_dataset:
        previous_dataset = session.get('active_dataset')
        active_dataset = new_dataset
        session['active_dataset'] = active_dataset

        if previous_dataset is None or previous_dataset != active_dataset:
            session['query_state'] = {
                'filters': [],
                'selected_columns': [],
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
        return render_template(
            'index.html',
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
    
    return render_template(
        'index.html',
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