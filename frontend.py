import os
import csv
import sqlite3
import itertools
from flask import Flask, request, render_template_string

APP = Flask(__name__)

CSV_PATH = "database_24_25.csv"  
DB_PATH = "stats.db"              
TABLE_NAME = "stats"              


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  
    return conn


def infer_sql_type(values):
    non_empty = [v for v in values if v not in (None, "", "NA", "N/A")]
    if not non_empty:
        return "TEXT"

    try:
        for v in non_empty:
            int(v)
        return "INTEGER"
    except ValueError:
        pass

    try:
        for v in non_empty:
            float(v)
        return "REAL"
    except ValueError:
        pass

    return "TEXT"


def build_db():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found at {CSV_PATH}")

    conn = get_db()
    cur = conn.cursor()

    cur.execute(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)

    sample_rows = rows[:200]
    cols_values = list(zip(*sample_rows)) if sample_rows else [[] for _ in header]
    col_types = [
        infer_sql_type(col_vals) for col_vals in cols_values
    ]

    col_defs = []
    for name, ctype in zip(header, col_types):
        safe_name = name.replace('"', '""')
        col_defs.append(f'"{safe_name}" {ctype}')
    create_sql = f'CREATE TABLE "{TABLE_NAME}" ({", ".join(col_defs)})'
    cur.execute(create_sql)

    placeholders = ", ".join(["?"] * len(header))
    columns_sql = ", ".join(f'"{h.replace(chr(34), chr(34)*2)}"' for h in header)
    insert_sql = f'INSERT INTO "{TABLE_NAME}" ({columns_sql}) VALUES ({placeholders})'
    cur.executemany(insert_sql, rows)

    conn.commit()
    conn.close()


def get_schema_and_count():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    row_count = cur.fetchone()[0]

    cur.execute(f"PRAGMA table_info({TABLE_NAME})")
    schema_info = cur.fetchall()
    conn.close()
    return row_count, schema_info


PAGE_TEMPLATE = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Data2App – SQL Web Console</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root {
      --bg: #0f172a;
      --card: #111827;
      --card-soft: #020617;
      --accent: #38bdf8;
      --accent-soft: rgba(56,189,248,0.15);
      --text: #e5e7eb;
      --muted: #9ca3af;
      --danger: #f97373;
      --radius: 14px;
    }

    * {
      box-sizing: border-box;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "SF Pro Text", sans-serif;
    }

    body {
      margin: 0;
      padding: 0;
      background: radial-gradient(circle at top, #1f2937 0, #020617 55%, #000 100%);
      color: var(--text);
    }

    .app-shell {
      max-width: 1120px;
      margin: 32px auto;
      padding: 0 16px 40px;
    }

    .app-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 24px;
    }

    .title-block h1 {
      margin: 0;
      font-size: 26px;
      letter-spacing: 0.03em;
    }

    .title-block p {
      margin: 4px 0 0;
      color: var(--muted);
      font-size: 13px;
    }

    .tag {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 11px;
      background: var(--accent-soft);
      color: var(--accent);
    }

    .grid {
      display: grid;
      grid-template-columns: minmax(0, 2.1fr) minmax(0, 1.3fr);
      gap: 18px;
    }

    @media (max-width: 900px) {
      .grid {
        grid-template-columns: minmax(0, 1fr);
      }
    }

    .card {
      background: linear-gradient(145deg, rgba(15,23,42,0.97), rgba(17,24,39,0.97));
      border-radius: var(--radius);
      border: 1px solid rgba(148,163,184,0.15);
      box-shadow:
        0 18px 45px rgba(15,23,42,0.75),
        0 0 0 1px rgba(15,23,42,0.9);
      padding: 16px 18px 16px;
    }

    .card-header {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 8px;
      margin-bottom: 8px;
    }

    .card-title {
      font-size: 15px;
      font-weight: 600;
    }

    .card-subtitle {
      font-size: 12px;
      color: var(--muted);
    }

    .pill {
      padding: 2px 8px;
      border-radius: 999px;
      border: 1px solid rgba(148,163,184,0.35);
      font-size: 11px;
      color: var(--muted);
    }

    .summary-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
      margin-top: 8px;
    }

    .summary-item {
      padding: 8px 9px;
      border-radius: 12px;
      background: radial-gradient(circle at top left, rgba(56,189,248,0.15), rgba(15,23,42,0.9));
      border: 1px solid rgba(148,163,184,0.22);
    }

    .summary-label {
      font-size: 11px;
      color: var(--muted);
      margin-bottom: 2px;
    }

    .summary-value {
      font-size: 15px;
      font-weight: 600;
    }

    .schema-table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
      font-size: 12px;
    }

    .schema-table th,
    .schema-table td {
      padding: 5px 6px;
      text-align: left;
    }

    .schema-table thead {
      background: rgba(15,23,42,0.9);
    }

    .schema-table tbody tr:nth-child(odd) {
      background: rgba(15,23,42,0.4);
    }

    .schema-table tbody tr:nth-child(even) {
      background: rgba(15,23,42,0.2);
    }

    .schema-table th {
      font-weight: 500;
      color: var(--muted);
      border-bottom: 1px solid rgba(148,163,184,0.3);
    }

    .schema-table td {
      border-bottom: 1px solid rgba(15,23,42,0.6);
    }

    .sql-textarea {
      width: 100%;
      min-height: 120px;
      padding: 9px 10px;
      border-radius: 10px;
      border: 1px solid rgba(148,163,184,0.35);
      background: radial-gradient(circle at top left, rgba(31,41,55,0.85), rgba(15,23,42,0.95));
      color: var(--text);
      font-size: 13px;
      resize: vertical;
    }

    .sql-textarea:focus {
      outline: 1px solid var(--accent);
      box-shadow: 0 0 0 1px rgba(56,189,248,0.3);
    }

    .button-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-top: 8px;
      gap: 8px;
      flex-wrap: wrap;
    }

    .btn-primary {
      border-radius: 999px;
      border: none;
      padding: 7px 18px;
      font-size: 13px;
      font-weight: 500;
      cursor: pointer;
      background: linear-gradient(135deg, #38bdf8, #0ea5e9);
      color: #0b1220;
      box-shadow: 0 10px 20px rgba(56,189,248,0.45);
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }

    .btn-primary:hover {
      filter: brightness(1.05);
      transform: translateY(-0.5px);
    }

    .btn-ghost {
      border-radius: 999px;
      border: 1px solid rgba(148,163,184,0.5);
      background: transparent;
      padding: 5px 11px;
      font-size: 12px;
      color: var(--muted);
      cursor: pointer;
    }

    .btn-ghost:hover {
      border-color: var(--accent);
      color: var(--accent);
    }

    .examples {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-top: 4px;
    }

    .example-chip {
      font-size: 11px;
      padding: 4px 8px;
      border-radius: 999px;
      border: 1px solid rgba(148,163,184,0.4);
      background: rgba(15,23,42,0.9);
      color: var(--muted);
      cursor: pointer;
    }

    .example-chip:hover {
      border-color: var(--accent);
      color: var(--accent);
    }

    .error {
      margin-top: 8px;
      padding: 6px 9px;
      border-radius: 9px;
      background: rgba(248,113,113,0.12);
      border: 1px solid rgba(248,113,113,0.55);
      color: #fecaca;
      font-size: 12px;
    }

    .result-card {
      margin-top: 14px;
      border-radius: var(--radius);
      background: linear-gradient(135deg, rgba(15,23,42,0.9), rgba(8,17,32,0.98));
      border: 1px solid rgba(148,163,184,0.28);
      padding: 10px 12px 12px;
      max-height: 430px;
      overflow: auto;
    }

    .result-meta {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 8px;
      margin-bottom: 6px;
      font-size: 12px;
      color: var(--muted);
    }

    .results-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }

    .results-table th,
    .results-table td {
      padding: 4px 6px;
      text-align: left;
    }

    .results-table thead {
      position: sticky;
      top: 0;
      background: radial-gradient(circle at top, #020617, #020617);
      z-index: 1;
    }

    .results-table th {
      font-weight: 500;
      border-bottom: 1px solid rgba(148,163,184,0.5);
      color: var(--muted);
    }

    .results-table tbody tr:nth-child(odd) {
      background: rgba(15,23,42,0.85);
    }

    .results-table tbody tr:nth-child(even) {
      background: rgba(15,23,42,0.6);
    }

    .footer-note {
      margin-top: 12px;
      font-size: 11px;
      color: var(--muted);
      text-align: right;
    }
  </style>
</head>
<body>
  <div class="app-shell">
    <header class="app-header">
      <div class="title-block">
        <h1>Data2App · SQL Front-End</h1>
        <p>Run SQL on your basketball game log dataset directly in the browser.</p>
      </div>
      <span class="tag">
        <span>●</span>
        <span>Backend: SQLite · Frontend: Flask</span>
      </span>
    </header>

    <div class="grid">
      <!-- LEFT: SQL console -->
      <section class="card">
        <div class="card-header">
          <div>
            <div class="card-title">SQL Console</div>
            <div class="card-subtitle">Only <code>SELECT</code> queries are allowed.</div>
          </div>
          <span class="pill">{{ table_name }} ({{ row_count }} rows)</span>
        </div>

        <form method="post">
          <textarea
            name="sql"
            class="sql-textarea"
            spellcheck="false"
            placeholder="e.g. SELECT Player, Tm, PTS FROM stats ORDER BY PTS DESC LIMIT 10;"
          >{{ sql_text }}</textarea>

          <div class="button-row">
            <button type="submit" class="btn-primary">
              ▶ Run query
            </button>
            <div style="flex: 1;"></div>
            <button type="button" class="btn-ghost" onclick="document.querySelector('textarea[name=sql]').value=''">
              Clear
            </button>
          </div>

          <div class="examples">
            <span class="card-subtitle" style="margin-right:4px;">Examples:</span>
            {% for ex in examples %}
              <button
                type="button"
                class="example-chip"
                onclick="document.querySelector('textarea[name=sql]').value = this.dataset.sql;"
                data-sql="{{ ex.sql|e }}"
              >
                {{ ex.label }}
              </button>
            {% endfor %}
          </div>
        </form>

        {% if error %}
          <div class="error">{{ error }}</div>
        {% endif %}

        {% if results %}
          <div class="result-card">
            <div class="result-meta">
              <div>{{ results|length }} row(s) returned</div>
              <div>Columns: {{ columns|length }}</div>
            </div>
            <table class="results-table">
              <thead>
                <tr>
                  {% for col in columns %}
                    <th>{{ col }}</th>
                  {% endfor %}
                </tr>
              </thead>
              <tbody>
                {% for row in results %}
                  <tr>
                    {% for col in columns %}
                      <td>{{ row[col] }}</td>
                    {% endfor %}
                  </tr>
                {% endfor %}
              </tbody>
            </table>
          </div>
        {% endif %}
      </section>

      <!-- RIGHT: Schema / summary -->
      <section class="card">
        <div class="card-header">
          <div>
            <div class="card-title">Dataset summary</div>
            <div class="card-subtitle">{{ csv_path }}</div>
          </div>
        </div>

        <div class="summary-grid">
          <div class="summary-item">
            <div class="summary-label">Total rows</div>
            <div class="summary-value">{{ row_count }}</div>
          </div>
          <div class="summary-item">
            <div class="summary-label">Columns</div>
            <div class="summary-value">{{ schema|length }}</div>
          </div>
          <div class="summary-item">
            <div class="summary-label">Table name</div>
            <div class="summary-value">{{ table_name }}</div>
          </div>
        </div>

        <table class="schema-table">
          <thead>
            <tr>
              <th>#</th>
              <th>Column</th>
              <th>Type</th>
            </tr>
          </thead>
          <tbody>
            {% for col in schema %}
              <tr>
                <td>{{ loop.index }}</td>
                <td>{{ col['name'] }}</td>
                <td>{{ col['type'] }}</td>
              </tr>
            {% endfor %}
          </tbody>
        </table>

        <div class="footer-note">
          Tip: try grouping, filters, and ordering:<br>
          <code>SELECT Tm, AVG(PTS) AS avg_pts FROM stats GROUP BY Tm ORDER BY avg_pts DESC LIMIT 10;</code>
        </div>
      </section>
    </div>
  </div>
</body>
</html>
"""

@APP.route("/", methods=["GET", "POST"])
def index():
    row_count, schema_info = get_schema_and_count()

    schema = [
        {"name": col[1], "type": col[2]}
        for col in schema_info
    ]

    default_sql = "SELECT Player, Tm, Opp, PTS FROM stats ORDER BY PTS DESC LIMIT 10;"

    sql_text = default_sql
    error = None
    results = []
    columns = []

    if request.method == "POST":
        sql_text = request.form.get("sql", "").strip()

        if not sql_text:
            error = "Please enter a SQL query."
        elif not sql_text.lower().startswith("select"):
            error = "Only SELECT queries are allowed for this demo."
        else:
            try:
                conn = get_db()
                cur = conn.cursor()
                cur.execute(sql_text)
                fetched = cur.fetchall()
                if cur.description:
                    columns = [d[0] for d in cur.description]
                results = [
                    {col: row[idx] for idx, col in enumerate(columns)}
                    for row in fetched
                ]
                conn.close()
            except sqlite3.Error as e:
                error = f"SQL error: {e}"

    examples = [
        {
            "label": "Top 10 scorers (PTS)",
            "sql": "SELECT Player, Tm, Opp, PTS FROM stats ORDER BY PTS DESC LIMIT 10;"
        },
        {
            "label": "Avg PTS by team",
            "sql": "SELECT Tm, ROUND(AVG(PTS), 2) AS avg_pts FROM stats GROUP BY Tm ORDER BY avg_pts DESC;"
        },
        {
            "label": "Games with 30+ points",
            "sql": "SELECT Player, Tm, Opp, PTS FROM stats WHERE PTS >= 30 ORDER BY PTS DESC;"
        },
        {
            "label": "Top rebounders",
            "sql": "SELECT Player, Tm, TRB, AST, PTS FROM stats ORDER BY TRB DESC LIMIT 10;"
        },
        {
            "label": "Opponents allowing most points",
            "sql": "SELECT Opp, COUNT(*) AS games, ROUND(AVG(PTS),2) AS avg_pts\nFROM stats\nGROUP BY Opp\nHAVING games >= 3\nORDER BY avg_pts DESC;"
        }
    ]

    return render_template_string(
        PAGE_TEMPLATE,
        table_name=TABLE_NAME,
        row_count=row_count,
        schema=schema,
        csv_path=CSV_PATH,
        sql_text=sql_text,
        error=error,
        results=results,
        columns=columns,
        examples=examples,
    )


if __name__ == "__main__":
    build_db()
    APP.run(debug=True)
