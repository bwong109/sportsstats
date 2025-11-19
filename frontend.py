from flask import Flask, request, render_template_string
from csv_parser import CSVParser  

# -----------------------------
# App + DataFrame-style setup
# -----------------------------
APP = Flask(__name__)

CSV_PATH = "data/database_24_25.csv"

parser = CSVParser(CSV_PATH)
parser.parse(type_inference=True)  

DATA = parser.get_data()          
SCHEMA = parser.get_schema()       
COLUMNS = list(SCHEMA.keys())


# -----------------------------
# Helper: pretty display
# -----------------------------
def to_table_rows(list_of_dicts, cols=None):
    if not list_of_dicts:
        return [], []

    if cols is None:
        cols = list(list_of_dicts[0].keys())

    rows = []
    for row in list_of_dicts:
        rows.append([row.get(c, "") for c in cols])
    return cols, rows


# -----------------------------
# HTML
# -----------------------------
PAGE = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Data2App · Custom DataFrame Frontend</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root {
      --bg: #020617;
      --card: #0b1220;
      --accent: #38bdf8;
      --accent-soft: rgba(56,189,248,0.15);
      --text: #e5e7eb;
      --muted: #9ca3af;
      --radius: 14px;
    }
    * {
      box-sizing: border-box;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
    }
    body {
      margin: 0;
      padding: 24px 16px 40px;
      background: radial-gradient(circle at top, #1f2937 0, #020617 55%, #000 100%);
      color: var(--text);
    }
    .shell {
      max-width: 1200px;
      margin: 0 auto;
    }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
      margin-bottom: 20px;
    }
    h1 {
      margin: 0;
      font-size: 26px;
      letter-spacing: 0.03em;
    }
    .subtitle {
      margin-top: 4px;
      color: var(--muted);
      font-size: 13px;
    }
    .tag {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      border-radius: 999px;
      border: 1px solid rgba(148,163,184,0.4);
      background: rgba(15,23,42,0.8);
      font-size: 11px;
      color: var(--muted);
    }
    .grid {
      display: grid;
      grid-template-columns: minmax(0, 1.8fr) minmax(0, 1.4fr);
      gap: 18px;
    }
    @media (max-width: 900px) {
      .grid {
        grid-template-columns: minmax(0, 1fr);
      }
    }
    .card {
      background: linear-gradient(145deg, rgba(15,23,42,0.98), rgba(15,23,42,0.93));
      border-radius: var(--radius);
      border: 1px solid rgba(148,163,184,0.2);
      box-shadow:
        0 18px 45px rgba(15,23,42,0.75),
        0 0 0 1px rgba(15,23,42,0.9);
      padding: 14px 16px 14px;
    }
    .card h2 {
      font-size: 15px;
      margin: 0 0 4px;
    }
    .card p {
      font-size: 12px;
      margin: 0 0 8px;
      color: var(--muted);
    }
    label {
      display: block;
      font-size: 12px;
      margin-top: 6px;
    }
    input[type="text"],
    input[type="number"],
    select {
      width: 100%;
      margin-top: 2px;
      padding: 5px 7px;
      border-radius: 8px;
      border: 1px solid rgba(148,163,184,0.45);
      background: rgba(15,23,42,0.9);
      color: var(--text);
      font-size: 12px;
    }
    input:focus,
    select:focus {
      outline: 1px solid var(--accent);
    }
    .btn {
      margin-top: 8px;
      padding: 6px 14px;
      border-radius: 999px;
      border: none;
      cursor: pointer;
      font-size: 12px;
      font-weight: 500;
      background: linear-gradient(135deg, #38bdf8, #0ea5e9);
      color: #020617;
    }
    .btn:hover {
      filter: brightness(1.06);
    }
    .small {
      font-size: 11px;
      color: var(--muted);
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
      margin: 8px 0 10px;
    }
    .summary-item {
      background: radial-gradient(circle at top left, rgba(56,189,248,0.16), rgba(15,23,42,0.9));
      border-radius: 10px;
      border: 1px solid rgba(148,163,184,0.25);
      padding: 6px 8px;
      font-size: 12px;
    }
    .summary-label {
      color: var(--muted);
      font-size: 11px;
      margin-bottom: 1px;
    }
    .summary-value {
      font-size: 14px;
      font-weight: 600;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 11px;
    }
    th, td {
      padding: 4px 6px;
      text-align: left;
    }
    thead {
      background: rgba(15,23,42,0.95);
      position: sticky;
      top: 0;
      z-index: 1;
    }
    th {
      border-bottom: 1px solid rgba(148,163,184,0.6);
      color: var(--muted);
      font-weight: 500;
    }
    tbody tr:nth-child(odd) {
      background: rgba(15,23,42,0.9);
    }
    tbody tr:nth-child(even) {
      background: rgba(15,23,42,0.7);
    }
    .results-card {
      margin-top: 14px;
      border-radius: var(--radius);
      border: 1px solid rgba(148,163,184,0.4);
      background: rgba(15,23,42,0.9);
      padding: 10px 12px 12px;
      max-height: 430px;
      overflow: auto;
    }
    .results-header {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 8px;
      margin-bottom: 6px;
      font-size: 12px;
      color: var(--muted);
    }
    .error {
      margin-top: 6px;
      padding: 6px 8px;
      border-radius: 8px;
      border: 1px solid rgba(248,113,113,0.6);
      background: rgba(248,113,113,0.12);
      color: #fecaca;
      font-size: 12px;
    }
    code {
      font-size: 11px;
      background: rgba(15,23,42,0.9);
      padding: 2px 4px;
      border-radius: 4px;
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="header">
      <div>
        <h1>Data2App · Custom DataFrame</h1>
        <div class="subtitle">
          7 SQL-style operations implemented using our own CSV parser & in-memory dataframe.
        </div>
      </div>
      <div class="tag">
        <span>●</span>
        <span>CSVParser · No pandas/csv/json</span>
      </div>
    </div>

    <div class="grid">
      <!-- LEFT: controls / functions -->
      <div>
        <!-- Dataset summary -->
        <section class="card">
          <h2>Dataset summary</h2>
          <p>Loaded once with <code>CSVParser.parse()</code> into main memory.</p>
          <div class="summary-grid">
            <div class="summary-item">
              <div class="summary-label">Rows</div>
              <div class="summary-value">{{ row_count }}</div>
            </div>
            <div class="summary-item">
              <div class="summary-label">Columns</div>
              <div class="summary-value">{{ col_count }}</div>
            </div>
            <div class="summary-item">
              <div class="summary-label">File</div>
              <div class="summary-value" style="font-size: 11px;">{{ csv_path }}</div>
            </div>
          </div>
          <div class="small">
            Example __getitem__ usage: <code>parser["PTS"]</code> returns the list of all PTS values.
          </div>
        </section>

        <!-- 1) Filtering -->
        <section class="card" style="margin-top: 12px;">
          <h2>1) Filter: team &amp; minimum points</h2>
          <p>SQL analog: <code>SELECT Player, Tm, PTS FROM stats WHERE Tm = ? AND PTS &gt;= ?</code></p>
          <form method="post">
            <input type="hidden" name="action" value="filter_team_pts">
            <label>
              Team code (Tm):
              <input type="text" name="team" placeholder="e.g. LAL, GSW">
            </label>
            <label>
              Minimum points (PTS ≥):
              <input type="number" name="min_pts" value="20">
            </label>
            <button class="btn" type="submit">Run filter</button>
          </form>
        </section>

        <!-- 2) Projection -->
        <section class="card" style="margin-top: 12px;">
          <h2>2) Projection: select columns</h2>
          <p>SQL analog: <code>SELECT col1, col2, ... FROM stats</code></p>
          <form method="post">
            <input type="hidden" name="action" value="project_columns">
            <label>
              Columns (comma-separated):
              <input type="text" name="cols" placeholder="e.g. Player,Tm,PTS">
            </label>
            <div class="small">
              Available columns: {{ available_cols }}
            </div>
            <button class="btn" type="submit">Project</button>
          </form>
        </section>

        <!-- 3) Top N scorers -->
        <section class="card" style="margin-top: 12px;">
          <h2>3) Top N scorers</h2>
          <p>SQL analog: <code>SELECT Player, Tm, PTS FROM stats ORDER BY PTS DESC LIMIT N</code></p>
          <form method="post">
            <input type="hidden" name="action" value="top_scorers">
            <label>
              N (number of rows):
              <input type="number" name="n" value="10">
            </label>
            <button class="btn" type="submit">Show top scorers</button>
          </form>
        </section>

        <!-- 4) Group by team: AVG(PTS) -->
        <section class="card" style="margin-top: 12px;">
          <h2>4) Group by team – AVG(PTS)</h2>
          <p>SQL analog: <code>SELECT Tm, AVG(PTS) FROM stats GROUP BY Tm</code></p>
          <form method="post">
            <input type="hidden" name="action" value="avg_pts_by_team">
            <button class="btn" type="submit">Compute team averages</button>
          </form>
        </section>

        <!-- 5) Group by opponent: COUNT + AVG(PTS) -->
        <section class="card" style="margin-top: 12px;">
          <h2>5) Group by Opp – COUNT &amp; AVG(PTS)</h2>
          <p>SQL analog: <code>SELECT Opp, COUNT(*), AVG(PTS) FROM stats GROUP BY Opp</code></p>
          <form method="post">
            <input type="hidden" name="action" value="opp_stats">
            <button class="btn" type="submit">Compute opponent stats</button>
          </form>
        </section>

        <!-- 6) Global aggregations -->
        <section class="card" style="margin-top: 12px;">
          <h2>6) Global aggregations</h2>
          <p>SQL analog: <code>SELECT COUNT(*), SUM(PTS), AVG(PTS) FROM stats</code></p>
          <form method="post">
            <input type="hidden" name="action" value="global_aggs">
            <button class="btn" type="submit">Compute global stats</button>
          </form>
        </section>

        <!-- 7) Join -->
        <section class="card" style="margin-top: 12px; margin-bottom: 12px;">
          <h2>7) Join with teams metadata</h2>
          <p>SQL analog: <code>SELECT p.Player, p.Tm, t.Region FROM stats p JOIN teams t ON p.Tm = t.Tm</code></p>
          <form method="post">
            <input type="hidden" name="action" value="join_teams">
            <button class="btn" type="submit">Run join</button>
          </form>
          <div class="small">
            Uses <code>parser.join(other_data, left_on="Tm", right_on="Tm")</code>.
          </div>
        </section>
      </div>

      <!-- RIGHT: result viewer -->
      <div>
        <section class="card">
          <h2>Result viewer</h2>
          <p>Shows the output of the last operation you ran.</p>
          {% if error %}
            <div class="error">{{ error }}</div>
          {% endif %}
          {% if result_title %}
            <div class="results-card">
              <div class="results-header">
                <div>{{ result_title }}</div>
                <div>{{ row_count_result }} row(s)</div>
              </div>
              {% if columns and rows %}
                <table>
                  <thead>
                    <tr>
                      {% for c in columns %}
                        <th>{{ c }}</th>
                      {% endfor %}
                    </tr>
                  </thead>
                  <tbody>
                    {% for r in rows %}
                      <tr>
                        {% for cell in r %}
                          <td>{{ cell }}</td>
                        {% endfor %}
                      </tr>
                    {% endfor %}
                  </tbody>
                </table>
              {% else %}
                <div class="small">No rows to display.</div>
              {% endif %}
            </div>
          {% else %}
            <div class="small">
              Run any function on the left to see results here.
            </div>
          {% endif %}
        </section>
      </div>
    </div>
  </div>
</body>
</html>
"""


# -----------------------------
# Route
# -----------------------------
@APP.route("/", methods=["GET", "POST"])
def index():
    error = None
    result_title = ""
    columns = []
    rows = []

    if request.method == "POST":
        action = request.form.get("action", "")

        try:
            if action == "filter_team_pts":
                team = request.form.get("team", "").strip().upper()
                min_pts_raw = request.form.get("min_pts", "0").strip()
                try:
                    min_pts = int(min_pts_raw)
                except ValueError:
                    min_pts = 0

                def cond(row):
                    row_team = str(row.get("Tm", "")).upper()
                    pts = row.get("PTS", 0)
                    if pts is None:
                        return False
                    if team and row_team != team:
                        return False
                    return pts >= min_pts

                filtered = parser.filter_rows(cond)
                cols, rows_ = to_table_rows(filtered, cols=["Player", "Tm", "PTS"] if filtered else [])
                result_title = f"Filter: team='{team or 'ANY'}', PTS >= {min_pts}"
                columns, rows = cols, rows_

            elif action == "project_columns":
                cols_str = request.form.get("cols", "")
                cols_req = [c.strip() for c in cols_str.split(",") if c.strip()]
                if not cols_req:
                    raise ValueError("Please enter at least one column.")
                projected = parser.filter_columns(cols_req)
                cols, rows_ = to_table_rows(projected, cols=cols_req)
                result_title = f"Projection on columns: {', '.join(cols_req)}"
                columns, rows = cols, rows_

            elif action == "top_scorers":
                n_raw = request.form.get("n", "10").strip()
                try:
                    n = int(n_raw)
                except ValueError:
                    n = 10
                rows_with_pts = [r for r in DATA if r.get("PTS") is not None]
                top = sorted(rows_with_pts, key=lambda r: r["PTS"], reverse=True)[:n]
                cols, rows_ = to_table_rows(top, cols=["Player", "Tm", "PTS"])
                result_title = f"Top {n} scorers by PTS"
                columns, rows = cols, rows_

            elif action == "avg_pts_by_team":
                agg = parser.aggregate(group_by="Tm", target_col="PTS", func="avg")
                rows_list = [{"Tm": k, "avg_PTS": round(v, 2) if v is not None else None}
                             for k, v in sorted(agg.items(), key=lambda kv: (kv[1] is None, -(kv[1] or 0)))]
                cols, rows_ = to_table_rows(rows_list, cols=["Tm", "avg_PTS"])
                result_title = "Average PTS per team (GROUP BY Tm)"
                columns, rows = cols, rows_

            elif action == "opp_stats":
                agg = parser.aggregate(group_by="Opp", target_col="PTS", func="avg")
                counts = parser.aggregate(group_by="Opp", target_col="PTS", func="count")
                rows_list = []
                for opp in agg.keys():
                    rows_list.append({
                        "Opp": opp,
                        "games": counts.get(opp, 0),
                        "avg_PTS": round(agg[opp], 2) if agg[opp] is not None else None
                    })
                rows_list.sort(key=lambda r: r["avg_PTS"] if r["avg_PTS"] is not None else -1, reverse=True)
                cols, rows_ = to_table_rows(rows_list, cols=["Opp", "games", "avg_PTS"])
                result_title = "Opponent stats: games & AVG(PTS) per Opp"
                columns, rows = cols, rows_

            elif action == "global_aggs":
                pts_list = parser["PTS"] 
                total_pts = parser.aggregate(group_by=None, target_col="PTS", func="sum")
                avg_pts = parser.aggregate(group_by=None, target_col="PTS", func="avg")
                count_rows = len(pts_list)
                rows_list = [{
                    "count_rows": count_rows,
                    "sum_PTS": total_pts,
                    "avg_PTS": round(avg_pts, 2) if avg_pts is not None else None
                }]
                cols, rows_ = to_table_rows(rows_list, cols=["count_rows", "sum_PTS", "avg_PTS"])
                result_title = "Global aggregations: COUNT(*), SUM(PTS), AVG(PTS)"
                columns, rows = cols, rows_

            elif action == "join_teams":
                teams_meta = [
                    {"Tm": "LAL", "Region": "West"},
                    {"Tm": "GSW", "Region": "West"},
                    {"Tm": "BOS", "Region": "East"},
                    {"Tm": "MIA", "Region": "East"},
                ]
                joined = parser.join(teams_meta, left_on="Tm", right_on="Tm")
                simplified = []
                for row in joined:
                    simplified.append({
                        "Player": row.get("Player"),
                        "Tm": row.get("Tm"),
                        "PTS": row.get("PTS"),
                        "Region": row.get("Region")
                    })
                cols, rows_ = to_table_rows(simplified, cols=["Player", "Tm", "PTS", "Region"])
                result_title = "Join stats with teams metadata on Tm"
                columns, rows = cols, rows_

            else:
                error = "Unknown action."

        except Exception as e:
            error = str(e)

    return render_template_string(
        PAGE,
        csv_path=CSV_PATH,
        row_count=len(DATA),
        col_count=len(COLUMNS),
        available_cols=", ".join(COLUMNS),
        error=error,
        result_title=result_title,
        columns=columns,
        rows=rows,
        row_count_result=len(rows),
    )


if __name__ == "__main__":
    APP.run(debug=True)