# app.py – Vehicle Telematics Dashboard (Final, Complete)
# Pages:
# 1. Trip Deep Dive (default)
# 2. Fleet KPI Overview (stats + box plot)
# 3. All Trips Comparison (clustered trends)
# Global metrics table shown on all pages

import os
import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html, Input, Output, dash_table

# ---------------------------------
# Data Loading
# ---------------------------------
OUTPUT_DIR = "output"
METRICS_CSV = os.path.join(OUTPUT_DIR, "trip_metrics.csv")

df = pd.read_csv(METRICS_CSV) if os.path.exists(METRICS_CSV) else pd.DataFrame()

NUMERIC_COLS = [
    "duration_minutes",
    "avg_speed",
    "distance_km",
    "max_speed",
    "energy_consumed_kwh",
]

for col in NUMERIC_COLS:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df = df.reset_index(drop=True)

# Synthetic trip timeline
df["trip_index"] = df.index + 1

# ---------------------------------
# Trip Clustering (for comparison page)
# ---------------------------------
CLUSTER_SIZE = 50

def make_cluster_label(idx, total, size):
    start = ((idx - 1) // size) * size + 1
    end = min(start + size - 1, total)
    return f"{start}-{end}"

if not df.empty:
    df["trip_cluster"] = df["trip_index"].apply(
        lambda x: make_cluster_label(x, len(df), CLUSTER_SIZE)
    )

    clustered_df = (
        df.groupby("trip_cluster", as_index=False)
          .agg({
              "avg_speed": "mean",
              "distance_km": "mean",
              "duration_minutes": "mean",
          })
    )
else:
    clustered_df = pd.DataFrame()

# ---------------------------------
# App Init
# ---------------------------------
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "Vehicle Telematics Analytics"

# ---------------------------------
# Helpers
# ---------------------------------
def stat_card(label, value):
    return html.Div(
        style={
            "padding": "18px",
            "border": "1px solid #e0e0e0",
            "borderRadius": "10px",
            "backgroundColor": "#fafafa",
            "minWidth": "180px",
            "textAlign": "center",
            "boxShadow": "0px 2px 6px rgba(0,0,0,0.05)",
        },
        children=[
            html.Div(label, style={"fontSize": "13px", "color": "#666"}),
            html.Div(value, style={"fontSize": "26px", "fontWeight": "600"}),
        ],
    )

def metrics_table():
    return html.Div(
        style={"marginTop": "50px"},
        children=[
            html.H3("📋 Trip Metrics Table"),

            dash_table.DataTable(
                id="metrics-table",
                data=df.to_dict("records"),
                columns=[
                    {"name": c.replace("_", " ").title(), "id": c}
                    for c in df.columns
                ],
                page_size=10,
                style_table={"overflowX": "auto"},
                style_cell={
                    "textAlign": "center",
                    "padding": "8px",
                    "fontSize": "13px",
                    "whiteSpace": "normal",
                },
                style_header={
                    "backgroundColor": "#f0f0f0",
                    "fontWeight": "bold",
                },
            ),
        ],
    )

# ---------------------------------
# Layout
# ---------------------------------
app.layout = html.Div(
    style={"padding": "24px", "fontFamily": "Arial"},
    children=[
        dcc.Location(id="url"),

        html.H1("🚗 Vehicle Telematics Dashboard"),

        html.Div(
            style={"display": "flex", "gap": "24px", "marginBottom": "12px"},
            children=[
                dcc.Link("Trip Deep Dive", href="/"),
                dcc.Link("Fleet KPI Overview", href="/fleet"),
                dcc.Link("All Trips Comparison", href="/compare"),
            ],
        ),

        html.Hr(),

        html.Div(id="page-content"),

        # ✅ GLOBAL TABLE (VISIBLE ON ALL PAGES)
        metrics_table(),
    ],
)

# ---------------------------------
# Page 1 – Trip Deep Dive
# ---------------------------------
trip_layout = html.Div([

    html.H2("🔍 Trip Deep Dive"),

    dcc.Dropdown(
        id="trip-selector",
        options=[
            {"label": t, "value": t}
            for t in df["trip_id"].dropna().unique()
        ],
        placeholder="Select Trip",
        style={"width": "320px"},
    ),

    html.Hr(),

    html.Div(id="trip-kpis", style={"display": "flex", "gap": "20px"}),

    dcc.Graph(id="trip-bar"),

    html.Br(),

    dcc.Dropdown(
        id="trend-metric",
        options=[
            {"label": "Average Speed", "value": "avg_speed"},
            {"label": "Distance (km)", "value": "distance_km"},
            {"label": "Duration (minutes)", "value": "duration_minutes"},
        ],
        value="avg_speed",
        style={"width": "320px", "marginBottom": "10px"},
    ),

    dcc.Graph(id="trip-trend"),
])

# ---------------------------------
# Page 2 – Fleet KPI Overview
# ---------------------------------
fleet_layout = html.Div([

    html.H2("📊 Fleet KPI Overview"),

    dcc.Dropdown(
        id="fleet-metric",
        options=[
            {"label": "Average Speed", "value": "avg_speed"},
            {"label": "Distance (km)", "value": "distance_km"},
            {"label": "Duration (minutes)", "value": "duration_minutes"},
        ],
        value="avg_speed",
        style={"width": "320px", "marginBottom": "30px"},
    ),

    html.Div(
        id="fleet-stats",
        style={
            "display": "grid",
            "gridTemplateColumns": "repeat(auto-fit, minmax(200px, 1fr))",
            "gap": "20px",
            "marginBottom": "40px",
        },
    ),

    dcc.Graph(id="fleet-boxplot"),
])

# ---------------------------------
# Page 3 – All Trips Comparison (Clustered)
# ---------------------------------
compare_layout = html.Div([

    html.H2("📈 All Trips Comparison (Clustered)"),

    html.P(
        f"Trips grouped into buckets of {CLUSTER_SIZE} for clearer trend analysis.",
        style={"color": "#555"},
    ),

    dcc.Graph(id="compare-speed"),
    dcc.Graph(id="compare-distance"),
    dcc.Graph(id="compare-duration"),
])

# ---------------------------------
# Router
# ---------------------------------
@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def route(pathname):
    if pathname == "/fleet":
        return fleet_layout
    if pathname == "/compare":
        return compare_layout
    return trip_layout

# ---------------------------------
# Trip Deep Dive Callback
# ---------------------------------
@app.callback(
    Output("trip-kpis", "children"),
    Output("trip-bar", "figure"),
    Output("trip-trend", "figure"),
    Input("trip-selector", "value"),
    Input("trend-metric", "value"),
)
def update_trip(trip_id, trend_metric):

    if not trip_id or df.empty:
        empty = px.bar(title="Select a trip to begin")
        return [], empty, empty

    t = df[df["trip_id"] == trip_id].iloc[0]

    kpis = [
        stat_card("Duration (min)", round(t["duration_minutes"], 2)),
        stat_card("Distance (km)", round(t["distance_km"], 2)),
        stat_card("Avg Speed", round(t["avg_speed"], 2)),
    ]

    bar = px.bar(
        x=["avg_speed", "distance_km", "duration_minutes"],
        y=[t["avg_speed"], t["distance_km"], t["duration_minutes"]],
        title=f"Trip {trip_id} – Key Metrics",
    )

    trend = px.line(
        df,
        x="trip_index",
        y=trend_metric,
        title=f"{trend_metric.replace('_', ' ').title()} Trend Across Trips",
    )

    trend.add_vline(
        x=t["trip_index"],
        line_dash="dash",
        annotation_text="Selected Trip",
    )

    return kpis, bar, trend

# ---------------------------------
# Fleet KPI Callback
# ---------------------------------
@app.callback(
    Output("fleet-stats", "children"),
    Output("fleet-boxplot", "figure"),
    Input("fleet-metric", "value"),
)
def update_fleet(metric):

    if df.empty:
        empty_fig = px.scatter(title="No data available")
        return [], empty_fig

    dff = df[df[metric].notna()]

    stats = [
        stat_card("Trips", len(dff)),
        stat_card("Mean", round(dff[metric].mean(), 2)),
        stat_card("Median", round(dff[metric].median(), 2)),
        stat_card("Min", round(dff[metric].min(), 2)),
        stat_card("Max", round(dff[metric].max(), 2)),
        stat_card("P25", round(dff[metric].quantile(0.25), 2)),
        stat_card("P75", round(dff[metric].quantile(0.75), 2)),
    ]

    boxplot = px.box(
        dff,
        y=metric,
        points="outliers",
        title=f"Fleet Distribution – {metric.replace('_', ' ').title()}",
    )

    return stats, boxplot

# ---------------------------------
# Clustered Comparison Callback
# ---------------------------------
@app.callback(
    Output("compare-speed", "figure"),
    Output("compare-distance", "figure"),
    Output("compare-duration", "figure"),
    Input("url", "pathname"),
)
def update_comparison(_):

    if clustered_df.empty:
        empty = px.scatter(title="No data available")
        return empty, empty, empty

    speed = px.line(
        clustered_df,
        x="trip_cluster",
        y="avg_speed",
        markers=True,
        title="Avg Speed – Clustered Trend",
    )

    distance = px.line(
        clustered_df,
        x="trip_cluster",
        y="distance_km",
        markers=True,
        title="Distance – Clustered Trend",
    )

    duration = px.line(
        clustered_df,
        x="trip_cluster",
        y="duration_minutes",
        markers=True,
        title="Duration – Clustered Trend",
    )

    return speed, distance, duration

# ---------------------------------
# Run
# ---------------------------------
if __name__ == "__main__":
    app.run(debug=True)
