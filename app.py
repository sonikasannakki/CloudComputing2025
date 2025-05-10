from flask import Flask, render_template_string, jsonify
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import plotly.io as pio
import datetime

app = Flask(__name__)
client = bigquery.Client()

TEMPLATE = """
<!doctype html>
<html>
<head>
  <title>Advanced Analytics Dashboard</title>
  <meta charset="utf-8">
</head>
<body>
  <h1>Advanced Analytics: Top Performing Age Groups by Traffic Channel</h1>
  <div>{{ traffic_chart|safe }}</div>

  <h2>Revenue Summary (Last 30 Days)</h2>
  <div>{{ revenue_chart|safe }}</div>

  <h2>API Access</h2>
  <p>Raw data: <a href="/api/traffic">/api/traffic</a> | <a href="/api/revenue">/api/revenue</a></p>
</body>
</html>
"""

@app.route("/")
def dashboard():
    traffic_df = get_traffic_data()
    revenue_df = get_revenue_data()

    fig1 = px.bar(traffic_df, x="traffic_source", y="total_revenue", color="age_group",
                 title="Total Revenue by Age Group and Traffic Source")
    traffic_chart = pio.to_html(fig1, full_html=False)

    fig2 = px.line(revenue_df, x="date", y="daily_revenue", title="Revenue Trend - Last 30 Days")
    revenue_chart = pio.to_html(fig2, full_html=False)

    return render_template_string(TEMPLATE, traffic_chart=traffic_chart, revenue_chart=revenue_chart)

@app.route("/api/traffic")
def api_traffic():
    df = get_traffic_data()
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/revenue")
def api_revenue():
    df = get_revenue_data()
    return jsonify(df.to_dict(orient="records"))

def get_traffic_data():
    query = """
        SELECT
          u.traffic_source,
          CASE
            WHEN u.age < 25 THEN 'Under 25'
            WHEN u.age BETWEEN 25 AND 34 THEN '25–34'
            WHEN u.age BETWEEN 35 AND 44 THEN '35–44'
            WHEN u.age BETWEEN 45 AND 54 THEN '45–54'
            ELSE '55+'
          END AS age_group,
          ROUND(SUM(oi.sale_price), 2) AS total_revenue
        FROM `comm034-coursework-6897699.thelook.users` u
        JOIN `comm034-coursework-6897699.thelook.orders` o ON u.id = o.user_id
        JOIN `comm034-coursework-6897699.thelook.order_items` oi ON o.order_id = oi.order_id
        WHERE o.status = 'Complete'
        GROUP BY traffic_source, age_group
        ORDER BY total_revenue DESC
    """
    return client.query(query).to_dataframe()

def get_revenue_data():
    thirty_days_ago = (datetime.datetime.utcnow() - datetime.timedelta(days=30)).date()
    query = f"""
        SELECT
          DATE(o.created_at) AS date,
          ROUND(SUM(oi.sale_price), 2) AS daily_revenue
        FROM `comm034-coursework-6897699.thelook.orders` o
        JOIN `comm034-coursework-6897699.thelook.order_items` oi ON o.order_id = oi.order_id
        WHERE o.status = 'Complete' AND o.created_at >= '{thirty_days_ago}'
        GROUP BY date
        ORDER BY date
    """
    return client.query(query).to_dataframe()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
