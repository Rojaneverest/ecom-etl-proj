import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import numpy as np
from datetime import datetime, timedelta

# Database connection
DATABASE_URL = "postgresql://postgres:root@localhost:5432/cp_database"
engine = create_engine(DATABASE_URL)

# Page configuration
st.set_page_config(page_title="E-commerce Analytics Dashboard", layout="wide")

# Title and description
st.title("E-commerce Analytics Dashboard")
st.markdown("""
Welcome to the E-commerce Analytics Dashboard! This dashboard provides comprehensive insights into your e-commerce business performance.
Explore various metrics and visualizations to understand your sales, customer behavior, and operational efficiency.
""")

# Sidebar filters
st.sidebar.header("Filters")
start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime('2017-01-01'))
end_date = st.sidebar.date_input("End Date", value=pd.to_datetime('2018-12-31'))

# Add category filter
selected_category = st.sidebar.multiselect(
    "Select Categories",
    pd.read_sql("SELECT DISTINCT product_category_name FROM products", engine)['product_category_name'].tolist(),
    default=[]
)

# Add state filter
selected_states = st.sidebar.multiselect(
    "Select States",
    pd.read_sql("SELECT DISTINCT geolocation_state FROM geolocation", engine)['geolocation_state'].tolist(),
    default=[]
)

# Add payment type filter
selected_payment_types = st.sidebar.multiselect(
    "Select Payment Types",
    pd.read_sql("SELECT DISTINCT payment_type FROM order_payments", engine)['payment_type'].tolist(),
    default=[]
)

# Main metrics
st.header("Key Business Metrics")

# Add more filters to the queries
def get_filter_clause(column, values):
    if not values:
        return ""
    if len(values) == 1:
        return f"{column} = '{values[0]}'"
    return f"{column} IN {tuple(values)}"

base_filters = []
if selected_category:
    base_filters.append(get_filter_clause("p.product_category_name", selected_category))
if selected_states:
    base_filters.append(get_filter_clause("g.geolocation_state", selected_states))
if selected_payment_types:
    base_filters.append(get_filter_clause("op.payment_type", selected_payment_types))

base_filters_str = " AND ".join(base_filters) if base_filters else "1=1"

# Total orders and revenue
orders_query = f"""
SELECT COUNT(DISTINCT o.order_id) as total_orders,
       SUM(oi.price) as total_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_purchase_timestamp BETWEEN '{start_date}' AND '{end_date}'
"""

metrics_df = pd.read_sql(orders_query, engine)

col1, col2 = st.columns(2)
with col1:
    st.metric("Total Orders", f"{metrics_df['total_orders'][0]:,}")
with col2:
    st.metric("Total Revenue", f"${metrics_df['total_revenue'][0]:,.2f}")

# Sales by category
st.header("Sales Analysis")
category_query = f"""
SELECT p.product_category_name,
       COUNT(*) as order_count,
       SUM(oi.price) as total_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_purchase_timestamp BETWEEN '{start_date}' AND '{end_date}'
GROUP BY p.product_category_name
ORDER BY total_revenue DESC
LIMIT 10
"""

category_df = pd.read_sql(category_query, engine)
fig = px.bar(category_df, x='product_category_name', y='total_revenue',
            title='Top 10 Categories by Revenue',
            labels={'product_category_name': 'Category', 'total_revenue': 'Revenue'})
st.plotly_chart(fig, use_container_width=True)

# Order status distribution
st.header("Order Status Analysis")
status_query = f"""
SELECT o.order_status,
       COUNT(*) as order_count
FROM orders o
WHERE o.order_purchase_timestamp BETWEEN '{start_date}' AND '{end_date}'
GROUP BY o.order_status
"""

status_df = pd.read_sql(status_query, engine)
fig = px.pie(status_df, values='order_count', names='order_status',
            title='Order Status Distribution')
st.plotly_chart(fig, use_container_width=True)

# Customer analysis
st.header("Customer Insights")

# Add customer segmentation
st.subheader("Customer Segmentation")
segmentation_query = f"""
WITH customer_metrics AS (
    SELECT 
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(oi.price) as total_spent,
        MAX(o.order_purchase_timestamp) - MIN(o.order_purchase_timestamp) as days_since_first_order,
        COUNT(DISTINCT DATE_TRUNC('month', o.order_purchase_timestamp)) as months_active
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN geolocation g ON c.geolocation_id = g.geolocation_id
    WHERE o.order_purchase_timestamp BETWEEN '{start_date}' AND '{end_date}'
        AND {base_filters_str}
    GROUP BY c.customer_unique_id
)
SELECT 
    CASE 
        WHEN total_orders >= 10 THEN 'Heavy Buyer'
        WHEN total_orders >= 5 THEN 'Regular Buyer'
        ELSE 'Occasional Buyer'
    END as customer_segment,
    COUNT(*) as customer_count,
    CAST(AVG(total_spent) as numeric(10,2)) as avg_spent,
    CAST(AVG(days_since_first_order) as numeric(10,2)) as avg_days_active
FROM customer_metrics
GROUP BY customer_segment
"""

segmentation_df = pd.read_sql(segmentation_query, engine)

fig = px.bar(segmentation_df, x='customer_segment', y='customer_count',
            title='Customer Segmentation by Purchase Behavior',
            labels={'customer_segment': 'Customer Segment', 'customer_count': 'Number of Customers'})
st.plotly_chart(fig, use_container_width=True)

# Add customer lifetime value
st.subheader("Customer Lifetime Value")
clv_query = f"""
WITH customer_metrics AS (
    SELECT 
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(oi.price) as total_spent,
        COUNT(DISTINCT DATE_TRUNC('month', o.order_purchase_timestamp)) as months_active
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN geolocation g ON c.geolocation_id = g.geolocation_id
    WHERE o.order_purchase_timestamp BETWEEN '{start_date}' AND '{end_date}'
        AND {base_filters_str}
    GROUP BY c.customer_unique_id
)
SELECT 
    CAST((total_spent / months_active) as numeric(10,2)) as monthly_clv,
    COUNT(*) as customer_count
FROM customer_metrics
GROUP BY CAST((total_spent / months_active) as numeric(10,2))
ORDER BY monthly_clv
"""

clv_df = pd.read_sql(clv_query, engine)

fig = px.histogram(clv_df, x='monthly_clv', y='customer_count',
                  title='Customer Lifetime Value Distribution',
                  labels={'monthly_clv': 'Monthly CLV', 'customer_count': 'Number of Customers'})
st.plotly_chart(fig, use_container_width=True)

# Add customer retention
st.subheader("Customer Retention")
retention_query = f"""
WITH first_purchase AS (
    SELECT 
        c.customer_unique_id,
        MIN(o.order_purchase_timestamp) as first_purchase
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN geolocation g ON c.geolocation_id = g.geolocation_id
    WHERE o.order_purchase_timestamp BETWEEN '{start_date}' AND '{end_date}'
        AND {base_filters_str}
    GROUP BY c.customer_unique_id
),
monthly_cohorts AS (
    SELECT 
        DATE_TRUNC('month', first_purchase) as cohort_month,
        COUNT(*) as cohort_size
    FROM first_purchase
    GROUP BY DATE_TRUNC('month', first_purchase)
),
retention_data AS (
    SELECT 
        DATE_TRUNC('month', first_purchase) as cohort_month,
        DATE_TRUNC('month', o.order_purchase_timestamp) as order_month,
        COUNT(DISTINCT c.customer_unique_id) as active_users
    FROM first_purchase fp
    JOIN customers c ON fp.customer_unique_id = c.customer_unique_id
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN geolocation g ON c.geolocation_id = g.geolocation_id
    WHERE o.order_purchase_timestamp BETWEEN '{start_date}' AND '{end_date}'
        AND {base_filters_str}
    GROUP BY DATE_TRUNC('month', first_purchase), DATE_TRUNC('month', o.order_purchase_timestamp)
)
SELECT 
    mc.cohort_month,
    rd.order_month,
    mc.cohort_size,
    rd.active_users,
    CAST((rd.active_users::decimal / mc.cohort_size) * 100 as numeric(10,2)) as retention_rate
FROM monthly_cohorts mc
LEFT JOIN retention_data rd ON mc.cohort_month = rd.cohort_month
ORDER BY mc.cohort_month, rd.order_month
"""

retention_df = pd.read_sql(retention_query, engine)

# Create a pivot table for the heatmap
heatmap_df = pd.pivot_table(retention_df, values='retention_rate',
                           index='cohort_month',
                           columns='order_month',
                           fill_value=0)

fig = px.imshow(heatmap_df, text_auto=True,
               title='Customer Retention Heatmap',
               labels=dict(x="Order Month", y="Cohort Month", color="Retention Rate (%)"))
st.plotly_chart(fig, use_container_width=True)

# Payment analysis
st.header("Payment Analysis")

# Payment method analysis
st.subheader("Payment Method Analysis")
payment_query = f"""
SELECT op.payment_type,
       COUNT(*) as payment_count,
       SUM(op.payment_value) as total_amount,
       AVG(op.payment_value) as avg_payment,
       COUNT(DISTINCT o.order_id) as unique_orders
FROM orders o
JOIN order_payments op ON o.order_id = op.order_id
WHERE o.order_purchase_timestamp BETWEEN '{start_date}' AND '{end_date}'
    AND {base_filters_str if base_filters_str else "1=1"}
GROUP BY op.payment_type
ORDER BY total_amount DESC
"""

payment_df = pd.read_sql(payment_query, engine)

# Create a donut chart for payment distribution
fig = px.pie(payment_df, values='total_amount', names='payment_type',
            title='Payment Method Distribution',
            hole=0.3)
fig.update_traces(textposition='inside', textinfo='percent+label')
st.plotly_chart(fig, use_container_width=True)

# Payment trends over time
st.subheader("Payment Trends")
payment_trend_query = f"""
SELECT 
    DATE_TRUNC('month', o.order_purchase_timestamp) as month,
    op.payment_type,
    COUNT(*) as payment_count,
    SUM(op.payment_value) as total_amount
FROM orders o
JOIN order_payments op ON o.order_id = op.order_id
WHERE o.order_purchase_timestamp BETWEEN '{start_date}' AND '{end_date}'
    AND {base_filters_str if base_filters_str else "1=1"}
GROUP BY DATE_TRUNC('month', o.order_purchase_timestamp), op.payment_type
ORDER BY month
"""

payment_trend_df = pd.read_sql(payment_trend_query, engine)

fig = px.line(payment_trend_df, x='month', y='total_amount', color='payment_type',
             title='Payment Method Trends Over Time',
             labels={'month': 'Month', 'total_amount': 'Total Amount'})
st.plotly_chart(fig, use_container_width=True)

# Payment installments analysis
st.subheader("Payment Installments Analysis")
installments_query = f"""
SELECT 
    op.payment_installments,
    COUNT(*) as payment_count,
    SUM(op.payment_value) as total_amount,
    AVG(op.payment_value) as avg_payment
FROM orders o
JOIN order_payments op ON o.order_id = op.order_id
WHERE o.order_purchase_timestamp BETWEEN '{start_date}' AND '{end_date}'
    AND {base_filters_str if base_filters_str else "1=1"}
GROUP BY op.payment_installments
ORDER BY op.payment_installments
"""

installments_df = pd.read_sql(installments_query, engine)

fig = px.bar(installments_df, x='payment_installments', y='total_amount',
            title='Payment Installments Distribution',
            labels={'payment_installments': 'Number of Installments', 'total_amount': 'Total Amount'})
st.plotly_chart(fig, use_container_width=True)

# Review analysis
st.header("Customer Satisfaction")

# Review score analysis
st.subheader("Review Score Analysis")
review_query = f"""
SELECT r.review_score,
       COUNT(*) as review_count,
       AVG(oi.price) as avg_order_value,
       COUNT(DISTINCT o.order_id) as affected_orders
FROM order_reviews r
JOIN orders o ON r.order_id = o.order_id
JOIN order_items oi ON o.order_id = oi.order_id
WHERE r.review_creation_date BETWEEN '{start_date}' AND '{end_date}'
    AND {base_filters_str if base_filters_str else "1=1"}
GROUP BY r.review_score
ORDER BY r.review_score
"""

review_df = pd.read_sql(review_query, engine)

fig = px.bar(review_df, x='review_score', y='review_count',
            title='Review Score Distribution',
            labels={'review_score': 'Review Score', 'review_count': 'Number of Reviews'})
st.plotly_chart(fig, use_container_width=True)

# Review response time analysis
st.subheader("Review Response Time")
response_time_query = f"""
SELECT 
    EXTRACT(EPOCH FROM (r.review_answer_timestamp - r.review_creation_date)) as response_time_seconds,
    COUNT(*) as review_count
FROM order_reviews r
WHERE r.review_creation_date BETWEEN '{start_date}' AND '{end_date}'
    AND r.review_answer_timestamp IS NOT NULL
    AND {base_filters_str if base_filters_str else "1=1"}
GROUP BY EXTRACT(EPOCH FROM (r.review_answer_timestamp - r.review_creation_date))
ORDER BY response_time_seconds
"""

response_time_df = pd.read_sql(response_time_query, engine)
response_time_df['response_time_minutes'] = response_time_df['response_time_seconds'] / 60

fig = px.histogram(response_time_df, x='response_time_minutes',
                  title='Review Response Time Distribution',
                  labels={'response_time_minutes': 'Response Time (minutes)', 'review_count': 'Number of Reviews'})
st.plotly_chart(fig, use_container_width=True)

# Review sentiment analysis
st.subheader("Review Sentiment Analysis")
sentiment_query = f"""
SELECT 
    CASE 
        WHEN r.review_score >= 4 THEN 'Positive'
        WHEN r.review_score >= 3 THEN 'Neutral'
        ELSE 'Negative'
    END as sentiment,
    COUNT(*) as review_count,
    AVG(oi.price) as avg_order_value
FROM order_reviews r
JOIN orders o ON r.order_id = o.order_id
JOIN order_items oi ON o.order_id = oi.order_id
WHERE r.review_creation_date BETWEEN '{start_date}' AND '{end_date}'
    AND {base_filters_str if base_filters_str else "1=1"}
GROUP BY sentiment
"""

sentiment_df = pd.read_sql(sentiment_query, engine)

fig = px.pie(sentiment_df, values='review_count', names='sentiment',
            title='Review Sentiment Distribution',
            hole=0.3)
fig.update_traces(textposition='inside', textinfo='percent+label')
st.plotly_chart(fig, use_container_width=True)

# Time-based analysis
st.header("Time-based Analysis")

# Add time-based filters
selected_timeframe = st.selectbox(
    "Select Timeframe",
    ["Monthly", "Weekly", "Daily"],
    index=0
)

timeframe_map = {
    "Monthly": "month",
    "Weekly": "week",
    "Daily": "day"
}

# Add trend analysis
st.subheader("Trend Analysis")
trend_query = f"""
SELECT 
    DATE_TRUNC('{timeframe_map[selected_timeframe]}', o.order_purchase_timestamp) as timeframe,
    COUNT(*) as order_count,
    SUM(oi.price) as total_revenue,
    AVG(oi.price) as avg_order_value,
    COUNT(DISTINCT o.customer_id) as unique_customers
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_purchase_timestamp BETWEEN '{start_date}' AND '{end_date}'
    AND {base_filters_str if base_filters_str else "1=1"}
GROUP BY DATE_TRUNC('{timeframe_map[selected_timeframe]}', o.order_purchase_timestamp)
ORDER BY timeframe
"""

trend_df = pd.read_sql(trend_query, engine)

col1, col2 = st.columns(2)
with col1:
    # Orders and Revenue
    fig = px.line(trend_df, x='timeframe', y=['order_count', 'total_revenue'],
                title=f'Orders and Revenue Over Time ({selected_timeframe})',
                labels={'timeframe': timeframe_map[selected_timeframe].capitalize(), 'value': 'Count/Revenue'})
    fig.update_layout(yaxis_title='Value')
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Customer metrics
    fig = px.line(trend_df, x='timeframe', y=['avg_order_value', 'unique_customers'],
                title=f'Customer Metrics Over Time ({selected_timeframe})',
                labels={'timeframe': timeframe_map[selected_timeframe].capitalize(), 'value': 'Value'})
    fig.update_layout(yaxis_title='Value')
    st.plotly_chart(fig, use_container_width=True)

# Add seasonality analysis
st.subheader("Seasonality Analysis")
seasonality_query = f"""
SELECT 
    EXTRACT(MONTH FROM o.order_purchase_timestamp) as month,
    EXTRACT(DOW FROM o.order_purchase_timestamp) as day_of_week,
    COUNT(*) as order_count,
    AVG(oi.price) as avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_purchase_timestamp BETWEEN '{start_date}' AND '{end_date}'
    AND {base_filters_str if base_filters_str else "1=1"}
GROUP BY EXTRACT(MONTH FROM o.order_purchase_timestamp), EXTRACT(DOW FROM o.order_purchase_timestamp)
"""

seasonality_df = pd.read_sql(seasonality_query, engine)

col1, col2 = st.columns(2)
with col1:
    # Monthly seasonality
    fig = px.bar(seasonality_df, x='month', y='order_count',
                title='Monthly Seasonality',
                labels={'month': 'Month', 'order_count': 'Order Count'})
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Weekly seasonality
    fig = px.bar(seasonality_df, x='day_of_week', y='avg_order_value',
                title='Weekly Seasonality',
                labels={'day_of_week': 'Day of Week', 'avg_order_value': 'Average Order Value'})
    st.plotly_chart(fig, use_container_width=True)

# Add forecast section
st.subheader("Sales Forecast")

# Simple moving average forecast
forecast_query = f"""
SELECT 
    DATE_TRUNC('month', o.order_purchase_timestamp) as month,
    SUM(oi.price) as total_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_purchase_timestamp BETWEEN '{start_date}' AND '{end_date}'
    AND {base_filters_str if base_filters_str else "1=1"}
GROUP BY DATE_TRUNC('month', o.order_purchase_timestamp)
ORDER BY month
"""

forecast_df = pd.read_sql(forecast_query, engine)
forecast_df['forecast'] = forecast_df['total_revenue'].rolling(window=3).mean()

fig = go.Figure()
fig.add_trace(go.Scatter(x=forecast_df['month'], y=forecast_df['total_revenue'],
                        mode='lines', name='Actual Revenue'))
fig.add_trace(go.Scatter(x=forecast_df['month'], y=forecast_df['forecast'],
                        mode='lines', name='3-Month Forecast'))
fig.update_layout(title='Revenue Forecast', xaxis_title='Month', yaxis_title='Revenue')
st.plotly_chart(fig, use_container_width=True)

# Geographical analysis
st.header("Geographical Insights")
geo_query = f"""
SELECT g.geolocation_state,
       COUNT(*) as order_count
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN geolocation g ON c.geolocation_id = g.geolocation_id
WHERE o.order_purchase_timestamp BETWEEN '{start_date}' AND '{end_date}'
GROUP BY g.geolocation_state
ORDER BY order_count DESC
LIMIT 10
"""

geo_df = pd.read_sql(geo_query, engine)
fig = px.bar(geo_df, x='geolocation_state', y='order_count',
            title='Top States by Order Count',
            labels={'geolocation_state': 'State', 'order_count': 'Order Count'})
st.plotly_chart(fig, use_container_width=True)

# Footer
st.markdown("""
---
Created with ❤️ by Cascade AI

## How to Use
1. Use the sidebar filters to select date range, categories, states, and payment types
2. Explore different timeframes for time-based analysis
3. Analyze customer segments and their behavior
4. Track payment trends and preferences
5. Monitor customer satisfaction and review patterns

## Key Insights
- Identify peak sales periods and seasonality
- Understand customer purchase patterns
- Track payment method trends
- Monitor customer satisfaction
- Analyze customer retention rates
""")
