import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from utils import (
    get_snowflake_connection,
    load_sales_overview_data,
    load_daily_sales_data,
    load_sales_by_geolocation,
    process_delivery_and_satisfaction_data,
    calculate_rfm,
    calculate_seller_performance
)

# --- Page Config ---
st.title("🏛️ Historical E-commerce Performance")
st.markdown("Analysis of aggregated data from the E-Commerce Data Warehouse.")

# --- Load Data ---
conn = get_snowflake_connection()
if conn:
    with st.spinner("Loading and processing data from Warehouse... This may take a moment. ❄️"):
        raw_sales_df = load_sales_overview_data(conn)
        daily_sales_df = load_daily_sales_data(conn)
        geo_sales_df = load_sales_by_geolocation(conn)
        
        # --- Data Processing ---
        sales_df = process_delivery_and_satisfaction_data(raw_sales_df)
        rfm_df = calculate_rfm(raw_sales_df)
        seller_perf_df = calculate_seller_performance(sales_df)

    # --- Main Dashboard ---
    
    # --- KPIs ---
    st.markdown("---")
    st.subheader("🚀 Overall Performance Metrics")
    # ... (KPI section remains the same)
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
    total_revenue = sales_df['payment_value'].sum()
    total_orders = sales_df['order_id'].nunique()
    unique_customers = sales_df['customer_unique_id'].nunique()
    avg_review_score = sales_df['review_score'].mean()
    kpi_col1.metric("💰 Total Revenue", f"${total_revenue:,.2f}")
    kpi_col2.metric("📦 Total Orders", f"{total_orders:,}")
    kpi_col3.metric("👥 Unique Customers", f"{unique_customers:,}")
    kpi_col4.metric("⭐ Average Review Score", f"{avg_review_score:.2f}")


    # --- NEW: Customer Segmentation (RFM) ---
    st.markdown("---")
    st.subheader("👥 Customer Segmentation (RFM Analysis)")
    st.markdown("Segmenting customers by Recency, Frequency, and Monetary value to identify key groups.")

    if not rfm_df.empty:
        rfm_col1, rfm_col2 = st.columns([1, 2])
        
        with rfm_col1:
            st.markdown("##### Customer Segment Distribution")
            segment_counts = rfm_df['Segment'].value_counts().reset_index()
            segment_counts.columns = ['Segment', 'Count']
            fig_rfm_dist = px.bar(
                segment_counts, x='Count', y='Segment', orientation='h',
                title="Number of Customers per Segment",
                labels={'Segment': 'RFM Segment', 'Count': 'Number of Customers'}
            )
            fig_rfm_dist.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_rfm_dist, use_container_width=True)

        with rfm_col2:
            st.markdown("##### RFM Segment Overview")
            # Create aggregated view by segment for cleaner visualization
            segment_summary = rfm_df.groupby('Segment').agg({
                'Recency': 'mean',
                'Frequency': 'mean', 
                'Monetary': 'mean',
                'Segment': 'count'
            }).rename(columns={'Segment': 'Customer_Count'}).reset_index()
            
            # Add segment descriptions for better understanding
            segment_descriptions = {
                'Loyal Customers': 'High value, frequent buyers',
                'At Risk': 'High value but haven\'t purchased recently', 
                'Hibernating': 'Low recent activity, low frequency',
                'About to Sleep': 'Recent customers at risk of churning',
                'New Customers': 'Recent first-time buyers',
                'Potential Loyalists': 'Recent customers with good frequency',
                'Needs Attention': 'Below average but not lost yet'
            }
            
            fig_rfm_clean = px.scatter(
                segment_summary,
                x='Recency', 
                y='Frequency', 
                color='Segment',
                size='Customer_Count',
                title="Average RFM Metrics by Customer Segment",
                labels={
                    'Recency': 'Avg Days Since Last Purchase', 
                    'Frequency': 'Avg Total Orders',
                    'Customer_Count': 'Number of Customers'
                },
                hover_data={
                    'Monetary': ':.2f',
                    'Customer_Count': True
                },
                size_max=100
            )
            fig_rfm_clean.update_layout(
                xaxis={'title': 'Avg Days Since Last Purchase (Lower = Better)'},
                yaxis={'title': 'Avg Total Orders (Higher = Better)'}
            )
            st.plotly_chart(fig_rfm_clean, use_container_width=True)
        
        # Move segment insights table outside the columns to center it
        st.markdown("##### Segment Insights")
        insight_df = segment_summary.copy()
        insight_df['Avg_Monetary'] = insight_df['Monetary'].round(2)
        insight_df = insight_df[['Segment', 'Customer_Count', 'Recency', 'Frequency', 'Avg_Monetary']]
        insight_df.columns = ['Segment', 'Customers', 'Avg Recency (Days)', 'Avg Orders', 'Avg Revenue ($)']
        st.dataframe(insight_df.round(1), use_container_width=True, hide_index=True)
    else:
        st.warning("Could not generate RFM segments.")

    # --- NEW: Seller Performance Scorecard ---
    st.markdown("---")
    st.subheader("🏆 Seller Performance Scorecard")
    st.markdown("Evaluating sellers based on revenue, customer satisfaction, and delivery speed.")
    
    if not seller_perf_df.empty:
        # Create sortable and filterable dataframe
        st.dataframe(
            seller_perf_df.sort_values('total_revenue', ascending=False).reset_index(drop=True),
            use_container_width=True,
            column_config={
                "seller_id": "Seller ID",
                "total_revenue": st.column_config.NumberColumn("Total Revenue ($)", format="$ %.2f"),
                "total_orders": "Total Orders",
                "avg_review_score": st.column_config.NumberColumn("Avg. Review (1-5)", format="⭐ %.2f"),
                "avg_delivery_time": "Avg. Delivery (Days)"
            },
            height=400
        )
    else:
        st.warning("Could not generate Seller Performance data.")


    # --- Existing Sections (Delivery, Satisfaction, etc.) ---
    st.markdown("---")
    st.subheader("🚚 Delivery & Satisfaction Insights")
    sat_col1, sat_col2 = st.columns(2)

    with sat_col1:
        st.markdown("##### Impact of Delivery on Review Score")
        if not sales_df.empty and 'delivery_status' in sales_df.columns:
            satisfaction_by_delivery = sales_df.groupby('delivery_status')['review_score'].mean().reset_index()
            fig_sat_delivery = px.bar(
                satisfaction_by_delivery,
                x='delivery_status', y='review_score', color='delivery_status',
                title="Average Review Score for On-Time vs. Late Deliveries",
                labels={'delivery_status': 'Delivery Status', 'review_score': 'Average Review Score'},
                color_discrete_map={'On-Time': 'green', 'Late': 'red'}
            )
            st.plotly_chart(fig_sat_delivery, use_container_width=True)
    
    with sat_col2:
        st.markdown("##### Delivery Timeliness Distribution (in Days)")
        if not sales_df.empty and 'delivery_delta_days' in sales_df.columns:
            fig_delivery_delta = px.histogram(
                sales_df, x='delivery_delta_days', nbins=100,
                title='Distribution of Delivery Difference (Actual - Estimated)',
                labels={'delivery_delta_days': 'Days Early (< 0) or Late (> 0)'}
            )
            st.plotly_chart(fig_delivery_delta, use_container_width=True)

    st.markdown("---")
    st.subheader("⭐ Customer Satisfaction Deep Dive")
    sat_col1, sat_col2 = st.columns(2)

    with sat_col1:
        st.markdown("##### Review Score Distribution")
        if not sales_df.empty and 'review_score' in sales_df.columns:
            fig_review_dist = px.histogram(
                sales_df, x='review_score', nbins=5,
                title='Distribution of Customer Review Scores',
                labels={'review_score': 'Review Score (1-5)', 'count': 'Number of Reviews'}
            )
            st.plotly_chart(fig_review_dist, use_container_width=True)
        else:
            st.warning("No review score data available.")
            
    with sat_col2:
        st.markdown("##### Top & Bottom 5 Categories by Review Score")
        if not sales_df.empty and 'product_category_name' in sales_df.columns:
            category_reviews = sales_df.groupby('product_category_name')['review_score'].mean().dropna().sort_values()
            
            top_5 = category_reviews.nlargest(5)
            bottom_5 = category_reviews.nsmallest(5)
            combined_reviews = pd.concat([top_5, bottom_5]).reset_index()
            combined_reviews['Performance'] = ['Top 5'] * 5 + ['Bottom 5'] * 5
            
            fig_cat_reviews = px.bar(
                combined_reviews,
                x='review_score', y='product_category_name', color='Performance',
                orientation='h',
                title="Highest and Lowest Rated Product Categories",
                labels={'product_category_name': 'Product Category', 'review_score': 'Average Review Score'},
                color_discrete_map={'Top 5': '#2ca02c', 'Bottom 5': '#d62728'}
            )
            fig_cat_reviews.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_cat_reviews, use_container_width=True)
        else:
            st.warning("No category review data available.")


    st.markdown("---")
    st.subheader("🏆 Leaderboards & Distributions")
    lb_col1, lb_col2, lb_col3 = st.columns(3)

    with lb_col1:
        st.markdown("##### Top 10 Categories by Revenue")
        if not sales_df.empty:
            category_sales = sales_df.groupby('product_category_name')['payment_value'].sum().nlargest(10).reset_index()
            st.dataframe(category_sales, use_container_width=True, hide_index=True)
        else:
            st.warning("No category sales data available.")

    with lb_col2:
        st.markdown("##### Top 10 Seller States by Revenue")
        if not sales_df.empty and 'seller_state' in sales_df.columns:
            seller_state_sales = sales_df.groupby('seller_state')['payment_value'].sum().nlargest(10).reset_index()
            st.dataframe(seller_state_sales, use_container_width=True, hide_index=True)
        else:
            st.warning("No seller state data available.")

    with lb_col3:
        st.markdown("##### Payment Installments Distribution")
        if not sales_df.empty and 'payment_installments' in sales_df.columns:
            cc_payments = sales_df[sales_df['payment_type'] == 'credit_card']
            fig_installments = px.histogram(
                cc_payments,
                x='payment_installments',
                title='Credit Card Installment Counts',
                labels={'payment_installments': 'Number of Installments'},
                nbins=int(cc_payments['payment_installments'].max())
            )
            st.plotly_chart(fig_installments, use_container_width=True)
        else:
            st.warning("No payment installment data available.")

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("##### Payment Method Popularity")
        if not sales_df.empty and 'payment_type' in sales_df.columns:
            payment_dist = sales_df['payment_type'].value_counts().reset_index()
            payment_dist.columns = ['Payment Type', 'Transactions']
            fig_payment = px.pie(
                payment_dist, names='Payment Type', values='Transactions',
                title='Popularity of Payment Methods', hole=0.3
            )
            st.plotly_chart(fig_payment, use_container_width=True)
        else:
            st.warning("No payment method data available.")

    with col2:
        st.markdown("##### Order Status Counts")
        if not raw_sales_df.empty and 'order_status' in raw_sales_df.columns:
            status_counts = raw_sales_df['order_status'].value_counts().reset_index() 
            status_counts.columns = ['Status', 'Count']
            fig_status = px.bar(
                status_counts, x='Status', y='Count', title='Number of Orders by Status', color='Status'
            )
            st.plotly_chart(fig_status, use_container_width=True)
        else:
            st.warning("No order status data available.")


else:
    st.warning("❌ Could not establish a connection to Snowflake. Please check your credentials in the `.env` file.")
    st.error("""
        **Troubleshooting Tips:**
        1. Verify your Snowflake credentials in the `.env` file
        2. Ensure you have an active internet connection
        3. Check if your Snowflake account is accessible
        4. Verify that the required tables exist in your Snowflake database
    """)