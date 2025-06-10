import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils import (
    get_snowflake_connection,
    load_sales_overview_data,
    load_daily_sales_data,
    load_sales_by_geolocation
)

# --- Page Title ---
st.title("🏛️ Historical E-commerce Performance")
st.markdown("Analysis of aggregated data from the E-Commerce Data Warehouse.")

# --- Load Data and Display Dashboard ---
conn = get_snowflake_connection()

if conn:
    with st.spinner("Loading historical data from Warehouse... ❄️"):
        sales_df = load_sales_overview_data(conn)
        daily_sales_df = load_daily_sales_data(conn)
        geo_sales_df = load_sales_by_geolocation(conn)

        # --- KPIs for Historical Data ---
        st.markdown("---")
        st.subheader("🚀 Overall Performance Metrics")

        total_revenue = sales_df['PAYMENT_VALUE'].sum()
        total_orders = sales_df['ORDER_ID'].nunique()
        unique_customers = sales_df['CUSTOMER_UNIQUE_ID'].nunique()
        avg_review_score = sales_df['REVIEW_SCORE'].mean()

        kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
        kpi_col1.metric("💰 Total Revenue", f"${total_revenue:,.2f}")
        kpi_col2.metric("📦 Total Orders", f"{total_orders:,}")
        kpi_col3.metric("👥 Unique Customers", f"{unique_customers:,}")
        kpi_col4.metric("⭐ Average Review Score", f"{avg_review_score:.2f}")

        st.markdown("---")

        # --- Time Series and Geo Charts ---
        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            st.subheader("📅 Daily Sales Trend")
            if not daily_sales_df.empty:
                fig_daily_sales = px.line(
                    daily_sales_df,
                    x='date',
                    y='total_sales',
                    title="Total Revenue Over Time",
                    labels={'date': 'Date', 'total_sales': 'Total Revenue ($)'}
                )
                st.plotly_chart(fig_daily_sales, use_container_width=True)
            else:
                st.warning("No daily sales data available.")

        with chart_col2:
            # Create sub-columns within chart_col2 to push content to the right
            # You can adjust the ratio to control how much it's pushed
            # For example, (0.1, 0.9) leaves 10% empty space on the left
            # (0.2, 0.8) leaves 20% empty space on the left
            empty_space_col, content_col = st.columns([0.1, 0.9]) # Adjust ratio as needed

            with content_col: # Place your content inside the 'content_col'
                st.subheader("🗺️ Sales by Customer State")
                if not geo_sales_df.empty:
                    # Ensure the 'total_sales' column is numeric
                    geo_sales_df['total_sales'] = pd.to_numeric(geo_sales_df['total_sales'], errors='coerce')
                    geo_sales_df.dropna(subset=['latitude', 'longitude', 'total_sales'], inplace=True)

                    # Normalize 'total_sales' for better map visualization
                    if not geo_sales_df.empty:
                        min_sales = geo_sales_df['total_sales'].min()
                        max_sales = geo_sales_df['total_sales'].max()

                        if max_sales > min_sales:  # Avoid division by zero if all sales are the same
                            geo_sales_df['scaled_sales'] = (
                                (geo_sales_df['total_sales'] - min_sales) / (max_sales - min_sales)
                            ) * 95 + 5  # Scale to a range of 5 to 100 for marker size
                        else:
                            geo_sales_df['scaled_sales'] = 5  # Default size if all sales are the same

                        # Display the map
                        st.map(
                            geo_sales_df,
                            latitude='latitude',
                            longitude='longitude',
                            size='scaled_sales',
                            use_container_width=True
                        )
                    else:
                        st.warning("No valid geolocation data available.")
                else:
                    st.warning("No geolocation data available.")

        # --- Leaderboards and Distributions ---
        st.markdown("---")
        lb_col1, lb_col2 = st.columns(2)

        with lb_col1:
            st.subheader("🛍️ Top 10 Product Categories by Revenue")
            if not sales_df.empty:
                category_sales = sales_df.groupby('PRODUCT_CATEGORY_NAME')['PAYMENT_VALUE'].sum().nlargest(10).reset_index()
                category_sales.columns = ['Product Category', 'Total Revenue']
                st.dataframe(category_sales, use_container_width=True, hide_index=True)
            else:
                st.warning("No category sales data available.")

        with lb_col2:
            st.subheader("🏆 Top 10 Seller States by Revenue")
            if not sales_df.empty and 'SELLER_STATE' in sales_df.columns:
                seller_state_sales = sales_df.groupby('SELLER_STATE')['PAYMENT_VALUE'].sum().nlargest(10).reset_index()
                seller_state_sales.columns = ['Seller State', 'Total Revenue']
                st.dataframe(seller_state_sales, use_container_width=True, hide_index=True)
            else:
                st.warning("No seller state data available.")

        st.markdown("---")

        # --- Payment Type Distribution ---
        st.subheader("💳 Payment Method Distribution")
        if not sales_df.empty and 'PAYMENT_TYPE' in sales_df.columns:
            payment_dist = sales_df['PAYMENT_TYPE'].value_counts().reset_index()
            payment_dist.columns = ['Payment Type', 'Number of Transactions']

            # Create a pie chart
            fig_payment = px.pie(
                payment_dist,
                names='Payment Type',
                values='Number of Transactions',
                title='Popularity of Payment Methods',
                hole=0.3
            )
            st.plotly_chart(fig_payment, use_container_width=True)
        else:
            st.warning("No payment method data available.")

        # --- Additional Analytics ---
        st.markdown("---")
        st.subheader("📊 Additional Analytics")

        # Add more analytics sections as needed
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 📦 Orders by Status")
            if not sales_df.empty and 'ORDER_STATUS' in sales_df.columns:
                status_counts = sales_df['ORDER_STATUS'].value_counts().reset_index()
                status_counts.columns = ['Status', 'Count']
                fig_status = px.bar(
                    status_counts,
                    x='Status',
                    y='Count',
                    title='Number of Orders by Status',
                    color='Status'
                )
                st.plotly_chart(fig_status, use_container_width=True)
            else:
                st.warning("No order status data available.")

        with col2:
            st.markdown("### 📦 Average Order Value by Category")
            if not sales_df.empty and 'PRODUCT_CATEGORY_NAME' in sales_df.columns:
                avg_order_value = sales_df.groupby('PRODUCT_CATEGORY_NAME')['PAYMENT_VALUE'].mean().sort_values(ascending=False).head(10).reset_index()
                avg_order_value.columns = ['Category', 'Average Order Value']
                fig_avg = px.bar(
                    avg_order_value,
                    x='Category',
                    y='Average Order Value',
                    title='Top 10 Categories by Average Order Value',
                    color='Average Order Value',
                    color_continuous_scale='Viridis'
                )
                st.plotly_chart(fig_avg, use_container_width=True)
            else:
                st.warning("No category data available.")

else:
    st.warning("❌ Could not establish a connection to Snowflake. Please check your credentials in the `.env` file.")
    st.error("""
        **Troubleshooting Tips:**
        1. Verify your Snowflake credentials in the `.env` file
        2. Ensure you have an active internet connection
        3. Check if your Snowflake account is accessible
        4. Verify that the required tables exist in your Snowflake database
    """)