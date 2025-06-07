# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

# Import helper functions and queries
from helpers import run_query
from queries import (
    get_delivery_performance_query,
    get_product_performance_query,
    get_customer_segmentation_query,
    get_sales_trends_query
)

# --- Page Configuration ---
st.set_page_config(
    page_title="E-commerce Analytics Dashboard",
    page_icon="🛒",
    layout="wide"
)

# --- Main App ---
st.title("🛒 E-commerce Analytics Dashboard")
st.markdown("Explore sales, product performance, delivery efficiency, and customer segments.")

# --- Sidebar for Navigation ---
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Choose a dashboard",
    ["Delivery Performance", "Product Performance", "Customer Segmentation", "Sales Trends"]
)

# --- Data Loading ---
# Initialize dataframes to None
df_delivery = pd.DataFrame()
df_products = pd.DataFrame()
df_customers = pd.DataFrame()
df_sales = pd.DataFrame()

# Load data based on the selected page to optimize performance
if page == "Delivery Performance":
    df_delivery = run_query(get_delivery_performance_query())
elif page == "Product Performance":
    df_products = run_query(get_product_performance_query())
elif page == "Customer Segmentation":
    df_customers = run_query(get_customer_segmentation_query())
elif page == "Sales Trends":
    df_sales = run_query(get_sales_trends_query())


# --- Dashboard Pages ---

# 1. Delivery Performance Page
if page == "Delivery Performance":
    st.header("🚚 Delivery Performance Analysis")
    
    # Define individual queries
    queries = {
        'orders_query': """
        SELECT 
            'ORDERS' as table_name,
            ORDER_ID,
            ORDER_STATUS,
            TO_CHAR(ORDER_PURCHASE_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') as purchase_date,
            TO_CHAR(ORDER_DELIVERED_CUSTOMER_DATE, 'YYYY-MM-DD HH24:MI:SS') as delivery_date,
            TO_CHAR(ORDER_APPROVED_AT, 'YYYY-MM-DD HH24:MI:SS') as approved_date,
            TO_CHAR(ORDER_DELIVERED_CARRIER_DATE, 'YYYY-MM-DD HH24:MI:SS') as carrier_date,
            TO_CHAR(ORDER_ESTIMATED_DELIVERY_DATE, 'YYYY-MM-DD HH24:MI:SS') as estimated_date
        FROM ECOM_DWH.PUBLIC.DWH_ORDERS
        WHERE ORDER_PURCHASE_TIMESTAMP IS NOT NULL
        AND ORDER_DELIVERED_CUSTOMER_DATE IS NOT NULL
        AND ORDER_PURCHASE_TIMESTAMP > DATEADD(year, -2, CURRENT_DATE())
        LIMIT 10
        """,
        'order_items_query': """
        SELECT 
            'ORDER_ITEMS' as table_name,
            ORDER_ID,
            PRODUCT_ID,
            SELLER_ID,
            PRICE,
            FREIGHT_VALUE
        FROM ECOM_DWH.PUBLIC.DWH_ORDER_ITEMS
        LIMIT 10
        """,
        'customers_query': """
        SELECT 
            'CUSTOMERS' as table_name,
            CUSTOMER_ID,
            CUSTOMER_UNIQUE_ID,
            CUSTOMER_CITY,
            CUSTOMER_STATE,
            CUSTOMER_ZIP_CODE_PREFIX
        FROM ECOM_DWH.PUBLIC.DWH_CUSTOMERS
        LIMIT 10
        """,
        'sellers_query': """
        SELECT 
            'SELLERS' as table_name,
            SELLER_ID,
            SELLER_CITY,
            SELLER_STATE
        FROM ECOM_DWH.PUBLIC.DWH_SELLERS
        LIMIT 10
        """
    }
    
    # Run each query and display results
    for query_name, query in queries.items():
        try:
            df = run_query(query)
            if not df.empty:
                st.subheader(f"Results for {query_name}:")
                st.code(query)
                st.dataframe(df)
            else:
                st.warning(f"No data returned for {query_name}")
        except Exception as e:
            st.error(f"Error executing {query_name}: {e}")
            st.code(query)

    if not df_delivery.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Avg. Total Delivery Days", f"{df_delivery['TOTAL_DELIVERY_DAYS'].mean():.1f}")
        col2.metric("Avg. Vendor Processing Days", f"{df_delivery['VENDOR_PROCESSING_DAYS'].mean():.1f}")
        col3.metric("Avg. Shipping Days", f"{df_delivery['SHIPPING_DAYS'].mean():.1f}")

        st.markdown("---")
        
        st.subheader("Average Delivery Time by State")
        state_delivery_time = df_delivery.groupby('CUSTOMER_STATE')['TOTAL_DELIVERY_DAYS'].mean().reset_index()
        fig_map = px.choropleth(
            state_delivery_time,
            locations='CUSTOMER_STATE',
            locationmode="USA-states",
            color='TOTAL_DELIVERY_DAYS',
            scope="usa",
            color_continuous_scale="RdYlGn_r",
            title="Average Delivery Days"
        )
        st.plotly_chart(fig_map, use_container_width=True)

        st.subheader("Delivery Time Trend by Month")
        monthly_delivery_trend = df_delivery.groupby('ORDER_MONTH')['TOTAL_DELIVERY_DAYS'].mean().reset_index()
        fig_trend = px.line(
            monthly_delivery_trend,
            x='ORDER_MONTH',
            y='TOTAL_DELIVERY_DAYS',
            title="Average Delivery Days Over Time",
            markers=True
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.warning("No data available to display for the Delivery Performance dashboard. Please check the data source and query configuration.")


# 2. Product Performance Page
elif page == "Product Performance":
    if not df_products.empty:
        st.header("📦 Product Category Performance")

        st.subheader("Top 10 Product Categories by Revenue")
        top_10_revenue = df_products.nlargest(10, 'TOTAL_REVENUE')
        fig_revenue = px.bar(
            top_10_revenue,
            x='TOTAL_REVENUE',
            y='PRODUCT_CATEGORY_NAME',
            orientation='h',
            title="Top 10 Categories by Total Revenue",
            labels={'PRODUCT_CATEGORY_NAME': 'Product Category', 'TOTAL_REVENUE': 'Total Revenue (R$)'}
        )
        st.plotly_chart(fig_revenue, use_container_width=True)

        st.subheader("Revenue vs. Return Risk Score by Category")
        fig_scatter = px.scatter(
            df_products,
            x='TOTAL_REVENUE',
            y='RETURN_RISK_SCORE',
            size='NUM_ORDERS',
            color='AVG_REVIEW_SCORE',
            hover_name='PRODUCT_CATEGORY_NAME',
            # FIX: Changed `sequential` to `diverging` and corrected the color scale name.
            color_continuous_scale=px.colors.diverging.RdYlGn,
            size_max=60,
            title="Category Performance: Revenue, Risk, and Reviews",
            labels={
                'TOTAL_REVENUE': 'Total Revenue',
                'RETURN_RISK_SCORE': 'Return Risk (Low Reviews / Orders)',
                'AVG_REVIEW_SCORE': 'Average Review Score'
            }
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.warning("No data available to display for the Product Performance dashboard. This may be due to a query error or no data being returned.")


# 3. Customer Segmentation Page
elif page == "Customer Segmentation":
    if not df_customers.empty:
        st.header("👥 Customer Segmentation")

        # --- ROBUST FIX FOR OverflowError ---

        # Clean and validate the data
        df_customers = df_customers.dropna(subset=['CUSTOMER_UNIQUE_ID', 'TOTAL_ORDERS', 'TOTAL_SPENT', 'AVG_REVIEW'])
        
        # Filter out invalid values
        df_customers = df_customers[
            (df_customers['TOTAL_ORDERS'] > 0) &
            (df_customers['TOTAL_SPENT'] > 0) &
            (df_customers['AVG_REVIEW'].notna())
        ]
        
        # Calculate days since last purchase safely
        now = pd.Timestamp.now()
        df_customers['LAST_PURCHASE_DATE'] = pd.to_datetime(df_customers['LAST_PURCHASE_DATE'], errors='coerce')
        
        # Filter out any dates that are too far in the past or in the future
        max_days = 3650  # 10 years maximum
        df_customers = df_customers[
            (df_customers['LAST_PURCHASE_DATE'].notna()) &
            (df_customers['LAST_PURCHASE_DATE'] > (now - pd.Timedelta(days=max_days))) &
            (df_customers['LAST_PURCHASE_DATE'] <= now)
        ]
        
        # Calculate days since last purchase using a safer method
        df_customers['DAYS_SINCE_LAST_PURCHASE'] = (now - df_customers['LAST_PURCHASE_DATE']).dt.total_seconds().astype('float64') / (24 * 3600)
        df_customers['DAYS_SINCE_LAST_PURCHASE'] = df_customers['DAYS_SINCE_LAST_PURCHASE'].fillna(0).astype(int)
        
        # Debug: Show data statistics
        st.write("Data Statistics:")
        st.write(f"Total customers: {len(df_customers)}")
        st.write(f"Average orders per customer: {df_customers['TOTAL_ORDERS'].mean():.1f}")
        st.write(f"Average spending per customer: ${df_customers['TOTAL_SPENT'].mean():,.2f}")
        st.write(f"Average review score: {df_customers['AVG_REVIEW'].mean():.2f}")
        
        # Only proceed if we have valid data
        if not df_customers.empty:

            # Create segments with more robust conditions
            df_customers['SEGMENT'] = 'Regular'
            df_customers.loc[df_customers['TOTAL_SPENT'] > 1000, 'SEGMENT'] = 'High Value'
            df_customers.loc[df_customers['TOTAL_ORDERS'] > 5, 'SEGMENT'] = 'Frequent Buyer'
            df_customers.loc[df_customers['DAYS_SINCE_LAST_PURCHASE'] > 180, 'SEGMENT'] = 'At Risk'
            
            # Get segment distribution
            segment_counts = df_customers['SEGMENT'].value_counts()
            
            # Debug: Show segment distribution
            st.write("\nSegment Distribution:")
            st.write(segment_counts)
            
            # Only show high-value customers if they exist
            if 'High Value' in df_customers['SEGMENT'].values:
                st.subheader("Top High-Value Customers")
                st.dataframe(
                    df_customers[df_customers['SEGMENT'] == 'High Value']
                    .sort_values('TOTAL_SPENT', ascending=False)
                    .head(10)
                    [['CUSTOMER_UNIQUE_ID', 'TOTAL_ORDERS', 'TOTAL_SPENT', 'AVG_REVIEW', 'DAYS_SINCE_LAST_PURCHASE']]
                )
            
            # Create and display pie chart
            fig_pie = px.pie(
                names=segment_counts.index,
                values=segment_counts.values,
                title='Customer Segment Distribution',
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            
            # Add percentage labels to the pie chart
            fig_pie.update_traces(textinfo='percent+label')
            
            # Display the pie chart
            st.plotly_chart(fig_pie, use_container_width=True)

            st.subheader("Top High-Value Customers")
            st.dataframe(
                df_customers[df_customers['SEGMENT'] == 'High Value']
                .sort_values('TOTAL_SPENT', ascending=False)
                .head(10)
            )
        else:
            st.warning("No valid customer purchase data available for segmentation after cleaning. All records may have missing or invalid last purchase dates.")

    else:
        st.warning("No data available to display for the Customer Segmentation dashboard. This may be due to a query error or no data being returned.")



# 4. Sales Trends Page
elif page == "Sales Trends":
    if not df_sales.empty:
        st.header("📈 Sales Trends & Analysis")

        st.subheader("Overall Monthly Revenue")
        monthly_revenue = df_sales.groupby('ORDER_MONTH')['MONTHLY_REVENUE'].sum().reset_index()
        fig_sales_line = px.line(
            monthly_revenue,
            x='ORDER_MONTH',
            y='MONTHLY_REVENUE',
            title="Total Revenue Over Time",
            labels={'MONTHLY_REVENUE': 'Total Revenue (R$)', 'ORDER_MONTH': 'Month'}
        )
        st.plotly_chart(fig_sales_line, use_container_width=True)

        st.subheader("Monthly Revenue by State")
        state_filter = st.selectbox("Select a State to Analyze", options=['All'] + sorted(df_sales['CUSTOMER_STATE'].unique().tolist()))
        
        if state_filter != 'All':
            filtered_df = df_sales[df_sales['CUSTOMER_STATE'] == state_filter]
        else:
            filtered_df = df_sales
            
        state_revenue = filtered_df.groupby('ORDER_MONTH')['MONTHLY_REVENUE'].sum().reset_index()
        fig_state_line = px.line(
            state_revenue,
            x='ORDER_MONTH',
            y='MONTHLY_REVENUE',
            title=f"Monthly Revenue for {state_filter}",
            labels={'MONTHLY_REVENUE': 'Total Revenue (R$)', 'ORDER_MONTH': 'Month'}
        )
        st.plotly_chart(fig_state_line, use_container_width=True)
    else:
        st.warning("No data available to display for the Sales Trends dashboard. Please check the data source and query configuration.")