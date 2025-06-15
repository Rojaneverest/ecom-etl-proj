import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from utils import (
    get_snowflake_connection,
    load_sales_overview_data,
    load_daily_sales_data,
    load_sales_by_geolocation,
    process_delivery_and_satisfaction_data,
    calculate_rfm,
    calculate_seller_performance,
    check_tables_exist
)

st.title("Historical E-commerce Performance")
st.markdown("Analysis of aggregated data from the E-Commerce Data Warehouse.")

# --- Define list of essential tables for the dashboard ---
REQUIRED_TABLES = [
    "DWH_ORDERS", "DWH_ORDER_ITEMS", "DWH_ORDER_PAYMENTS",
    "DWH_PRODUCTS", "DWH_CUSTOMERS", "DWH_SELLERS",
    "DWH_ORDER_REVIEWS", "DWH_GEOLOCATION"
]

# --- Main Logic with Integrated Table Check ---
conn = get_snowflake_connection()

if conn:
    missing_tables = []
    with st.spinner("Connecting to data warehouse and verifying data availability... ❄️"):
        missing_tables = check_tables_exist(conn, REQUIRED_TABLES)

    # --- LOGIC 1: CHECK IF TABLES ARE MISSING ---
    if missing_tables:
        st.error("Data Not Available", icon="🚨")
        st.warning("The dashboard cannot be displayed because one or more source tables are missing in Snowflake.")
        st.markdown("---")
        st.info("Please ensure that your data ingestion and transformation (ETL/ELT) processes have run successfully and that the table names match the required list.")
        st.stop() # Halt execution to prevent errors

    # --- LOGIC 2: IF TABLES EXIST, PROCEED TO LOAD DATA ---
    else:
        with st.spinner("Loading and processing data... This may take a moment. ❄️"):
            raw_sales_df = load_sales_overview_data(conn)

        # --- LOGIC 3: CHECK IF TABLES EXIST BUT ARE EMPTY ---
        if raw_sales_df.empty:
            st.warning("Data Not Available", icon="⚠️")
            st.info("The required data tables exist in Snowflake, but they appear to be empty.")
            st.markdown("Please verify that the data ingestion process has successfully loaded records into the tables.")
            st.stop() # Halt execution as no charts can be rendered

        # --- If data exists, proceed with the full dashboard logic ---
        else:
            with st.spinner("Analyzing data and building visualizations... ⚙️"):
                daily_sales_df = load_daily_sales_data(conn)
                geo_sales_df = load_sales_by_geolocation(conn)
                sales_df = process_delivery_and_satisfaction_data(raw_sales_df)
                rfm_df = calculate_rfm(raw_sales_df)
                seller_perf_df = calculate_seller_performance(sales_df)

    # --- SECTION 1: KPI OVERVIEW ---
    st.markdown("---")
    st.subheader("Overall Performance Metrics")
    
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
    total_revenue = sales_df['payment_value'].sum()
    total_orders = sales_df['order_id'].nunique()
    unique_customers = sales_df['customer_unique_id'].nunique()
    avg_review_score = sales_df['review_score'].mean()
    
    kpi_col1.metric("💰 Total Revenue", f"${total_revenue:,.2f}")
    kpi_col2.metric("📦 Total Orders", f"{total_orders:,}")
    kpi_col3.metric("👥 Unique Customers", f"{unique_customers:,}")
    kpi_col4.metric("⭐ Average Review Score", f"{avg_review_score:.2f}")

    # --- SECTION 2: HISTORICAL TRENDS ---
    st.markdown("---")
    st.subheader("Historical Trends & Geographic Distribution")
    
    trend_col1, trend_col2 = st.columns(2, gap="large")
    
    with trend_col1:
        st.markdown("##### Daily Sales Trend")
        if not daily_sales_df.empty:
            fig_daily_sales = px.line(
                daily_sales_df, 
                x='date', 
                y='total_sales', 
                title="Total Revenue Over Time",
                labels={'total_sales': 'Revenue ($)', 'date': 'Date'}
            )
            fig_daily_sales.update_layout(showlegend=False)
            st.plotly_chart(fig_daily_sales, use_container_width=True)
        else:
            st.info("No daily sales data available.")

# Replace the existing geographic sales visualization section with this improved version

    with trend_col2:
        st.markdown("##### Sales by Customer State")
        if not geo_sales_df.empty:
            # Create an interactive Plotly scatter map with hover data
            fig_geo = px.scatter_mapbox(
                geo_sales_df,
                lat='latitude',
                lon='longitude',
                size='total_sales',
                color='total_sales',
                hover_name='customer_state',  # Assuming you have state names
                hover_data={
                    'total_sales': ':$,.2f',
                    'latitude': ':.2f',
                    'longitude': ':.2f'
                },
                color_continuous_scale='Viridis',
                size_max=30,
                zoom=3,
                labels={
                    'total_sales': 'Total Sales ($)',
                    'customer_state': 'State'
                }
            )
            
            # Update map layout for better appearance
            fig_geo.update_layout(
                mapbox_style="carto-positron",  # Free map style
                height=400,
                margin={"r":0,"t":30,"l":0,"b":0}
            )
            
            # If you want to use a different map style, you can use:
            # mapbox_style="carto-positron"  # Clean, minimal style
            # mapbox_style="stamen-terrain"  # Terrain style
            
            st.plotly_chart(fig_geo, use_container_width=True)

    # --- SECTION 3: CUSTOMER SEGMENTATION ---
    st.markdown("---")
    st.subheader("Customer Segmentation (RFM Analysis)")
    st.markdown("Segmenting customers by Recency, Frequency, and Monetary value to identify key groups.")

    if not rfm_df.empty:
        rfm_col1, rfm_col2 = st.columns([1, 2])
        
        with rfm_col1:
            st.markdown("##### Customer Segment Distribution")
            segment_counts = rfm_df['Segment'].value_counts().reset_index()
            segment_counts.columns = ['Segment', 'Count']
            
            fig_rfm_dist = px.bar(
                segment_counts, 
                x='Count', 
                y='Segment', 
                orientation='h',
                title="Customers per Segment",
                labels={'Segment': 'RFM Segment', 'Count': 'Number of Customers'},
                color='Count',
                color_continuous_scale='viridis'
            )
            fig_rfm_dist.update_layout(
                yaxis={'categoryorder': 'total ascending'},
                showlegend=False
            )
            st.plotly_chart(fig_rfm_dist, use_container_width=True)

        with rfm_col2:
            st.markdown("##### RFM Segment Overview")
            segment_summary = rfm_df.groupby('Segment').agg({
                'Recency': 'mean',
                'Frequency': 'mean', 
                'Monetary': 'mean',
                'Segment': 'count'
            }).rename(columns={'Segment': 'Customer_Count'}).reset_index()
            
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
                size_max=80
            )
            fig_rfm_clean.update_layout(
                xaxis={'title': 'Avg Days Since Last Purchase (Lower = Better)'},
                yaxis={'title': 'Avg Total Orders (Higher = Better)'}
            )
            st.plotly_chart(fig_rfm_clean, use_container_width=True)
        
        # Segment insights table
        st.markdown("##### Segment Insights")
        insight_df = segment_summary.copy()
        insight_df['Avg_Monetary'] = insight_df['Monetary'].round(2)
        insight_df = insight_df[['Segment', 'Customer_Count', 'Recency', 'Frequency', 'Avg_Monetary']]
        insight_df.columns = ['Segment', 'Customers', 'Avg Recency (Days)', 'Avg Orders', 'Avg Revenue ($)']
        st.dataframe(insight_df.round(1), use_container_width=True, hide_index=True)
    else:
        st.warning("Could not generate RFM segments.")

    # --- SECTION 4: SELLER PERFORMANCE ---
    st.markdown("---")
    st.subheader("Seller Performance Scorecard")
    st.markdown("Evaluating sellers based on revenue, customer satisfaction, and delivery speed.")

    if not seller_perf_df.empty:
        # Sort the DataFrame first
        sorted_sellers = seller_perf_df.sort_values('total_revenue', ascending=False).reset_index(drop=True)
        
        # --- PAGINATION LOGIC ---
        items_per_page = 10
        
        # Initialize session state for the page number
        if 'page_number' not in st.session_state:
            st.session_state.page_number = 0

        total_items = len(sorted_sellers)
        total_pages = (total_items // items_per_page) + (1 if total_items % items_per_page > 0 else 0)

        # Get the start and end indices for the current page
        start_idx = st.session_state.page_number * items_per_page
        end_idx = start_idx + items_per_page
        paginated_df = sorted_sellers.iloc[start_idx:end_idx]

        # Display the DataFrame for the current page
        st.dataframe(
            paginated_df,
            use_container_width=True,
            column_config={
                "seller_id": "Seller ID",
                "total_revenue": st.column_config.NumberColumn("Total Revenue ($)", format="$ %.2f"),
                "total_orders": "Total Orders",
                "avg_review_score": st.column_config.NumberColumn("Avg. Review (1-5)", format="⭐ %.2f"),
                "avg_delivery_time": "Avg. Delivery (Days)"
            },
            hide_index=True
        )

        # --- PAGINATION CONTROLS ---
        # Create columns for the navigation buttons and page indicator
        nav_col1, nav_col2, nav_col3, _ = st.columns([0.15, 0.15, 0.2, 0.5])

        # "Previous" button
        if nav_col1.button("Previous", use_container_width=True, disabled=(st.session_state.page_number == 0)):
            st.session_state.page_number -= 1
            st.experimental_rerun()

        # "Next" button
        if nav_col2.button("Next", use_container_width=True, disabled=(st.session_state.page_number >= total_pages - 1)):
            st.session_state.page_number += 1
            st.experimental_rerun()

        # Page indicator
        nav_col3.markdown(f"Page **{st.session_state.page_number + 1}** of **{total_pages}**")

    else:
        st.warning("Could not generate Seller Performance data.")

    # --- SECTION 5: DELIVERY & SATISFACTION ---
    st.markdown("---")
    st.subheader("Delivery & Customer Satisfaction Analysis")
    
    # Delivery Analysis
    delivery_col1, delivery_col2 = st.columns(2)

    with delivery_col1:
        st.markdown("##### Impact of Delivery on Review Score")
        if not sales_df.empty and 'delivery_status' in sales_df.columns:
            satisfaction_by_delivery = sales_df.groupby('delivery_status')['review_score'].mean().reset_index()
            fig_sat_delivery = px.bar(
                satisfaction_by_delivery,
                x='delivery_status', 
                y='review_score', 
                color='delivery_status',
                title="Avg Review Score: On-Time vs Late Deliveries",
                labels={'delivery_status': 'Delivery Status', 'review_score': 'Average Review Score'},
                color_discrete_map={'On-Time': '#2a9d8f', 'Late': '#e76f51'}
            )
            fig_sat_delivery.update_layout(showlegend=False)
            st.plotly_chart(fig_sat_delivery, use_container_width=True)
    
    with delivery_col2:
        st.markdown("##### Delivery Timeliness Distribution")
        if not sales_df.empty and 'delivery_delta_days' in sales_df.columns:
            fig_delivery_delta = px.histogram(
                sales_df, 
                x='delivery_delta_days', 
                nbins=50,
                title='Delivery Time Difference (Actual - Estimated)',
                labels={'delivery_delta_days': 'Days Early (< 0) or Late (> 0)', 'count': 'Number of Orders'},
                color_discrete_sequence=['#428BCA']
            )
            fig_delivery_delta.add_vline(x=0, line_dash="dash", line_color="red", 
                                       annotation_text="On Time", annotation_position="top")
            st.plotly_chart(fig_delivery_delta, use_container_width=True)

    # Customer Satisfaction Analysis
    sat_col1, sat_col2 = st.columns(2)

    with sat_col1:
        st.markdown("##### Review Score Distribution")
        if not sales_df.empty and 'review_score' in sales_df.columns:
            fig_review_dist = px.histogram(
                sales_df, 
                x='review_score', 
                nbins=5,
                title='Distribution of Customer Review Scores',
                labels={'review_score': 'Review Score (1-5)', 'count': 'Number of Reviews'},
                color_discrete_sequence=['#4a4e69']
            )
            fig_review_dist.update_layout(bargap=0.1)
            st.plotly_chart(fig_review_dist, use_container_width=True)
        else:
            st.warning("No review score data available.")
            
    with sat_col2:
        st.markdown("##### Top & Bottom Categories by Reviews")
        if not sales_df.empty and 'product_category_name' in sales_df.columns:
            category_reviews = sales_df.groupby('product_category_name')['review_score'].mean().dropna().sort_values()
            
            top_5 = category_reviews.nlargest(5)
            bottom_5 = category_reviews.nsmallest(5)
            combined_reviews = pd.concat([top_5, bottom_5]).reset_index()
            combined_reviews['Performance'] = ['Top 5'] * 5 + ['Bottom 5'] * 5
            
            fig_cat_reviews = px.bar(
                combined_reviews,
                x='review_score', 
                y='product_category_name', 
                color='Performance',
                orientation='h',
                title="Highest and Lowest Rated Categories",
                labels={'product_category_name': 'Product Category', 'review_score': 'Average Review Score'},
                color_discrete_map={'Top 5': '#2a9d8f', 'Bottom 5': '#e76f51'}
            )
            fig_cat_reviews.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig_cat_reviews, use_container_width=True)
        else:
            st.warning("No category review data available.")

    # --- SECTION 6: BUSINESS INSIGHTS & LEADERBOARDS ---
    st.markdown("---")
    st.subheader("Business Insights & Leaderboards")
    
    # Top performers
    leader_col1, leader_col2, leader_col3 = st.columns(3)

    with leader_col1:
        st.markdown("##### Top 10 Categories by Revenue")
        if not sales_df.empty:
            category_sales = sales_df.groupby('product_category_name')['payment_value'].sum().nlargest(10).reset_index()
            category_sales['payment_value'] = category_sales['payment_value'].round(2)
            category_sales.columns = ['Category', 'Revenue ($)']
            st.dataframe(category_sales, use_container_width=True, hide_index=True)
        else:
            st.warning("No category sales data available.")

    with leader_col2:
        st.markdown("##### Top 10 Seller States by Revenue")
        if not sales_df.empty and 'seller_state' in sales_df.columns:
            seller_state_sales = sales_df.groupby('seller_state')['payment_value'].sum().nlargest(10).reset_index()
            seller_state_sales['payment_value'] = seller_state_sales['payment_value'].round(2)
            seller_state_sales.columns = ['State', 'Revenue ($)']
            st.dataframe(seller_state_sales, use_container_width=True, hide_index=True)
        else:
            st.warning("No seller state data available.")

    with leader_col3:
        st.markdown("##### Order Status Distribution")
        if not raw_sales_df.empty and 'order_status' in raw_sales_df.columns:
            status_counts = raw_sales_df['order_status'].value_counts().reset_index() 
            status_counts.columns = ['Status', 'Count']

            # Exclude "unavailable"
            status_counts = status_counts[status_counts['Status'].str.lower() != 'unavailable']

            # Pull out small slices
            pull_values = np.where(status_counts['Status'].isin(['shipped', 'canceled']), 0.1, 0)

            fig_status = px.pie(
                status_counts, 
                names='Status', 
                values='Count', 
                hole=0.3,
                color_discrete_map={'shipped': '#5CB85C', 'canceled': '#D9534F', 'delivered': '#669bbc'}
            )

            fig_status.update_traces(
                textinfo='percent',
                textposition='outside',
                pull=pull_values
            )

            st.plotly_chart(fig_status, use_container_width=True)
        else:
            st.warning("No order status data available.")

    # Payment Analysis
    payment_col1, payment_col2 = st.columns(2)

    with payment_col1:
        st.markdown("##### Payment Method Popularity")
        if not sales_df.empty and 'payment_type' in sales_df.columns:
            payment_dist = sales_df['payment_type'].value_counts().reset_index()
            payment_dist.columns = ['Payment Type', 'Transactions']
            
            fig_payment = px.pie(
                payment_dist, 
                names='Payment Type', 
                values='Transactions',
                title='Payment Methods Distribution', 
                hole=0.3,
                color_discrete_map={'credit_card': '#5CB85C', 'canceled': '#D9534F'}
            )
            st.plotly_chart(fig_payment, use_container_width=True)
        else:
            st.warning("No payment method data available.")

    with payment_col2:
        st.markdown("##### Credit Card Installments")
        if not sales_df.empty and 'payment_installments' in sales_df.columns:
            cc_payments = sales_df[sales_df['payment_type'] == 'credit_card']
            if not cc_payments.empty:
                fig_installments = px.histogram(
                    cc_payments,
                    x='payment_installments',
                    title='Credit Card Installment Distribution',
                    labels={'payment_installments': 'Number of Installments', 'count': 'Frequency'},
                    nbins=int(cc_payments['payment_installments'].max()) if cc_payments['payment_installments'].max() > 0 else 10,
                    color_discrete_sequence=['#9467bd']
                )
                st.plotly_chart(fig_installments, use_container_width=True)
            else:
                st.info("No credit card payment data available.")
        else:
            st.warning("No payment installment data available.")

else:
    st.warning("❌ Could not establish a connection to Snowflake. Please check your credentials in the `.env` file.")
    st.error("""
        **Troubleshooting Tips:**
        1. Verify your Snowflake credentials in the `.env` file
        2. Ensure you have an active internet connection
        3. Check if your Snowflake account is accessible
        4. Verify that the required tables exist in your Snowflake database
    """)