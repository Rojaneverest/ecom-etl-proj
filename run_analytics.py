import pandas as pd
import numpy as np
from datetime import timedelta
import os

def calculate_all_metrics(data_path='data/extracted_data'):
    """
    Loads all e-commerce data from CSVs, processes it, and calculates all metrics
    found in the historical analytics dashboard.

    Args:
        data_path (str): The path to the directory containing the CSV files.
    """
    print("--- Starting E-commerce Data Analysis ---")

    # --- 1. Load all datasets from CSV files ---
    print("\n[Step 1/6] Loading all CSV files...")
    try:
        customers_df = pd.read_csv(os.path.join(data_path, 'olist_customers_dataset.csv'))
        geolocation_df = pd.read_csv(os.path.join(data_path, 'olist_geolocation_dataset.csv'))
        order_items_df = pd.read_csv(os.path.join(data_path, 'olist_order_items_dataset.csv'))
        order_payments_df = pd.read_csv(os.path.join(data_path, 'olist_order_payments_dataset.csv'))
        order_reviews_df = pd.read_csv(os.path.join(data_path, 'olist_order_reviews_dataset.csv'))
        orders_df = pd.read_csv(os.path.join(data_path, 'olist_orders_dataset.csv'))
        products_df = pd.read_csv(os.path.join(data_path, 'olist_products_dataset.csv'))
        sellers_df = pd.read_csv(os.path.join(data_path, 'olist_sellers_dataset.csv'))
        category_translation_df = pd.read_csv(os.path.join(data_path, 'product_category_name_translation.csv'))
        print("...all CSVs loaded successfully.")
    except FileNotFoundError as e:
        print(f"\nError: Could not find a CSV file. Please check the path: {data_path}")
        print(f"Details: {e}")
        return

    # --- 2. Merge datasets into a single comprehensive DataFrame ---
    # This step replicates the logic from `load_sales_overview_data`
    print("\n[Step 2/6] Merging all datasets into a single master table...")
    
    # Merge orders with payments, items, reviews, and customers
    merged_df = orders_df.merge(order_payments_df, on='order_id', how='left')
    merged_df = merged_df.merge(order_reviews_df, on='order_id', how='left')
    merged_df = merged_df.merge(order_items_df, on='order_id', how='left')
    merged_df = merged_df.merge(customers_df, on='customer_id', how='left')
    
    # Merge with product, seller, and category translation data
    merged_df = merged_df.merge(products_df, on='product_id', how='left')
    merged_df = merged_df.merge(sellers_df, on='seller_id', how='left')
    merged_df = merged_df.merge(category_translation_df, on='product_category_name', how='left')

    # Use the English category name and drop the original
    merged_df.drop('product_category_name', axis=1, inplace=True)
    merged_df.rename(columns={'product_category_name_english': 'product_category_name'}, inplace=True)

    # For consistency with the dashboard, let's call the final merged df 'sales_df'
    raw_sales_df = merged_df.copy()
    print("...datasets merged successfully.")
    
    # --- 3. Process data for delivery and satisfaction analysis ---
    # This replicates the logic from `process_delivery_and_satisfaction_data`
    print("\n[Step 3/6] Processing data for delivery & satisfaction metrics...")
    sales_df = raw_sales_df.copy()
    
    date_cols = [
        'order_purchase_timestamp',
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    for col in date_cols:
        sales_df[col] = pd.to_datetime(sales_df[col], errors='coerce')

    sales_df.dropna(subset=date_cols, inplace=True)

    sales_df['actual_delivery_time'] = (sales_df['order_delivered_customer_date'] - sales_df['order_purchase_timestamp']).dt.days
    sales_df['delivery_delta_days'] = (sales_df['order_delivered_customer_date'] - sales_df['order_estimated_delivery_date']).dt.days
    
    sales_df['delivery_status'] = np.where(sales_df['order_delivered_customer_date'] <= sales_df['order_estimated_delivery_date'], 'On-Time', 'Late')
    
    # Filter out records with negative delivery time which indicates bad data
    sales_df = sales_df[sales_df['actual_delivery_time'] >= 0]
    print("...delivery and satisfaction data processed.")

    # --- 4. Calculate all metrics and generate data for charts/tables ---
    print("\n[Step 4/6] Calculating all dashboard metrics...")
    
    # --- SECTION 1: KPI OVERVIEW ---
    print("\n--- 🚀 Overall Performance Metrics ---")
    total_revenue = sales_df['payment_value'].sum()
    total_orders = sales_df['order_id'].nunique()
    unique_customers = sales_df['customer_unique_id'].nunique()
    avg_review_score = sales_df['review_score'].mean()
    print(f"💰 Total Revenue: ${total_revenue:,.2f}")
    print(f"📦 Total Orders: {total_orders:,}")
    print(f"👥 Unique Customers: {unique_customers:,}")
    print(f"⭐ Average Review Score: {avg_review_score:.2f}")

    # --- SECTION 2: HISTORICAL TRENDS & GEOGRAPHIC DISTRIBUTION ---
    print("\n--- 📈 Historical Trends & Geographic Distribution ---")
    # Daily Sales Trend (replicates `load_daily_sales_data`)
    daily_sales_df = sales_df.copy()
    daily_sales_df['date'] = pd.to_datetime(daily_sales_df['order_purchase_timestamp']).dt.date
    daily_sales_agg = daily_sales_df.groupby('date')['payment_value'].sum().reset_index()
    daily_sales_agg.rename(columns={'payment_value': 'total_sales'}, inplace=True)
    print("📊 Daily Sales Trend (Top 5 rows):")
    print(daily_sales_agg.head())

    # Sales by Customer State (replicates `load_sales_by_geolocation`)
    geo_sales_df = sales_df.groupby('customer_state')['payment_value'].sum().reset_index()
    geo_sales_df.rename(columns={'payment_value': 'total_sales'}, inplace=True)
    
    # Get average lat/lng for each state from geolocation data
    # We use a simplified approach here by averaging coords for zip codes within a state
    state_coords = geolocation_df.groupby('geolocation_state')[['geolocation_lat', 'geolocation_lng']].mean().reset_index()
    state_coords.rename(columns={'geolocation_state': 'customer_state', 'geolocation_lat': 'latitude', 'geolocation_lng': 'longitude'}, inplace=True)
    
    geo_sales_with_coords = geo_sales_df.merge(state_coords, on='customer_state', how='left')
    print("\n🗺️ Sales by Customer State (Top 5 rows):")
    print(geo_sales_with_coords.sort_values('total_sales', ascending=False).head())

    # --- SECTION 3: CUSTOMER SEGMENTATION (RFM) ---
    print("\n--- 👥 Customer Segmentation (RFM Analysis) ---")
    # Replicates `calculate_rfm`
    rfm_df = sales_df.copy()
    snapshot_date = rfm_df['order_purchase_timestamp'].max() + timedelta(days=1)
    
    rfm_calc = rfm_df.groupby('customer_unique_id').agg({
        'order_purchase_timestamp': lambda x: (snapshot_date - x.max()).days,
        'order_id': 'nunique',
        'payment_value': 'sum'
    }).rename(columns={
        'order_purchase_timestamp': 'Recency',
        'order_id': 'Frequency',
        'payment_value': 'Monetary'
    })

    r_labels, f_labels, m_labels = range(4, 0, -1), range(1, 5), range(1, 5)
    rfm_calc['R_Score'] = pd.qcut(rfm_calc['Recency'].rank(method='first'), q=4, labels=r_labels).astype(int)
    rfm_calc['F_Score'] = pd.qcut(rfm_calc['Frequency'].rank(method='first'), q=4, labels=f_labels).astype(int)
    rfm_calc['M_Score'] = pd.qcut(rfm_calc['Monetary'].rank(method='first'), q=4, labels=m_labels).astype(int)

    def rfm_segment(row):
        if row['R_Score'] >= 4 and row['F_Score'] >= 4: return 'Champions'
        if row['R_Score'] >= 3 and row['F_Score'] >= 3: return 'Loyal Customers'
        if row['R_Score'] >= 3 and row['F_Score'] < 3: return 'Potential Loyalists'
        if row['R_Score'] < 3 and row['F_Score'] >= 4: return 'Cannot Lose'
        if row['R_Score'] >= 2 and row['F_Score'] >= 2: return 'At Risk'
        if row['R_Score'] < 2 and row['F_Score'] < 2: return 'Hibernating'
        return 'Needs Attention'
    
    rfm_calc['Segment'] = rfm_calc.apply(rfm_segment, axis=1)
    
    segment_summary = rfm_calc.groupby('Segment').agg(
        Customer_Count=('Segment', 'count'),
        Recency=('Recency', 'mean'),
        Frequency=('Frequency', 'mean'),
        Monetary=('Monetary', 'mean')
    ).round(1)
    
    print("📊 RFM Segment Summary:")
    print(segment_summary)

    # --- SECTION 4: SELLER PERFORMANCE ---
    print("\n--- 🏆 Seller Performance Scorecard ---")
    # Replicates `calculate_seller_performance`
    seller_perf_df = sales_df.dropna(subset=['seller_id', 'review_score', 'actual_delivery_time'])
    seller_summary = seller_perf_df.groupby('seller_id').agg(
        total_revenue=('payment_value', 'sum'),
        total_orders=('order_id', 'nunique'),
        avg_review_score=('review_score', 'mean'),
        avg_delivery_time=('actual_delivery_time', 'mean')
    ).round(2)
    print("📊 Seller Performance (Top 5 by Revenue):")
    print(seller_summary.sort_values('total_revenue', ascending=False).head())
    
    # --- SECTION 5: DELIVERY & SATISFACTION ---
    print("\n--- 🚚 Delivery & Customer Satisfaction Analysis ---")
    satisfaction_by_delivery = sales_df.groupby('delivery_status')['review_score'].mean().reset_index()
    print("📊 Average Review Score by Delivery Status:")
    print(satisfaction_by_delivery)
    
    print("\n📊 Review Score Distribution:")
    review_dist = sales_df['review_score'].value_counts(normalize=True).sort_index() * 100
    print(review_dist.round(2).astype(str) + '%')

    print("\n📊 Top & Bottom Categories by Average Review Score:")
    category_reviews = sales_df.groupby('product_category_name')['review_score'].mean().dropna().sort_values()
    top_5_cats = category_reviews.nlargest(5)
    bottom_5_cats = category_reviews.nsmallest(5)
    print("  Top 5 Rated Categories:")
    print(top_5_cats)
    print("\n  Bottom 5 Rated Categories:")
    print(bottom_5_cats)

    # --- SECTION 6: BUSINESS INSIGHTS & LEADERBOARDS ---
    print("\n--- 📊 Business Insights & Leaderboards ---")
    
    print("📊 Top 10 Categories by Revenue:")
    category_sales = sales_df.groupby('product_category_name')['payment_value'].sum().nlargest(10)
    print(category_sales.round(2))

    print("\n📊 Top 10 Seller States by Revenue:")
    seller_state_sales = sales_df.groupby('seller_state')['payment_value'].sum().nlargest(10)
    print(seller_state_sales.round(2))
    
    print("\n📊 Order Status Distribution:")
    status_counts = raw_sales_df['order_status'].value_counts(normalize=True) * 100
    print(status_counts.round(2).astype(str) + '%')

    print("\n📊 Payment Method Popularity:")
    payment_dist = sales_df['payment_type'].value_counts(normalize=True) * 100
    print(payment_dist.round(2).astype(str) + '%')
    
    print("\n📊 Credit Card Installments Distribution:")
    cc_installments = sales_df[sales_df['payment_type'] == 'credit_card']['payment_installments'].value_counts(normalize=True).sort_index() * 100
    print(cc_installments.round(2).astype(str) + '%')

    print("\n[Step 5/6] All metrics calculated.")
    print("\n--- ✅ Analysis Complete ---")
    
if __name__ == '__main__':
    # Set the path to the folder containing your CSV files
    # Make sure this path is correct for your system
    csv_folder_path = 'data/extracted_data'
    calculate_all_metrics(data_path=csv_folder_path)

