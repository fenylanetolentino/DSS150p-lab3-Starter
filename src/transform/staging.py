from pathlib import Path
import pandas as pd
import json
from src.config import SETTINGS, path_for

def build_staging(raw_dir: Path, run_id: str):
    """Create cleaned, typed staging datasets and write to Parquet."""
    
    # 1. Setup our dynamic paths
    staging_dir = path_for('staging_dir') / f"run_id={run_id}"
    quarantine_dir = path_for('quarantine_dir') / f"run_id={run_id}"
    staging_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    
    staged_at = pd.Timestamp.utcnow()
    quarantine_records = []

    # --- CUSTOMERS ---
    cust_df = pd.read_csv(raw_dir / 'customers.csv')
    cust_df['updated_at'] = pd.to_datetime(cust_df['updated_at'], utc=True)
    cust_df = cust_df.sort_values('updated_at').drop_duplicates(subset=['customer_id'], keep='last')
    
    # Clean text (missing values are naturally retained as NaN)
    cust_df['email'] = cust_df['email'].str.strip().str.lower()
    cust_df['city'] = cust_df['city'].str.strip().str.title()
    
    cust_df['pipeline_run_id'] = run_id
    cust_df['staged_at_utc'] = staged_at
    cust_df.to_parquet(staging_dir / 'customers.parquet', index=False)

    # --- PRODUCTS ---
    # Safely load JSON (handles both standard arrays and JSON lines)
    with open(raw_dir / 'products.json', 'r') as f:
        content = f.read().strip()
        prod_data = json.loads(content) if content.startswith('[') else [json.loads(line) for line in content.split('\n') if line]
        
    prod_df = pd.json_normalize(prod_data)
    if 'category.name' in prod_df.columns:
        prod_df = prod_df.rename(columns={'category.name': 'category_name', 'category.department': 'category_department'})
        
    prod_df['updated_at'] = pd.to_datetime(prod_df['updated_at'], utc=True)
    prod_df = prod_df.sort_values('updated_at').drop_duplicates(subset=['product_id'], keep='last')
    prod_df['unit_price'] = pd.to_numeric(prod_df['unit_price'], errors='coerce')
    
    # Quarantine invalid prices
    prod_valid = prod_df[prod_df['unit_price'] >= 0].copy()
    prod_invalid = prod_df[(prod_df['unit_price'] < 0) | (prod_df['unit_price'].isna())].copy()
    for _, row in prod_invalid.iterrows():
        quarantine_records.append({'dataset': 'products', 'record_id': str(row['product_id']), 'reason': 'Invalid unit_price'})
        
    prod_valid['pipeline_run_id'] = run_id
    prod_valid['staged_at_utc'] = staged_at
    prod_valid.to_parquet(staging_dir / 'products.parquet', index=False)

    # --- ORDERS ---
    ord_df = pd.read_csv(raw_dir / 'orders.csv')
    ord_df['order_timestamp'] = pd.to_datetime(ord_df['order_timestamp'], utc=True)
    ord_df['updated_at'] = pd.to_datetime(ord_df['updated_at'], utc=True)
    ord_df = ord_df.sort_values('updated_at').drop_duplicates(subset=['order_id'], keep='last')
    ord_df['quantity'] = pd.to_numeric(ord_df['quantity'], errors='coerce')
    
    # Read rules from configuration
    allowed = SETTINGS['quality']['allowed_order_statuses']
    min_q = SETTINGS['quality']['min_quantity']
    max_q = SETTINGS['quality']['max_quantity']
    
    q_valid = ord_df['quantity'].between(min_q, max_q)
    status_valid = ord_df['status'].isin(allowed)
    
    # Quarantine invalid orders
    ord_valid = ord_df[q_valid & status_valid].copy()
    ord_invalid = ord_df[~(q_valid & status_valid)].copy()
    for _, row in ord_invalid.iterrows():
        quarantine_records.append({'dataset': 'orders', 'record_id': str(row['order_id']), 'reason': 'Invalid quantity or status'})
        
    ord_valid['pipeline_run_id'] = run_id
    ord_valid['staged_at_utc'] = staged_at
    ord_valid.to_parquet(staging_dir / 'orders.parquet', index=False)

    # --- QUARANTINE OUTPUT ---
    quar_df = pd.DataFrame(quarantine_records) if quarantine_records else pd.DataFrame(columns=['dataset', 'record_id', 'reason'])
    if not quar_df.empty:
        quar_df.to_parquet(quarantine_dir / 'staging_quarantine.parquet', index=False)

    return {
        'customers': cust_df,
        'products': prod_valid,
        'orders': ord_valid
    }, quar_df