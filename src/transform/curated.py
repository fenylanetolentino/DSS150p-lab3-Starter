import pandas as pd
import hashlib
from pathlib import Path
from src.config import path_for

def build_curated(staging_dfs: dict, run_id: str):
    """Join staging orders/customers/products and create analysis-ready sales rows."""
    
    # 1. Setup paths
    curated_dir = path_for('curated_dir') / f"run_id={run_id}"
    quarantine_dir = path_for('quarantine_dir') / f"run_id={run_id}"
    curated_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    
    processed_at = pd.Timestamp.utcnow()
    quarantine_records = []

    customers = staging_dfs['customers']
    products = staging_dfs['products']
    orders = staging_dfs['orders']

    # --- 2. ORPHAN HANDLING & JOINS ---
    # Merge customers and isolate orphans
    merged = orders.merge(customers[['customer_id', 'email']], on='customer_id', how='left', indicator='_merge_cust')
    orphan_cust = merged[merged['_merge_cust'] == 'left_only']
    for _, row in orphan_cust.iterrows():
        quarantine_records.append({'dataset': 'curated', 'record_id': str(row['order_id']), 'reason': 'Orphan customer reference'})
        
    merged = merged[merged['_merge_cust'] == 'both'].drop(columns=['_merge_cust'])
    
    # Merge products and isolate orphans. We ONLY pull product_id to avoid unit_price collisions!
    merged = merged.merge(products[['product_id']], on='product_id', how='left', indicator='_merge_prod')
    orphan_prod = merged[merged['_merge_prod'] == 'left_only']
    for _, row in orphan_prod.iterrows():
        quarantine_records.append({'dataset': 'curated', 'record_id': str(row['order_id']), 'reason': 'Orphan product reference'})
        
    # Valid curated records
    curated = merged[merged['_merge_prod'] == 'both'].copy().drop(columns=['_merge_prod'])

    # --- 3. CALCULATIONS ---
    # Safely convert discount_pct to numeric, default to 0 if missing
    curated['discount_pct'] = pd.to_numeric(curated['discount_pct'], errors='coerce').fillna(0.0)
        
    curated['gross_amount'] = curated['quantity'] * curated['unit_price']
    curated['discount_amount'] = curated['gross_amount'] * curated['discount_pct']
    curated['net_amount'] = curated['gross_amount'] - curated['discount_amount']

    # --- 4. AUDIT COLUMNS ---
    curated['source_updated_at'] = curated['updated_at'] 
    curated['pipeline_run_id'] = run_id
    curated['processed_at_utc'] = processed_at

    # --- 5. DETERMINISTIC RECORD HASH ---
    # We strictly exclude pipeline timestamps to ensure the hash is deterministic based purely on business content.
    business_cols = ['order_id', 'customer_id', 'product_id', 'quantity', 'status', 'gross_amount', 'discount_amount', 'net_amount']
    
    def compute_hash(row):
        val_str = "|".join([str(row[col]) for col in business_cols])
        return hashlib.sha256(val_str.encode('utf-8')).hexdigest()
        
    if not curated.empty:
        curated['record_hash'] = curated.apply(compute_hash, axis=1)
    else:
        curated['record_hash'] = pd.Series(dtype='str')
        
    # --- 6. OUTPUT ---
    curated.to_parquet(curated_dir / 'sales_order_lines.parquet', index=False)
    
    quar_df = pd.DataFrame(quarantine_records) if quarantine_records else pd.DataFrame(columns=['dataset', 'record_id', 'reason'])
    if not quar_df.empty:
        quar_df.to_parquet(quarantine_dir / 'curated_quarantine.parquet', index=False)

    return curated, quar_df
