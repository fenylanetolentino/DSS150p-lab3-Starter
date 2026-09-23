import os
import time
import statistics
import pandas as pd
from sqlalchemy import text
from src.load.postgres import get_engine

def measure_read_median(func, repeats):
    """Executes a function multiple times and returns the median execution time."""
    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        func()
        times.append(time.perf_counter() - start)
    return statistics.median(times)

def run_benchmark(curated_path, output_dir, repeats: int = 5):
    """Compare the same logical dataset in CSV, JSON Lines, Parquet, and PostgreSQL."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Load the baseline curated dataset
    df = pd.read_parquet(curated_path)
    row_count = len(df)
    
    csv_path = os.path.join(output_dir, "benchmark.csv")
    json_path = os.path.join(output_dir, "benchmark.jsonl")
    parquet_path = os.path.join(output_dir, "benchmark.parquet")
    
    benchmark_data = []

    # 1. CSV Formating
    start_write = time.perf_counter()
    df.to_csv(csv_path, index=False)
    csv_write = time.perf_counter() - start_write
    
    csv_read_full = measure_read_median(lambda: pd.read_csv(csv_path), repeats)
    # CORRECTED FILTER: status=DELIVERED
    csv_read_filtered = measure_read_median(
        lambda: pd.read_csv(csv_path, usecols=['order_id', 'status']).query("status == 'DELIVERED'"), 
        repeats
    )
    
    benchmark_data.append({
        "format": "CSV",
        "size_bytes": os.path.getsize(csv_path),
        "write_time_sec": round(csv_write, 4),
        "read_full_time_sec": round(csv_read_full, 4),
        "read_filtered_time_sec": round(csv_read_filtered, 4),
        "row_count": row_count
    })

    # 2. JSON Lines Formatting
    start_write = time.perf_counter()
    df.to_json(json_path, orient="records", lines=True)
    json_write = time.perf_counter() - start_write
    
    json_read_full = measure_read_median(lambda: pd.read_json(json_path, orient="records", lines=True), repeats)
    # CORRECTED FILTER: status=DELIVERED
    json_read_filtered = measure_read_median(
        lambda: pd.read_json(json_path, orient="records", lines=True).query("status == 'DELIVERED'"), 
        repeats
    )

    benchmark_data.append({
        "format": "JSON Lines",
        "size_bytes": os.path.getsize(json_path),
        "write_time_sec": round(json_write, 4),
        "read_full_time_sec": round(json_read_full, 4),
        "read_filtered_time_sec": round(json_read_filtered, 4),
        "row_count": row_count
    })

    # 3. Parquet (Snappy Compression)
    start_write = time.perf_counter()
    df.to_parquet(parquet_path, compression="snappy", index=False)
    parquet_write = time.perf_counter() - start_write
    
    parquet_read_full = measure_read_median(lambda: pd.read_parquet(parquet_path), repeats)
    # CORRECTED FILTER: status=DELIVERED
    parquet_read_filtered = measure_read_median(
        lambda: pd.read_parquet(parquet_path, columns=['order_id', 'status'], filters=[('status', '=', 'DELIVERED')]), 
        repeats
    )

    benchmark_data.append({
        "format": "Parquet (Snappy)",
        "size_bytes": os.path.getsize(parquet_path),
        "write_time_sec": round(parquet_write, 4),
        "read_full_time_sec": round(parquet_read_full, 4),
        "read_filtered_time_sec": round(parquet_read_filtered, 4),
        "row_count": row_count
    })

    # 4. PostgreSQL (Server Storage & Index Evaluation)
    engine = get_engine()
    with engine.connect() as conn:
        size_query = text("SELECT pg_total_relation_size('curated.sales_order_lines');")
        pg_size = conn.execute(size_query).scalar()
        
        pg_read_full = measure_read_median(
            lambda: pd.read_sql_table("sales_order_lines", con=engine, schema="curated"), 
            repeats
        )
        # CORRECTED FILTER: status=DELIVERED
        pg_read_filtered = measure_read_median(
            lambda: pd.read_sql_query("SELECT order_id, status FROM curated.sales_order_lines WHERE status = 'DELIVERED'", con=engine), 
            repeats
        )

    benchmark_data.append({
        "format": "PostgreSQL",
        "size_bytes": pg_size,
        "write_time_sec": None,
        "read_full_time_sec": round(pg_read_full, 4),
        "read_filtered_time_sec": round(pg_read_filtered, 4),
        "row_count": row_count
    })

    # Output generation
    results_df = pd.DataFrame(benchmark_data)
    results_path = os.path.join(output_dir, "benchmark_results.csv")
    results_df.to_csv(results_path, index=False)
    return results_df
