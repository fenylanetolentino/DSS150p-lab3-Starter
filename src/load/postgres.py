import re
import pandas as pd
from sqlalchemy import create_engine, text
from src.config import DB


def get_engine():
    """Create a SQLAlchemy engine using the secure configuration."""
    url = f"postgresql://{DB['user']}:{DB['password']}@{DB['host']}:{DB['port']}/{DB['dbname']}"
    return create_engine(url)


def upsert_curated(df: pd.DataFrame, run_id: str) -> int:
    """
    Load curated.sales_order_lines using rerun-safe UPSERT semantics.
    Satisfies Goal 2, Task E requirements.
    """
    if df.empty:
        return 0

    engine = get_engine()
    # Strip everything except letters, digits, and underscores so the
    # resulting string is always a safe, unquoted Postgres identifier.
    safe_run_id = re.sub(r'[^0-9a-zA-Z_]', '_', run_id).lower()
    temp_table = f"temp_load_{safe_run_id}"

    with engine.begin() as conn:
        # Clean up any leftover temp table from a previous failed run
        conn.execute(text(f"DROP TABLE IF EXISTS curated.{temp_table};"))

        # NEW: Dynamically fetch allowed columns from the database schema
        valid_cols_query = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'curated' AND table_name = 'sales_order_lines'
        """)
        valid_columns = [row[0] for row in conn.execute(valid_cols_query)]

        # Filter the DataFrame to only include columns that actually exist in the DB
        cols_to_keep = [c for c in df.columns if c in valid_columns]
        df_filtered = df[cols_to_keep]

        # 1. Load filtered data to a temporary staging table
        df_filtered.to_sql(temp_table, con=conn, schema='curated', if_exists='replace', index=False)

        # 2. Build the UPSERT query dynamically
        columns = df_filtered.columns.tolist()
        cols_csv = ", ".join(columns)

        set_cols = [f"{col} = EXCLUDED.{col}" for col in columns if col != 'order_id']
        set_clause = ",\n            ".join(set_cols)

        upsert_sql = text(f"""
            INSERT INTO curated.sales_order_lines ({cols_csv})
            SELECT {cols_csv} FROM curated.{temp_table}
            ON CONFLICT (order_id) DO UPDATE SET
                {set_clause}
            WHERE curated.sales_order_lines.record_hash IS DISTINCT FROM EXCLUDED.record_hash;
        """)

        # 3. Execute the UPSERT and clean up the temporary table
        result = conn.execute(upsert_sql)
        conn.execute(text(f"DROP TABLE curated.{temp_table};"))

        return result.rowcount


def load_partition(df: pd.DataFrame, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    if df.empty:
        return 0

    # 1. Use our existing UPSERT logic so the partition load remains rerun-safe and deduplicated
    rows_affected = upsert_curated(df, run_id)

    # 2. Format the partition key (e.g., '2026-01') to match the professor's schema
    partition_key = f"{year}-{month:02d}"
    loaded_at = pd.Timestamp.utcnow()

    # 3. Record this action in the audit table using partition_key and row_count
    engine = get_engine()
    with engine.begin() as conn:
        audit_sql = text("""
            INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id)
            VALUES (:partition_key, :loaded_at, :row_count, :run_id)
            ON CONFLICT (partition_key) DO UPDATE SET
                loaded_at_utc = EXCLUDED.loaded_at_utc,
                row_count = EXCLUDED.row_count,
                pipeline_run_id = EXCLUDED.pipeline_run_id;
        """)
        conn.execute(audit_sql, {
            "partition_key": partition_key,
            "loaded_at": loaded_at,
            "row_count": len(df),
            "run_id": run_id
        })

    return rows_affected