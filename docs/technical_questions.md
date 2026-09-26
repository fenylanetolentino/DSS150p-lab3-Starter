1. Why is record_hash useful for rerun-safe loading, and which columns should not be included in it?

record_hash gives the UPSERT a way to tell "did this row's actual business content change?" without comparing every column one by one. In build_curated, the hash is computed from business_cols = ['order_id', 'customer_id', 'product_id', 'quantity', 'status', 'gross_amount', 'discount_amount', 'net_amount'], and the load UPSERT only overwrites a row WHERE record_hash IS DISTINCT FROM EXCLUDED.record_hash. This is what made rerunning load twice upsert 0 rows the second time.

Columns that must be excluded from the hash: pipeline_run_id, processed_at_utc, staged_at_utc, and any other pipeline/audit timestamp. If these were included, the hash would change every single run purely because the pipeline executed at a different time — even if the underlying order data never changed — which would defeat the whole point of rerun-safety and cause every UPSERT to rewrite every row unnecessarily.

2. Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?

Raw data is the only layer that's an exact, untouched copy of what the source system actually sent. If a bug is later found in the staging or curated transformation logic (like the partition duplicate-append bug I found and fixed), the raw snapshots let you re-run the fix against the original input and get a corrected result — without raw data, you'd have no way to know what the source truly looked like at that point in time, and any bug in a transformation would be permanent and undiagnosable. It's also required for audit/compliance: extract_sources(run_id) copies files into a run-specific folder precisely so each run's raw input is independently reproducible.

3. What is the difference between a data-quality rejection and a system exception?

A data-quality rejection is an expected, handled outcome — the row exists, it's readable, but it fails a business rule (e.g., a negative unit_price, an invalid status, or an orphan customer_id/product_id reference). These get routed to quarantine with a reason, and the pipeline continues normally. A system exception is an unexpected failure that prevents the pipeline from doing its job at all — a missing file, a broken database connection, a bug in the code. These are caught by execute_stage(), logged with full context, and re-raised as a RuntimeError that stops the pipeline, because there's no safe way to "quarantine" a stage that can't run.

4. Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?

Parquet stores data column-by-column instead of row-by-row, and encodes/compresses each column independently. In my benchmark, this meant a query like net_amount > 100 only had to read the net_amount and order_id columns from disk, while CSV had to parse the entire row — every column — for every line just to check one condition. That's a big part of why Parquet's filtered read (0.0154s) beat CSV's (0.3908s) so heavily, on top of Parquet's file size being roughly a third of CSV's.

5. Why is a DAG that contains all transformation logic directly considered harder to maintain?

If the transformation logic lived inside the DAG file itself, you'd have two copies of your business logic to keep in sync — one for local/CLI use and one embedded in Airflow — and any bug fix (like my partition dedup fix) would need to be applied and tested in two places. It also means you can't unit test the logic outside of an Airflow environment, and every DAG parse would re-execute import-time logic, slowing down the scheduler. My DAG avoids this entirely — every task is a BashOperator calling python -m src.cli <command>, so the DAG only owns scheduling, retries, and parameters, while src/ owns the actual logic.

6. How do retries interact with idempotency? Give an example where retries without idempotency cause damage.

Retries assume that running the same task again is safe — that's only true if the task is idempotent. In my Goal 4 Task E test, extract failed 3 times automatically (due to a missing source file) and was later retried manually several more times; because extract only reads from source and writes into a fresh run_id-scoped folder, none of those retries caused any damage. But if load were not idempotent — for example, if it did a plain INSERT instead of an UPSERT ... ON CONFLICT, and a task failed after inserting rows but before Airflow could mark it complete — the automatic retry would re-insert the same rows a second time, creating duplicate order_ids in curated.sales_order_lines. My design avoids this because the UPSERT with record_hash comparison makes a retry a no-op if the data already made it in.

7. What trade-off is introduced by partitioning too aggressively?

Over-partitioning (too many partition keys, or a key with very high cardinality) creates the "small files problem": instead of a few large, efficient Parquet files, you get thousands of tiny ones. I actually hit a version of this bug myself — before the fix, each partition folder accumulated up to 10 duplicate part-files from repeated runs, and even at that modest scale it was clearly wasteful. At real scale, over-partitioning causes slow directory listings, heavy metadata overhead for the query engine, and can make reads slower than a single well-organized file, even though partitioning is supposed to speed up filtered queries.

8. How would you adapt the pipeline if the source became an API or database instead of local files?

Only src/extract/ would need to change — extract_sources(run_id) currently does shutil.copy2() from local files, but the interface (take a run_id, return a path to a run-specific raw snapshot) wouldn't need to change. I'd replace the file-copy logic with an API client or a database query that pulls records for that run and writes them out as the same raw snapshot format (e.g., calling the API and dumping the JSON response into data/raw/run_id=.../orders.json), so transform, load, validate, and the Airflow DAG would all keep working unchanged. I'd also need to add pagination/rate-limit handling and probably an incremental "since last run" parameter to avoid re-pulling the entire dataset every time, but the raw→staging→curated→quarantine structure and the CLI/DAG contract stay exactly the same.