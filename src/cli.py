import argparse
import logging
from src.config import PROJECT_ROOT, DB, SETTINGS
from src.common.audit import new_run_id

# Configure logging for the entire pipeline
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s'
)
logger = logging.getLogger('dss150p_pipeline')

def execute_stage(stage_name, func, *args, **kwargs):
    """
    Executes a pipeline stage with strict error handling and context preservation.
    Satisfies Goal 2, Task D requirements.
    """
    logger.info(f"Starting stage: {stage_name}")
    try:
        result = func(*args, **kwargs)
        logger.info(f"Successfully completed stage: {stage_name}")
        return result
    except Exception as e:
        # Log the error with exc_info=True to preserve the full stack trace context
        logger.error(f"FATAL ERROR in pipeline stage '{stage_name}': {e}", exc_info=True)
        # Re-raise to ensure the process exits with a failure status (no silent failures)
        raise RuntimeError(f"Pipeline failed during {stage_name}") from e

def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args()

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        return

    # Generate a single, consistent run ID for this execution
    run_id = new_run_id()

    # Wire up Extract
    if args.command in ('extract', 'run-all'):
        from src.extract.files import extract_sources
        execute_stage('Extract', extract_sources, run_id)

    # Wire up Transform
    if args.command in ('transform', 'run-all'):
        from src.transform.staging import build_staging
        from src.transform.curated import build_curated
        from src.config import path_for
        
        def run_transform():
            # Find the raw data folder for this specific run ID
            raw_dir = path_for('raw_dir') / f"run_id={run_id}"
            stg_dfs, _ = build_staging(raw_dir, run_id)
            build_curated(stg_dfs, run_id)
            
        execute_stage('Transform', run_transform)

    # Wire up Load
    if args.command in ('load', 'run-all'):
        from src.load.postgres import upsert_curated
        from src.config import path_for
        import pandas as pd
        import os
        
        def run_load():
            curated_base = path_for('curated_dir')
            # Find all folders starting with run_id=
            run_dirs = [d for d in curated_base.iterdir() if d.is_dir() and d.name.startswith('run_id=')]
            if not run_dirs:
                raise FileNotFoundError("No curated data found to load. Run 'transform' first.")
            
            # Get the most recently modified folder
            latest_run_dir = max(run_dirs, key=os.path.getmtime)
            parquet_file = latest_run_dir / 'sales_order_lines.parquet'
            
            logger.info(f"Loading data from {parquet_file}")
            df = pd.read_parquet(parquet_file)
            
            # Extract the run_id from the folder name
            extracted_run_id = latest_run_dir.name.split('=')[1]
            rows_affected = upsert_curated(df, extracted_run_id)
            logger.info(f"Successfully UPSERTED {rows_affected} rows into PostgreSQL.")
            
        execute_stage('Load', run_load)

    # Wire up Benchmark
    if args.command == 'benchmark':
        from src.benchmark.storage import run_benchmark
        from src.config import path_for
        import os

        def run_bench():
            curated_base = path_for('curated_dir')
            run_dirs = [d for d in curated_base.iterdir() if d.is_dir() and d.name.startswith('run_id=')]
            if not run_dirs:
                raise FileNotFoundError("No curated data found to benchmark. Run 'transform' first.")

            latest_run_dir = max(run_dirs, key=os.path.getmtime)
            parquet_file = latest_run_dir / 'sales_order_lines.parquet'
            benchmark_dir = PROJECT_ROOT / 'data' / 'benchmarks'

            logger.info(f"Running storage benchmark on {parquet_file}")
            results = run_benchmark(str(parquet_file), str(benchmark_dir), args.repeats)

            print("\n" + "="*60)
            print("BENCHMARK RESULTS")
            print("="*60)
            print(results.to_string(index=False))
            print("="*60 + "\n")

        execute_stage('Benchmark', run_bench)

   # Wire up Load Partition
    if args.command == 'load-partition':
        from src.load.postgres import load_partition
        from src.config import PROJECT_ROOT
        import pandas as pd
        
        def run_load_partition():
            partition_path = PROJECT_ROOT / 'data' / 'partitioned' / f'order_year={args.year}' / f'order_month={args.month}'
            if not partition_path.exists():
                raise FileNotFoundError(f"Partition not found: {partition_path}")
            
            logger.info(f"Loading partition {args.year}-{args.month} from {partition_path}")
            df = pd.read_parquet(partition_path)
            
            rows = load_partition(df, args.year, args.month, run_id)
            logger.info(f"Successfully loaded partition into PostgreSQL. Upserted {rows} rows.")
            
        execute_stage('Load Partition', run_load_partition)

    # Leave the others as NotImplemented for now
    if args.command == 'validate':
        raise NotImplementedError(f"Command '{args.command}' is not yet wired up.")

if __name__ == '__main__':
    main()