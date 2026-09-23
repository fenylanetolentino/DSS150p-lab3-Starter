from pathlib import Path
import shutil
from src.config import path_for

def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory."""
    
    # 1. Get our base paths using the correct keys from settings.yml
    source_dir = path_for('source_dir')
    
    # 2. Create the run-specific raw folder (e.g., data/raw/run_id=test_123/)
    run_raw_dir = path_for('raw_dir') / f"run_id={run_id}"
    run_raw_dir.mkdir(parents=True, exist_ok=True)
    
    # 3. List the exact files the professor required us to copy
    files_to_copy = ["customers.csv", "products.json", "orders.csv"]
    
    # 4. Safely copy each file without altering the original
    for file_name in files_to_copy:
        src_file = source_dir / file_name
        dest_file = run_raw_dir / file_name
        
        # Verify the file actually exists before trying to copy it
        if src_file.exists():
            shutil.copy2(src_file, dest_file)
        else:
            raise FileNotFoundError(f"Source file missing: {src_file}")
            
    # 5. Return the new folder path as requested
    return run_raw_dir