"""ETL script to load NYC 311 CSV into DuckDB."""
import sys
import os
import duckdb
from pathlib import Path
from .config import settings

# Fix Windows console encoding for Unicode characters
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')


def create_directories():
    """Ensure data directory exists."""
    os.makedirs(os.path.dirname(settings.db_path), exist_ok=True)


def load_and_transform_csv(csv_path: str):
    """
    Load CSV file and create cleaned table in DuckDB.
    
    Args:
        csv_path: Path to the NYC 311 CSV file
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    create_directories()
    
    # Connect to DuckDB
    conn = duckdb.connect(settings.db_path, read_only=False)
    
    try:
        # Drop existing tables if they exist
        conn.execute(f"DROP TABLE IF EXISTS raw_nyc_311")
        conn.execute(f"DROP TABLE IF EXISTS {settings.table_name}")
        
        # Load CSV into raw table
        print(f"Loading CSV from {csv_path}...", flush=True)
        print("   This may take several minutes for large files...", flush=True)
        import time
        start_time = time.time()
        
        try:
            # Check file size first
            file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            print(f"   File size: {file_size_mb:.1f} MB", flush=True)
            
            print("   Executing CSV load query...", flush=True)
            conn.execute(f"""
                CREATE TABLE raw_nyc_311 AS
                SELECT * FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
            """)
            elapsed = time.time() - start_time
            print(f"   [OK] CSV loaded in {elapsed:.1f} seconds", flush=True)
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"   [ERROR] CSV loading failed after {elapsed:.1f} seconds: {str(e)}", flush=True)
            raise
        
        # Get column names - read directly from CSV header (fastest and most reliable)
        print("   Getting column information...", flush=True)
        try:
            # Read CSV header directly - this is the fastest method
            print("   Reading column names from CSV header...", flush=True)
            import csv
            with open(csv_path, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f)
                raw_columns = next(reader)  # Get header row
            print(f"   [OK] Read {len(raw_columns)} columns from CSV header", flush=True)
        except Exception as e1:
            print(f"   Warning: CSV header read failed ({e1}), trying database query...", flush=True)
            try:
                # Fallback: Query database with a simple approach
                print("   Querying database for column names...", flush=True)
                # Use a very simple query that should work
                result = conn.execute("SELECT * FROM raw_nyc_311 LIMIT 1").fetchone()
                if result:
                    # Get column names from the result description
                    raw_columns = [desc[0] for desc in conn.description] if hasattr(conn, 'description') else []
                    if not raw_columns:
                        # Last resort: use PRAGMA
                        pragma_result = conn.execute("PRAGMA table_info('raw_nyc_311')").fetchall()
                        raw_columns = [row[1] for row in pragma_result] if pragma_result else []
                print(f"   [OK] Got columns from database query", flush=True)
            except Exception as e2:
                print(f"   [ERROR] All methods failed: {e2}", flush=True)
                raise
        
        print(f"   Found {len(raw_columns)} columns in CSV", flush=True)
        print(f"   Column names: {', '.join(raw_columns[:10])}{'...' if len(raw_columns) > 10 else ''}", flush=True)
        
        # Map common NYC 311 column names (adjust based on actual CSV headers)
        print("   Mapping column names...", flush=True)
        # Common column names in NYC 311 dataset:
        # - Created Date, Closed Date
        # - Complaint Type, Descriptor
        # - Incident Zip, Borough
        # - Latitude, Longitude
        # etc.
        
        # Build column mapping with safe casting
        column_mappings = []
        
        # Check which columns exist and map them appropriately
        created_date_col = None
        closed_date_col = None
        incident_zip_col = None
        latitude_col = None
        longitude_col = None
        
        print("   Searching for date and location columns...", flush=True)
        for col in raw_columns:
            col_lower = col.lower().replace(' ', '_').replace('-', '_')
            
            # Find date columns
            if 'created_date' in col_lower or (created_date_col is None and 'created' in col_lower and 'date' in col_lower):
                created_date_col = col
            if 'closed_date' in col_lower or (closed_date_col is None and 'closed' in col_lower and 'date' in col_lower):
                closed_date_col = col
                
            # Find zip code column
            if 'incident_zip' in col_lower or (incident_zip_col is None and 'zip' in col_lower):
                incident_zip_col = col
                
            # Find location columns
            if 'latitude' in col_lower:
                latitude_col = col
            if 'longitude' in col_lower:
                longitude_col = col
        
        print(f"   Found columns: Created Date={created_date_col}, Closed Date={closed_date_col}, Zip={incident_zip_col}, Lat={latitude_col}, Lon={longitude_col}", flush=True)
        
        # Build SELECT statement with transformations
        print("   Building SELECT statement...", flush=True)
        select_parts = []
        
        # Add all original columns first
        for col in raw_columns:
            select_parts.append(f'"{col}"')
        
        # Add transformed columns
        if created_date_col:
            select_parts.append(f'strptime("{created_date_col}", \'%m/%d/%Y %I:%M:%S %p\') AS created_ts')
        
        if closed_date_col:
            select_parts.append(f'strptime("{closed_date_col}", \'%m/%d/%Y %I:%M:%S %p\') AS closed_ts')
        
        if created_date_col and closed_date_col:
            select_parts.append(f"""
                CASE
                    WHEN strptime("{created_date_col}", '%m/%d/%Y %I:%M:%S %p') IS NOT NULL 
                         AND strptime("{closed_date_col}", '%m/%d/%Y %I:%M:%S %p') IS NOT NULL
                    THEN DATEDIFF('day', 
                                  strptime("{created_date_col}", '%m/%d/%Y %I:%M:%S %p'),
                                  strptime("{closed_date_col}", '%m/%d/%Y %I:%M:%S %p'))
                    ELSE NULL
                END AS time_to_close_days
            """)
        
        if latitude_col and longitude_col:
            select_parts.append(f"""
                ({latitude_col} IS NOT NULL AND {longitude_col} IS NOT NULL
                 AND TRY_CAST({latitude_col} AS DOUBLE) <> 0 
                 AND TRY_CAST({longitude_col} AS DOUBLE) <> 0) AS geocoded
            """)
        elif latitude_col or longitude_col:
            # If only one exists, still create geocoded column
            lat_col = latitude_col or 'NULL'
            lon_col = longitude_col or 'NULL'
            select_parts.append(f"""
                ({lat_col} IS NOT NULL AND {lon_col} IS NOT NULL
                 AND TRY_CAST({lat_col} AS DOUBLE) <> 0 
                 AND TRY_CAST({lon_col} AS DOUBLE) <> 0) AS geocoded
            """)
        
        if incident_zip_col:
            select_parts.append(f"""
                LPAD(CAST("{incident_zip_col}" AS VARCHAR), 5, '0') AS zip_code
            """)
        
        # Create cleaned table
        print(f"Creating cleaned table with transformations...", flush=True)
        print(f"   SELECT statement has {len(select_parts)} columns", flush=True)
        
        if len(select_parts) == 0:
            raise ValueError("No columns to select! Check column mapping logic.")
        
        create_sql = f"""
            CREATE TABLE {settings.table_name} AS
            SELECT {', '.join(select_parts)}
            FROM raw_nyc_311
        """
        
        print("   Executing CREATE TABLE query (this may take a while for large datasets)...", flush=True)
        start_time = time.time()
        try:
            conn.execute(create_sql)
            elapsed = time.time() - start_time
            print(f"   [OK] Cleaned table created in {elapsed:.1f} seconds", flush=True)
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"   [ERROR] Failed to create cleaned table after {elapsed:.1f} seconds: {str(e)}", flush=True)
            print(f"   SQL (first 500 chars): {create_sql[:500]}...", flush=True)
            raise
        
        # Get row count
        print("   Counting rows...", flush=True)
        row_count = conn.execute(f"SELECT COUNT(*) FROM {settings.table_name}").fetchone()[0]
        print(f"[SUCCESS] Successfully loaded {row_count:,} rows into {settings.table_name}", flush=True)
        
        # Show sample of columns
        sample_cols = conn.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{settings.table_name}'
            LIMIT 10
        """).df()
        print("\nSample columns:")
        print(sample_cols.to_string())
        
        # Validate time_to_close_days calculation
        if created_date_col and closed_date_col:
            print("\n   Validating time_to_close_days calculation...", flush=True)
            try:
                # Check how many rows have non-NULL time_to_close_days
                validation_query = f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(time_to_close_days) as rows_with_time_to_close,
                        COUNT(CASE WHEN time_to_close_days <= 3 THEN 1 END) as closed_within_3_days,
                        MIN(time_to_close_days) as min_days,
                        MAX(time_to_close_days) as max_days,
                        AVG(time_to_close_days) as avg_days
                    FROM {settings.table_name}
                """
                validation_result = conn.execute(validation_query).fetchone()
                if validation_result:
                    total, with_time, within_3, min_days, max_days, avg_days = validation_result
                    print(f"   Total rows: {total:,}", flush=True)
                    print(f"   Rows with time_to_close_days: {with_time:,} ({with_time/total*100:.1f}%)", flush=True)
                    print(f"   Closed within 3 days: {within_3:,}", flush=True)
                    if min_days is not None:
                        print(f"   Min days: {min_days}, Max days: {max_days}, Avg days: {avg_days:.1f}", flush=True)
                    else:
                        print(f"   ⚠️  WARNING: All time_to_close_days values are NULL!", flush=True)
                        print(f"   This may indicate date parsing issues. Check date column formats.", flush=True)
                
                # Show sample rows with dates
                sample_query = f"""
                    SELECT 
                        "{created_date_col}" as created_date_raw,
                        created_ts,
                        "{closed_date_col}" as closed_date_raw,
                        closed_ts,
                        time_to_close_days
                    FROM {settings.table_name}
                    WHERE time_to_close_days IS NOT NULL
                    LIMIT 5
                """
                sample_rows = conn.execute(sample_query).fetchall()
                if sample_rows:
                    print(f"\n   Sample rows with valid time_to_close_days:", flush=True)
                    for i, row in enumerate(sample_rows[:3], 1):
                        print(f"   {i}. Created: {row[0]} -> {row[1]}, Closed: {row[2]} -> {row[3]}, Days: {row[4]}", flush=True)
                else:
                    print(f"\n   ⚠️  No rows with valid time_to_close_days found!", flush=True)
                    # Check if dates are parsing at all
                    check_dates_query = f"""
                        SELECT 
                            COUNT(*) as total,
                            COUNT(created_ts) as has_created_ts,
                            COUNT(closed_ts) as has_closed_ts
                        FROM {settings.table_name}
                    """
                    date_check = conn.execute(check_dates_query).fetchone()
                    if date_check:
                        total, has_created, has_closed = date_check
                        print(f"   Date parsing check:", flush=True)
                        print(f"   - Rows with created_ts: {has_created:,} ({has_created/total*100:.1f}%)", flush=True)
                        print(f"   - Rows with closed_ts: {has_closed:,} ({has_closed/total*100:.1f}%)", flush=True)
            except Exception as e:
                print(f"   ⚠️  Validation check failed: {e}", flush=True)
        
        # Clear schema cache so it will be refreshed on next access
        from .schema_cache import clear_schema_cache
        clear_schema_cache()
        
    finally:
        conn.close()


def main():
    """CLI entry point for ETL script."""
    print("ETL script started...", flush=True)
    print(f"Arguments: {sys.argv}", flush=True)
    
    if len(sys.argv) < 2:
        print("Usage: python -m backend.etl <path_to_nyc_311.csv>", flush=True)
        sys.exit(1)
    
    csv_path = sys.argv[1]
    print(f"Processing CSV: {csv_path}", flush=True)
    
    try:
        load_and_transform_csv(csv_path)
        print("\n[SUCCESS] ETL completed successfully!", flush=True)
        print("You can now start the server and ask questions.", flush=True)
    except Exception as e:
        print(f"\n[ERROR] ETL failed: {str(e)}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

