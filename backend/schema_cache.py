"""Schema introspection and caching for the NYC 311 table."""
import logging
from typing import Dict, Any, List
import duckdb
from .config import settings
from .db import get_conn

logger = logging.getLogger(__name__)


# In-memory schema cache
_schema_cache: Dict[str, Any] = None


def _introspect_schema() -> Dict[str, Any]:
    """
    Introspect the schema of the NYC 311 table.
    
    Returns:
        Dictionary containing table schema information
    """
    logger.info("🔍 [SCHEMA] Starting schema introspection...")
    logger.info(f"   Database path: {settings.db_path}")
    logger.info(f"   Table name: {settings.table_name}")
    
    conn = get_conn()
    try:
        # Use DuckDB's table function to get column info directly
        logger.info("   Getting column structure from table...")
        try:
            # Method 1: Use DuckDB's table function (fastest)
            logger.info("   Querying table metadata...")
            # Get column info using a simple metadata query
            try:
                # Try using PRAGMA table_info which is usually fast
                pragma_result = conn.execute(f"PRAGMA table_info('{settings.table_name}')").fetchall()
                if pragma_result:
                    column_names = [row[1] for row in pragma_result]  # Column name is in position 1
                    column_types = [row[2] for row in pragma_result]  # Type is in position 2
                    logger.info(f"   Got {len(column_names)} columns from PRAGMA")
                    
                    import pandas as pd
                    columns_info = pd.DataFrame({
                        'column_name': column_names,
                        'data_type': column_types
                    })
                    logger.info(f"   Successfully got column information")
                else:
                    raise ValueError("PRAGMA returned no results")
            except Exception as pragma_error:
                logger.warning(f"   PRAGMA failed ({pragma_error}), trying alternative method...")
                # Fallback: Use a very simple query with explicit column selection
                # Query just the first column to get structure info
                try:
                    first_col_result = conn.execute(f"SELECT * FROM {settings.table_name} LIMIT 0").df()
                    column_names = list(first_col_result.columns)
                    logger.info(f"   Got {len(column_names)} columns from LIMIT 0 query")
                    
                    import pandas as pd
                    columns_info = pd.DataFrame({
                        'column_name': column_names,
                        'data_type': ['VARCHAR'] * len(column_names)  # Default type
                    })
                    logger.info(f"   Successfully got column information (using default types)")
                except Exception as limit_error:
                    logger.error(f"   LIMIT 0 also failed: {limit_error}")
                    raise
            
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"   LIMIT 0 method failed: {error_msg}")
            
            # Check if table doesn't exist
            if "does not exist" in error_msg or "Catalog Error" in error_msg:
                # Check what tables do exist
                try:
                    existing_tables = conn.execute("SHOW TABLES").fetchall()
                    logger.error(f"   Available tables: {[t[0] for t in existing_tables] if existing_tables else 'None'}")
                except:
                    pass
                raise ValueError(
                    f"Table '{settings.table_name}' does not exist in database.\n"
                    f"Please run the ETL script first: python -m backend.etl data/your_file.csv"
                )
            
            # Fallback: try DESCRIBE
            logger.info("   Trying DESCRIBE as fallback...")
            try:
                describe_result = conn.execute(f"DESCRIBE {settings.table_name}").fetchdf()
                logger.info(f"   DESCRIBE successful, found {len(describe_result)} columns")
                columns_info = describe_result
                # Handle different column name variations
                if 'column_name' not in columns_info.columns:
                    if 'name' in columns_info.columns:
                        columns_info['column_name'] = columns_info['name']
                    else:
                        columns_info['column_name'] = columns_info.iloc[:, 0]
                if 'data_type' not in columns_info.columns:
                    if 'type' in columns_info.columns:
                        columns_info['data_type'] = columns_info['type']
                    elif 'column_type' in columns_info.columns:
                        columns_info['data_type'] = columns_info['column_type']
                    else:
                        columns_info['data_type'] = columns_info.iloc[:, 1]
            except Exception as e2:
                logger.error(f"   DESCRIBE also failed: {str(e2)}")
                raise ValueError(
                    f"Could not introspect table '{settings.table_name}'. "
                    f"Error: {error_msg}. Please ensure the ETL script has been run."
                )
        
        logger.info(f"   Found {len(columns_info)} columns")
        columns = []
        
        for idx, row in columns_info.iterrows():
            col_name = row['column_name']
            col_type = row['data_type'].upper()
            logger.debug(f"   Processing column {idx+1}/{len(columns_info)}: {col_name} ({col_type})")
            
            col_info: Dict[str, Any] = {
                "name": col_name,
                "type": col_type
            }
            
            # Add type-specific statistics (optimized - skip expensive stats for now)
            # For large datasets, computing stats for every column can be very slow
            # We'll only get basic info and skip MIN/MAX/DISTINCT counts for now
            try:
                if col_type in ['INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT', 'DECIMAL', 'NUMERIC']:
                    # Skip MIN/MAX for now - too slow on large datasets
                    # Just mark as numeric type
                    pass
                    
                elif col_type in ['TIMESTAMP', 'DATE']:
                    # Skip MIN/MAX for timestamps - too slow
                    pass
                    
                elif col_type in ['VARCHAR', 'TEXT', 'CHAR']:
                    # Skip DISTINCT count - very expensive on large text columns
                    # Just mark as text type
                    pass
                    
                elif col_type == 'BOOLEAN':
                    # Boolean columns are simple, no stats needed
                    pass
            except Exception as e:
                logger.warning(f"   Warning: Could not get stats for column {col_name}: {e}")
                # Continue without stats
            
            columns.append(col_info)
        
        # Get total row count (this can be slow but is useful)
        logger.info("   Getting total row count...")
        total_rows_result = conn.execute(f"SELECT COUNT(*) as cnt FROM {settings.table_name}").fetchone()
        total_rows = total_rows_result[0] if total_rows_result else 0
        logger.info(f"   Total rows: {total_rows:,}")
        
        schema_result = {
            "table": settings.table_name,
            "total_rows": int(total_rows),
            "columns": columns
        }
        
        logger.info(f"✅ [SCHEMA] Schema introspection completed: {len(columns)} columns")
        return schema_result
    except Exception as e:
        logger.error(f"❌ [SCHEMA] Schema introspection failed: {str(e)}", exc_info=True)
        raise
    finally:
        conn.close()


def get_schema() -> Dict[str, Any]:
    """
    Get the cached schema, or introspect if not cached.
    
    Returns:
        Dictionary containing table schema information
    """
    global _schema_cache
    if _schema_cache is None:
        logger.info("📋 [SCHEMA] Schema cache miss, introspecting...")
        _schema_cache = _introspect_schema()
    else:
        logger.debug("📋 [SCHEMA] Using cached schema")
    return _schema_cache


def clear_schema_cache():
    """Clear the schema cache (useful after ETL operations)."""
    global _schema_cache
    _schema_cache = None

