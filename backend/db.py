"""Database utilities for DuckDB operations."""
import logging
from typing import Tuple, List, Dict, Any
import duckdb
from .config import settings

logger = logging.getLogger(__name__)


def get_conn():
    """Get a DuckDB connection to the configured database."""
    return duckdb.connect(settings.db_path, read_only=False)


def run_query(sql: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    """
    Execute a SQL query and return columns and rows.
    
    Args:
        sql: SQL query string to execute
        
    Returns:
        Tuple of (column_names, rows_as_dicts)
        
    Raises:
        Exception: If query execution fails
    """
    logger.debug(f"   Executing SQL query: {sql[:200]}...")
    conn = get_conn()
    try:
        # Execute query and get column names from result description
        # This avoids pandas array issues by using native DuckDB methods
        result = conn.execute(sql)
        
        # Get column names from result description (available before fetching)
        columns = None
        if hasattr(result, 'description') and result.description:
            columns = [desc[0] for desc in result.description]
        
        # If no description, try to get column names from a wrapped query
        if not columns:
            try:
                # Wrap query to get column info (works for most SELECT queries)
                wrapped_sql = f"SELECT * FROM ({sql}) LIMIT 0"
                col_result = conn.execute(wrapped_sql)
                if hasattr(col_result, 'description') and col_result.description:
                    columns = [desc[0] for desc in col_result.description]
            except Exception as wrap_error:
                logger.warning(f"   Could not get columns from wrapped query: {wrap_error}")
        
        # If still no columns, we have a problem
        if not columns:
            raise ValueError("Could not determine column names from query result")
        
        # Fetch all rows using native method (avoids pandas issues)
        all_rows = result.fetchall()
        
        # Convert rows to list of dictionaries
        rows = []
        for row in all_rows:
            row_dict = {}
            for i, col in enumerate(columns):
                if i < len(row):
                    value = row[i]
                    # Convert numpy/pandas types to Python native types
                    if hasattr(value, 'item'):
                        row_dict[col] = value.item()
                    elif hasattr(value, 'tolist'):
                        row_dict[col] = value.tolist()
                    else:
                        row_dict[col] = value
            rows.append(row_dict)
        
        logger.debug(f"   Query executed: {len(columns)} columns, {len(rows)} rows")
        return columns, rows
    except Exception as e:
        logger.error(f"   Query execution error: {str(e)}")
        raise
    finally:
        conn.close()

