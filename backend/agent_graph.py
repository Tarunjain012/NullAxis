"""LangGraph agent for SQL generation and execution."""
import logging
from typing import Dict, Any, List
import json
import re
from langgraph.graph import StateGraph, END
from .deepseek_client import call_deepseek_json
from .db import run_query
from .schema_cache import get_schema

logger = logging.getLogger(__name__)


# Context type definition
Context = Dict[str, Any]


# Maximum repair attempts
MAX_REPAIR_ATTEMPTS = 2


def input_node(state: Context) -> Context:
    """Initialize context with user question and schema."""
    logger.info("🔵 [INPUT_NODE] Initializing context...")
    user_question = state.get("user_question", "")
    logger.info(f"   Question: {user_question[:100]}...")
    
    if "schema" not in state:
        logger.info("   Loading schema from database...")
        state["schema"] = get_schema()
        schema_cols = len(state["schema"].get("columns", []))
        logger.info(f"   Schema loaded: {schema_cols} columns, {state['schema'].get('total_rows', 0):,} total rows")
    else:
        logger.info("   Schema already in context")
    
    if "repair_count" not in state:
        state["repair_count"] = 0
    
    logger.info("✅ [INPUT_NODE] Context initialized")
    return state


async def sql_generation_node(state: Context) -> Context:
    """Generate SQL query using DeepSeek."""
    logger.info("🔵 [SQL_GENERATION] Starting SQL generation...")
    user_question = state.get("user_question", "")
    schema = state.get("schema", {})
    
    logger.info(f"   Calling DeepSeek API to generate SQL...")
    
    # Build system prompt for SQL generation
    system_prompt = """You are a SQL generator for a DuckDB database with one table `nyc_311`.

Your task:
1. You will receive the table schema and a natural-language question.
2. You must output a single SQL query as JSON.

Constraints:
- Use only table `nyc_311`.
- Use only columns that exist in the provided schema.
- Use only SELECT or WITH queries (CTEs).
- Always include a LIMIT clause ≤ 1000.
- Never perform DDL/DML (no INSERT/UPDATE/DELETE/ALTER/DROP/etc.).
- Use proper SQL syntax for DuckDB.
- For aggregations, use appropriate GROUP BY clauses.
- For date comparisons, use proper timestamp functions.
- When filtering on calculated columns (like time_to_close_days), handle NULL values appropriately (e.g., use IS NOT NULL in WHERE clauses or COALESCE in comparisons).

Output format (JSON):
{
  "sql": "SELECT ...",
  "explanation": "Brief explanation of what the query does",
  "confidence": 0.0-1.0
}"""

    # Build user prompt with schema and question
    schema_json = json.dumps(schema, indent=2)
    user_prompt = f"""Schema:
{schema_json}

Question: {user_question}

Generate a SQL query to answer this question. Return only valid JSON."""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    
    try:
        result = await call_deepseek_json(messages, temperature=0.1)
        
        state["deepseek_sql"] = result.get("sql", "")
        state["sql_explanation"] = result.get("explanation", "")
        state["sql_confidence"] = result.get("confidence", 0.0)
        
        logger.info(f"✅ [SQL_GENERATION] SQL generated successfully")
        logger.info(f"   Generated SQL: {state['deepseek_sql'][:200]}...")
        logger.info(f"   Explanation: {state.get('sql_explanation', 'N/A')[:100]}...")
        logger.info(f"   Confidence: {state.get('sql_confidence', 0.0):.2f}")
        
    except Exception as e:
        logger.error(f"❌ [SQL_GENERATION] Failed: {str(e)}")
        state["sql_error"] = f"SQL generation failed: {str(e)}"
        state["deepseek_sql"] = None
    
    return state


def sql_validation_node(state: Context) -> Context:
    """Validate generated SQL query."""
    logger.info("🔵 [SQL_VALIDATION] Starting validation...")
    sql = state.get("deepseek_sql", "")
    schema = state.get("schema", {})
    
    logger.info(f"   Validating SQL: {sql[:200]}...")
    
    # Clear previous validation errors
    state["sql_error"] = None
    state["validated_sql"] = None
    
    if not sql or not sql.strip():
        logger.error("❌ [SQL_VALIDATION] SQL query is empty")
        state["sql_error"] = "SQL query is empty"
        return state
    
    sql_upper = sql.upper().strip()
    
    # Check if starts with SELECT or WITH
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        logger.error(f"❌ [SQL_VALIDATION] SQL must start with SELECT or WITH")
        state["sql_error"] = "SQL must start with SELECT or WITH"
        return state
    
    logger.info("   ✓ SQL starts with SELECT/WITH")
    
    # Check for forbidden keywords
    forbidden_keywords = [
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "ATTACH",
        "PRAGMA", "CREATE", "TRUNCATE", "COPY", "ANALYZE", "EXECUTE",
        "EXEC", "CALL", "MERGE", "REPLACE"
    ]
    
    for keyword in forbidden_keywords:
        # Use word boundaries to avoid false positives
        pattern = r'\b' + re.escape(keyword) + r'\b'
        if re.search(pattern, sql_upper):
            state["sql_error"] = f"Forbidden keyword found: {keyword}"
            return state
    
    # Check table references (should only reference nyc_311 or CTE names)
    # This is a simple check - in production you'd use a proper SQL parser
    table_refs = re.findall(r'\bFROM\s+(\w+)\b', sql_upper)
    table_refs.extend(re.findall(r'\bJOIN\s+(\w+)\b', sql_upper))
    
    valid_tables = {"nyc_311", "NYC_311", "Nyc_311"}  # Allow case variations
    # Extract CTE names from WITH clauses
    with_matches = re.findall(r'\bWITH\s+(\w+)', sql_upper)
    valid_tables.update(with_matches)
    
    for table_ref in table_refs:
        # Case-insensitive comparison
        table_ref_lower = table_ref.lower()
        if table_ref_lower != "nyc_311" and table_ref not in valid_tables:
            state["sql_error"] = f"Invalid table reference: {table_ref}. Only 'nyc_311' and CTEs are allowed."
            return state
    
    # Check for LIMIT clause
    has_limit = re.search(r'\bLIMIT\s+\d+', sql_upper)
    if not has_limit:
        # Auto-append LIMIT 1000
        sql = sql.rstrip(';').strip() + " LIMIT 1000"
        state["deepseek_sql"] = sql
    
    # Check LIMIT value
    limit_match = re.search(r'\bLIMIT\s+(\d+)', sql_upper)
    if limit_match:
        limit_value = int(limit_match.group(1))
        if limit_value > 1000:
            state["sql_error"] = f"LIMIT value {limit_value} exceeds maximum of 1000"
            return state
    
    # Basic column validation (check against schema)
    # Extract column references (simplified - not perfect but catches common issues)
    schema_columns = {col["name"].lower() for col in schema.get("columns", [])}
    
    # This is a simplified check - in production use a proper SQL parser
    # For now, we'll let DuckDB handle column validation during execution
    
    # Validation passed
    state["validated_sql"] = state.get("deepseek_sql", sql)
    logger.info(f"✅ [SQL_VALIDATION] Validation passed")
    logger.info(f"   Validated SQL: {state['validated_sql'][:200]}...")
    return state


async def sql_repair_node(state: Context) -> Context:
    """Repair invalid SQL using DeepSeek."""
    logger.info("🔵 [SQL_REPAIR] Starting SQL repair...")
    user_question = state.get("user_question", "")
    schema = state.get("schema", {})
    previous_sql = state.get("deepseek_sql", "")
    sql_error = state.get("sql_error", "")
    repair_count = state.get("repair_count", 0)
    
    logger.info(f"   Repair attempt: {repair_count + 1}/{MAX_REPAIR_ATTEMPTS}")
    logger.info(f"   Previous SQL: {previous_sql[:200]}...")
    logger.info(f"   Error: {sql_error}")
    
    if repair_count >= MAX_REPAIR_ATTEMPTS:
        logger.error(f"❌ [SQL_REPAIR] Maximum repair attempts reached")
        state["sql_error"] = f"Maximum repair attempts ({MAX_REPAIR_ATTEMPTS}) reached. Last error: {sql_error}"
        return state
    
    state["repair_count"] = repair_count + 1
    logger.info(f"   Calling DeepSeek API to repair SQL...")
    
    system_prompt = """You are a SQL repair assistant for DuckDB.

Your task:
1. You receive a schema, a natural-language question, a previous invalid SQL query, and an error message.
2. You must output a corrected SQL query that fixes the error.

Constraints (same as SQL generation):
- Use only table `nyc_311`.
- Use only columns that exist in the provided schema.
- Use only SELECT or WITH queries.
- Always include a LIMIT clause ≤ 1000.
- Never perform DDL/DML.
- Fix the specific error mentioned.

Output format (JSON):
{
  "sql": "SELECT ...",
  "explanation": "What was fixed and why"
}"""

    schema_json = json.dumps(schema, indent=2)
    user_prompt = f"""Schema:
{schema_json}

Question: {user_question}

Previous SQL (had error):
{previous_sql}

Error: {sql_error}

Generate a corrected SQL query. Return only valid JSON."""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    
    try:
        result = await call_deepseek_json(messages, temperature=0.1)
        
        state["deepseek_sql"] = result.get("sql", "")
        state["sql_explanation"] = result.get("explanation", "")
        state["sql_error"] = None  # Clear error, will be re-validated
        
        logger.info(f"✅ [SQL_REPAIR] SQL repaired successfully")
        logger.info(f"   Repaired SQL: {state['deepseek_sql'][:200]}...")
        
    except Exception as e:
        logger.error(f"❌ [SQL_REPAIR] Failed: {str(e)}")
        state["sql_error"] = f"SQL repair failed: {str(e)}"
    
    return state


def sql_execution_node(state: Context) -> Context:
    """Execute validated SQL query."""
    logger.info("🔵 [SQL_EXECUTION] Starting SQL execution...")
    validated_sql = state.get("validated_sql")
    
    if not validated_sql:
        logger.error("❌ [SQL_EXECUTION] No validated SQL to execute")
        state["sql_error"] = "No validated SQL to execute"
        return state
    
    logger.info(f"   Executing SQL: {validated_sql[:200]}...")
    
    try:
        columns, rows = run_query(validated_sql)
        state["result_columns"] = columns
        state["result_rows"] = rows
        state["sql_error"] = None  # Clear any previous errors
        
        logger.info(f"✅ [SQL_EXECUTION] Query executed successfully")
        logger.info(f"   Columns: {', '.join(columns)}")
        logger.info(f"   Rows returned: {len(rows):,}")
        if rows:
            logger.info(f"   Sample row: {str(rows[0])[:200]}...")
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ [SQL_EXECUTION] Failed: {error_msg}")
        state["sql_error"] = f"SQL execution failed: {error_msg}"
        state["result_columns"] = []
        state["result_rows"] = []
    
    return state


async def answer_generation_node(state: Context) -> Context:
    """Generate natural language answer from query results."""
    logger.info("🔵 [ANSWER_GENERATION] Starting answer generation...")
    user_question = state.get("user_question", "")
    validated_sql = state.get("validated_sql", "")
    result_columns = state.get("result_columns", [])
    result_rows = state.get("result_rows", [])
    
    # If there was an error, don't generate answer
    if state.get("sql_error"):
        logger.warning(f"⚠️  [ANSWER_GENERATION] Error detected, skipping LLM call: {state['sql_error']}")
        state["final_answer"] = f"Error: {state['sql_error']}"
        return state
    
    logger.info(f"   Calling DeepSeek API to generate answer...")
    logger.info(f"   Result rows: {len(result_rows):,}, Columns: {len(result_columns)}")
    
    system_prompt = """You are a data analyst assistant.

Your task:
1. You receive a user's question, the SQL query used to answer it, and the resulting table.
2. You must provide a clear, concise answer in 2-4 sentences.

Guidelines:
- Describe the answer using only information from the result table.
- Do not invent counts or values not present in the results.
- If the result is a single scalar/row, state it explicitly.
- If there are many rows, summarize the key patterns (top groups, trends, percentages).
- Use specific numbers from the results.
- Be conversational but precise.

Output format (JSON):
{
  "answer": "Your answer here..."
}"""

    # Prepare sample rows (limit to 50 for prompt)
    sample_rows = result_rows[:50]
    
    user_prompt = f"""Question: {user_question}

SQL Query:
{validated_sql}

Result Table:
Columns: {', '.join(result_columns)}
Total Rows: {len(result_rows)}

Sample Rows (first {len(sample_rows)}):
{json.dumps(sample_rows, indent=2, default=str)}

Generate a clear answer to the question based on these results. Return only valid JSON."""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    
    try:
        result = await call_deepseek_json(messages, temperature=0.3)
        state["final_answer"] = result.get("answer", "Unable to generate answer.")
        
        logger.info(f"✅ [ANSWER_GENERATION] Answer generated successfully")
        logger.info(f"   Answer: {state['final_answer'][:200]}...")
        
    except Exception as e:
        logger.error(f"❌ [ANSWER_GENERATION] Failed: {str(e)}")
        # Fallback answer if generation fails
        if result_rows:
            state["final_answer"] = f"Query executed successfully and returned {len(result_rows)} rows. Columns: {', '.join(result_columns)}"
        else:
            state["final_answer"] = f"Query executed but returned no results. Error generating answer: {str(e)}"
    
    return state


def output_node(state: Context) -> Context:
    """Prepare final output payload."""
    logger.info("🔵 [OUTPUT_NODE] Preparing final output...")
    logger.info(f"   Final answer length: {len(state.get('final_answer', '') or '')} chars")
    logger.info(f"   SQL: {'Present' if state.get('validated_sql') else 'None'}")
    logger.info(f"   Rows: {len(state.get('result_rows', []))}")
    logger.info(f"   Error: {'None' if not state.get('sql_error') else state['sql_error'][:100]}")
    logger.info("✅ [OUTPUT_NODE] Output prepared")
    return state


def should_repair(state: Context) -> str:
    """Conditional edge: should we repair SQL?"""
    sql_error = state.get("sql_error")
    repair_count = state.get("repair_count", 0)
    validated_sql = state.get("validated_sql")
    
    # If we have a validated SQL, continue to execution
    if validated_sql:
        logger.info(f"   → Routing to SQL execution (validated SQL present)")
        return "continue"
    
    # If there's an error and we can still repair, go to repair
    if sql_error and repair_count < MAX_REPAIR_ATTEMPTS:
        logger.info(f"   → Routing to SQL repair (attempt {repair_count + 1}/{MAX_REPAIR_ATTEMPTS})")
        return "repair"
    
    # If there's an error but no validated SQL and repairs exhausted, skip to answer generation
    if sql_error and not validated_sql:
        logger.warning(f"   → Routing to answer generation (repairs exhausted, no valid SQL)")
        return "skip_to_answer"
    
    # Default: continue to execution
    logger.info(f"   → Routing to SQL execution (default)")
    return "continue"


def should_retry_validation(state: Context) -> str:
    """Conditional edge: after repair, re-validate."""
    return "validate"


def build_graph() -> StateGraph:
    """Build and return the LangGraph state graph."""
    workflow = StateGraph(dict)
    
    # Add nodes
    workflow.add_node("input", input_node)
    workflow.add_node("generate_sql", sql_generation_node)
    workflow.add_node("validate_sql", sql_validation_node)
    workflow.add_node("repair_sql", sql_repair_node)
    workflow.add_node("execute_sql", sql_execution_node)
    workflow.add_node("generate_answer", answer_generation_node)
    workflow.add_node("output", output_node)
    
    # Set entry point
    workflow.set_entry_point("input")
    
    # Add edges
    workflow.add_edge("input", "generate_sql")
    workflow.add_edge("generate_sql", "validate_sql")
    
    # Conditional: repair, continue, or skip to answer
    workflow.add_conditional_edges(
        "validate_sql",
        should_repair,
        {
            "repair": "repair_sql",
            "continue": "execute_sql",
            "skip_to_answer": "generate_answer"
        }
    )
    
    # After repair, re-validate
    workflow.add_edge("repair_sql", "validate_sql")
    
    # After execution, generate answer
    workflow.add_edge("execute_sql", "generate_answer")
    
    # After answer generation, output
    workflow.add_edge("generate_answer", "output")
    
    # Output is terminal
    workflow.add_edge("output", END)
    
    return workflow.compile()


# Global graph instance
_graph = None


def get_graph():
    """Get or create the LangGraph instance."""
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph


async def run_agent(user_question: str) -> Dict[str, Any]:
    """
    Run the analytics agent for a user question.
    
    Args:
        user_question: Natural language question about the data
        
    Returns:
        Dictionary with answer_text, sql, columns, rows, error
    """
    logger.info("=" * 80)
    logger.info("🚀 [AGENT] Starting agent execution")
    logger.info("=" * 80)
    
    # Initialize context
    logger.info("   Loading schema...")
    try:
        schema = get_schema()
        logger.info(f"   Schema loaded: {len(schema.get('columns', []))} columns")
    except Exception as e:
        logger.error(f"❌ [AGENT] Failed to load schema: {str(e)}", exc_info=True)
        raise
    
    context: Context = {
        "user_question": user_question,
        "schema": schema,
        "deepseek_sql": None,
        "sql_explanation": None,
        "validated_sql": None,
        "sql_error": None,
        "result_rows": [],
        "result_columns": [],
        "final_answer": None,
        "repair_count": 0
    }
    
    # Run graph
    logger.info("   Building LangGraph workflow...")
    graph = get_graph()
    
    # LangGraph supports async invoke
    try:
        logger.info("   Invoking LangGraph workflow...")
        final_state = await graph.ainvoke(context)
        logger.info("✅ [AGENT] Agent execution completed successfully")
    except Exception as e:
        logger.error(f"❌ [AGENT] Agent execution failed: {str(e)}", exc_info=True)
        return {
            "answer_text": f"Agent execution failed: {str(e)}",
            "sql": None,
            "columns": [],
            "rows": [],
            "error": str(e)
        }
    
    # Extract final output
    result = {
        "answer_text": final_state.get("final_answer"),
        "sql": final_state.get("validated_sql"),
        "columns": final_state.get("result_columns", []),
        "rows": final_state.get("result_rows", []),
        "error": final_state.get("sql_error")
    }
    
    logger.info("=" * 80)
    logger.info("🏁 [AGENT] Agent execution finished")
    logger.info("=" * 80)
    
    return result

