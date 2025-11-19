# NYC 311 Analytics Agent - Complete Flow Diagram

## End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. FRONTEND (index.html)                                        │
│    User enters question → submitQuestion()                     │
│    POST /chat with {question: "..."}                            │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. FASTAPI BACKEND (main.py)                                    │
│    @app.post("/chat")                                           │
│    Receives ChatRequest → calls run_agent(req.question)         │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. AGENT GRAPH (agent_graph.py)                                │
│    run_agent() initializes context:                             │
│    - user_question: str                                         │
│    - schema: dict (from schema_cache)                          │
│    - All other fields: None/empty                               │
│                                                                 │
│    Then calls: graph.ainvoke(context)                          │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. LANGGRAPH EXECUTION FLOW                                    │
│                                                                 │
│    ┌─────────────┐                                              │
│    │ input_node  │  Initialize schema if missing               │
│    └──────┬──────┘                                              │
│           │                                                      │
│           ▼                                                      │
│    ┌──────────────────┐                                         │
│    │ sql_generation_  │  Call DeepSeek API                     │
│    │ node (async)     │  Generate SQL from question + schema   │
│    └──────┬───────────┘                                         │
│           │                                                      │
│           ▼                                                      │
│    ┌──────────────────┐                                         │
│    │ sql_validation_  │  Check SQL safety:                      │
│    │ node (sync)      │  - Starts with SELECT/WITH             │
│    └──────┬───────────┘  - No forbidden keywords                │
│           │            - Valid table references                  │
│           │            - LIMIT ≤ 1000                            │
│           │                                                      │
│           ├──────────────┐                                      │
│           │              │                                      │
│           ▼              ▼                                      │
│    ┌──────────┐  ┌──────────────┐                             │
│    │ repair   │  │ continue     │                              │
│    │ (if error│  │ (if valid or │                              │
│    │  & tries │  │  max repairs)│                              │
│    │  < 2)    │  │              │                              │
│    └────┬─────┘  └──────┬───────┘                              │
│         │               │                                        │
│         │               ▼                                        │
│         │      ┌──────────────────┐                              │
│         │      │ sql_execution_   │  Run query via DuckDB        │
│         │      │ node (sync)      │  Get columns + rows          │
│         │      └──────┬───────────┘                              │
│         │             │                                            │
│         │             ▼                                            │
│         │      ┌──────────────────┐                              │
│         │      │ answer_generation│  Call DeepSeek API            │
│         │      │ _node (async)    │  Generate natural language    │
│         │      └──────┬───────────┘                              │
│         │             │                                            │
│         │             ▼                                            │
│         │      ┌─────────────┐                                   │
│         │      │ output_node │  Return final state              │
│         │      └──────┬───────┘                                  │
│         │             │                                            │
│         └─────────────┘                                            │
│              (loop back to validate if repaired)                 │
│                                                                 │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. RETURN TO FASTAPI                                            │
│    Extract from final_state:                                    │
│    - answer_text: str                                            │
│    - sql: str                                                    │
│    - columns: list[str]                                          │
│    - rows: list[dict]                                            │
│    - error: str | None                                           │
│                                                                 │
│    Return ChatResponse(**result)                                │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. FRONTEND DISPLAY                                             │
│    Receive JSON response → Display:                             │
│    - answer_text in answer box                                  │
│    - sql in SQL box (if present)                                │
│    - rows in table (if present)                                 │
│    - error in error box (if present)                             │
└─────────────────────────────────────────────────────────────────┘
```

## Key Components

### State Context (passed between nodes)
```python
{
    "user_question": str,           # Original question
    "schema": dict,                  # Table schema with column info
    "deepseek_sql": str | None,      # Generated SQL
    "sql_explanation": str | None,    # Explanation of SQL
    "validated_sql": str | None,     # Validated SQL
    "sql_error": str | None,         # Error message if any
    "result_columns": list[str],      # Query result columns
    "result_rows": list[dict],       # Query result rows
    "final_answer": str | None,      # Natural language answer
    "repair_count": int              # Number of repair attempts
}
```

### Node Functions

1. **input_node** (sync)
   - Ensures schema is loaded
   - Initializes repair_count

2. **sql_generation_node** (async)
   - Calls DeepSeek API with question + schema
   - Returns JSON with SQL, explanation, confidence

3. **sql_validation_node** (sync)
   - Validates SQL syntax and safety
   - Sets validated_sql or sql_error

4. **sql_repair_node** (async)
   - Called if validation fails and repair_count < 2
   - Calls DeepSeek API to fix SQL
   - Increments repair_count

5. **sql_execution_node** (sync)
   - Executes validated_sql via DuckDB
   - Returns columns and rows
   - Handles execution errors

6. **answer_generation_node** (async)
   - Calls DeepSeek API with question + SQL + results
   - Generates natural language answer
   - Handles error cases

7. **output_node** (sync)
   - Final node, returns state as-is

## Error Handling

- **SQL Generation Error**: Sets sql_error, flow continues to validation
- **Validation Error**: Triggers repair if attempts < 2, else continues to execution
- **Execution Error**: Sets sql_error, flow continues to answer generation
- **Answer Generation Error**: Falls back to basic answer or error message

## API Calls to DeepSeek

1. **SQL Generation**: Question + Schema → SQL query
2. **SQL Repair**: Question + Schema + Previous SQL + Error → Fixed SQL
3. **Answer Generation**: Question + SQL + Results → Natural language answer

