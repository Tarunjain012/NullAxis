# NYC 311 Data Analytics Agent

A fully functional, LLM-powered analytics agent for the NYC 311 dataset.  
It uses DeepSeek to generate & repair SQL queries dynamically, LangGraph to orchestrate the agent workflow, and DuckDB for fast local analytics.  
Ask natural-language questions and get results + generated SQL instantly.

## Quick Start

### Prerequisites
- Python 3.8+ 
- DeepSeek API key ([Get one here](https://platform.deepseek.com/))

### Setup Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/Tarunjain012/NullAxis.git
   cd NullAxis
   ```

2. **Install dependencies**
   ```bash
   pip install -r backend/requirements.txt
   ```

3. **Create `.env` file** in the project root:
   - Copy `.env.example` to `.env`: `cp .env.example .env` (Linux/Mac) or `copy .env.example .env` (Windows)
   - Or create `.env` manually with:
     ```env
     DEEPSEEK_API_KEY=your_deepseek_api_key_here
     DEEPSEEK_BASE_URL=https://api.deepseek.com/v1
     DEEPSEEK_MODEL=deepseek-chat
     DB_PATH=data/nyc_311.duckdb
     TABLE_NAME=nyc_311
     ```
   - Replace `your_deepseek_api_key_here` with your actual DeepSeek API key

4. **Download and load data**
   - Download the NYC 311 dataset from [NYC Open Data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9)
   - Place the downloaded NYC 311 CSV in the `data/` directory
   - Run the ETL script:
     ```bash
     python -m backend.etl data/311_Service_Requests_from_2010_to_Present.csv
     ```
   - **Note:** This may take several minutes for large datasets. The script will show progress.

5. **Start the backend server**
   ```bash
   uvicorn backend.main:app --reload
   ```
   The API will be available at `http://localhost:8000`

6. **Open the frontend**
   - Option 1: Open `frontend/index.html` directly in your browser
   - Option 2: Serve it with a simple HTTP server:
     ```bash
     # Python
     python -m http.server 8080
     
     # Node.js
     npx http-server frontend -p 8080
     ```
   - Navigate to `http://localhost:8080` (or open `frontend/index.html`)

## Testing the App

Once both backend and frontend are running:

1. **Open the web interface** (from step 6 above)
2. **Example queries you can try** :
  - "Top 10 complaint types"
  - "Percent of requests closed within 3 days"
  - "Which ZIP code reported the most complaints in 2024?"
  - "Average time_to_close_days by borough"
  - "Trend of complaints over months for Noise complaints"

3. **Click "Ask"** or press `Ctrl+Enter` (Windows/Linux) or `Shift+Enter` (Mac)

4. **View the results:**
   - Natural language answer
   - Generated SQL query
   - Results table with data

### Health Check

You can verify the backend is running by visiting:
- `http://localhost:8000/health` - Should return `{"status": "ok"}`

## Features

- Natural language query interface
- Automatic SQL generation using DeepSeek LLM
- SQL validation and repair
- Fast local data processing with DuckDB
- Clean web interface for asking questions and viewing results

## Project Structure

```
.
├── backend/
│   ├── main.py              # FastAPI application
│   ├── agent_graph.py       # LangGraph agent implementation
│   ├── deepseek_client.py   # DeepSeek API wrapper
│   ├── db.py                # DuckDB utilities
│   ├── schema_cache.py      # Schema introspection
│   ├── etl.py               # CSV loading and transformation
│   ├── config.py            # Configuration management
│   └── requirements.txt     # Python dependencies
├── frontend/
│   └── index.html           # Web interface
├── data/                    # Database storage (data files not included)
├── setup.py                 # Setup helper script
└── README.md
```

## Architecture

- **Backend**: FastAPI with LangGraph orchestration
- **LLM**: DeepSeek API for SQL generation and answer synthesis
- **Database**: DuckDB for fast local SQL execution
- **Frontend**: Vanilla HTML/JS with modern UI

## API Endpoints

- `GET /` - Root endpoint
- `GET /health` - Health check
- `POST /chat` - Submit a question and get analysis results

### Chat Request Example

```json
{
  "question": "What are the top 10 complaint types?"
}
```

### Chat Response Example

```json
{
  "answer_text": "The top 10 complaint types are...",
  "sql": "SELECT complaint_type, COUNT(*) as count FROM nyc_311 GROUP BY complaint_type ORDER BY count DESC LIMIT 10",
  "columns": ["complaint_type", "count"],
  "rows": [{"complaint_type": "Noise", "count": 12345}, ...],
  "error": null
}
```

## Technical Notes

- The agent automatically validates and repairs SQL queries
- Maximum LIMIT is enforced at 1000 rows
- Only SELECT queries are allowed (no DDL/DML)
- Schema is automatically introspected from the database
- Data files are not included in the repository due to size limitations (>100MB)

## Troubleshooting

- **Backend not starting**: Check that port 8000 is not in use
- **API errors**: Verify your DeepSeek API key is correct in `.env`
- **Database errors**: Ensure the ETL script completed successfully
- **Frontend can't connect**: Make sure the backend is running on `http://localhost:8000`
