"""FastAPI backend for NYC 311 Analytics Agent."""
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from .agent_graph import run_agent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="NYC 311 Analytics Agent")

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    """Request model for chat endpoint."""
    question: str


class ChatResponse(BaseModel):
    """Response model for chat endpoint."""
    answer_text: str | None
    sql: str | None
    columns: list[str]
    rows: list[dict]
    error: str | None


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "NYC 311 Analytics Agent API"}


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    """
    Chat endpoint for natural language queries.
    
    Args:
        req: Chat request with question
        
    Returns:
        Chat response with answer, SQL, and results
    """
    logger.info(f"📥 Received question: {req.question[:100]}...")
    try:
        result = await run_agent(req.question)
        logger.info(f"✅ Successfully processed question. Answer length: {len(result.get('answer_text', '') or '')} chars")
        if result.get('error'):
            logger.warning(f"⚠️  Error in result: {result['error']}")
        return ChatResponse(**result)
    except Exception as e:
        logger.error(f"❌ Error processing question: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

