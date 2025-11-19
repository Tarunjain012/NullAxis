"""DeepSeek API client wrapper."""
import json
import logging
import httpx
from typing import List, Dict, Any, Optional
from .config import settings

logger = logging.getLogger(__name__)


async def call_deepseek_json(
    messages: List[Dict[str, str]],
    response_format: Optional[Dict[str, Any]] = None,
    temperature: float = 0.1
) -> Dict[str, Any]:
    """
    Call DeepSeek chat/completions endpoint and return parsed JSON from the model.
    
    Args:
        messages: OpenAI-style messages list
        response_format: Optional response format specification
        temperature: Temperature for generation (default 0.1 for more deterministic output)
        
    Returns:
        Parsed JSON dictionary from the model's response
        
    Raises:
        Exception: If API call fails or response is invalid JSON
    """
    url = f"{settings.deepseek_base_url}/chat/completions"
    
    headers = {
        "Authorization": f"Bearer {settings.deepseek_api_key}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": settings.deepseek_model,
        "messages": messages,
        "temperature": temperature
    }
    
    # If response_format is provided, add it to payload
    if response_format:
        payload["response_format"] = response_format
    
    logger.debug(f"   API URL: {url}")
    logger.debug(f"   Model: {settings.deepseek_model}")
    logger.debug(f"   Temperature: {temperature}")
    logger.debug(f"   Messages count: {len(messages)}")
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        logger.info(f"   Sending request to DeepSeek API...")
        response = await client.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        result = response.json()
        
        # Extract content from assistant message
        content = result["choices"][0]["message"]["content"]
        
        logger.debug(f"   Response received, content length: {len(content)} chars")
        
        # Parse JSON from content
        # Handle cases where content might be wrapped in markdown code blocks
        content = content.strip()
        if content.startswith("```json"):
            content = content[7:]  # Remove ```json
        if content.startswith("```"):
            content = content[3:]  # Remove ```
        if content.endswith("```"):
            content = content[:-3]  # Remove closing ```
        content = content.strip()
        
        parsed_result = json.loads(content)
        logger.info(f"   ✅ DeepSeek API call successful")
        logger.debug(f"   Parsed result keys: {list(parsed_result.keys())}")
        
        return parsed_result

