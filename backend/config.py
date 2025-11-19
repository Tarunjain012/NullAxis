"""Configuration management for the NYC 311 Analytics Agent."""
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    db_path: str = Field(default="data/nyc_311.duckdb")
    table_name: str = Field(default="nyc_311")
    
    deepseek_api_key: str
    deepseek_base_url: str = Field(default="https://api.deepseek.com/v1")
    deepseek_model: str = Field(default="deepseek-chat")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()

