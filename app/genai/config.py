from pydantic import BaseSettings
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()

class GenAIConfig(BaseSettings):
    """Configuration for GenAI module."""
    
    # OpenAI Configuration
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = "gpt-4"
    openai_temperature: float = 0.1
    openai_max_tokens: int = 4000
    
    # Database Configuration
    database_host: str = os.getenv("DATABASE_HOST", "localhost")
    database_port: int = int(os.getenv("DATABASE_PORT", "5432"))
    database_name: str = os.getenv("DATABASE_NAME", "zoo_db")
    database_user: str = os.getenv("DATABASE_USER", "postgres")
    database_password: str = os.getenv("DATABASE_PASSWORD", "")
    
    # Vector Database Configuration
    vector_db_path: str = "data/vector_db"
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    
    # RAG Configuration
    chunk_size: int = 1000
    chunk_overlap: int = 200
    top_k_results: int = 5
    
    # Analysis Configuration
    cost_forecast_months: int = 3
    daily_analysis_hours: int = 6  # Run analysis every 6 hours
    min_cost_saving_threshold: float = 50.0  # Minimum $50 savings to report
    min_revenue_increase_threshold: float = 100.0  # Minimum $100 increase to report
    
    # Chatbot Configuration
    chat_history_length: int = 10
    max_query_length: int = 500
    
    class Config:
        env_file = ".env"

# Global configuration instance
config = GenAIConfig() 