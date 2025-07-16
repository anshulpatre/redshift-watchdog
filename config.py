"""
Configuration module for Redshift Watchdog
"""
import os
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration class for the application"""
    
    # Gemini API Configuration
    GOOGLE_API_KEY: str = os.getenv("GOOGLE_API_KEY", "AIzaSyCufVk_n37OZvtIlz2z131N8")
    GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
    
    # Redshift Database Configuration (defaults)
    REDSHIFT_HOST: Optional[str] = os.getenv("REDSHIFT_HOST")
    REDSHIFT_DATABASE: Optional[str] = os.getenv("REDSHIFT_DATABASE")
    REDSHIFT_USER: Optional[str] = os.getenv("REDSHIFT_USER")
    REDSHIFT_PASSWORD: Optional[str] = os.getenv("REDSHIFT_PASSWORD")
    REDSHIFT_PORT: int = int(os.getenv("REDSHIFT_PORT", "5439"))
    
    # Application Configuration
    DB_MCP_MODE: str = os.getenv("DB_MCP_MODE", "readonly")
    STREAMLIT_PORT: int = int(os.getenv("STREAMLIT_PORT", "8501"))
    
    @classmethod
    def get_redshift_params(cls) -> dict:
        """Get Redshift connection parameters from environment"""
        return {
            "host": cls.REDSHIFT_HOST,
            "database": cls.REDSHIFT_DATABASE,
            "user": cls.REDSHIFT_USER,
            "password": cls.REDSHIFT_PASSWORD,
            "port": cls.REDSHIFT_PORT
        }
    
    @classmethod
    def has_redshift_config(cls) -> bool:
        """Check if all required Redshift parameters are configured"""
        params = cls.get_redshift_params()
        return all([
            params["host"],
            params["database"],
            params["user"],
            params["password"]
        ]) 