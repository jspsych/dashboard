import os
from dotenv import load_dotenv

load_dotenv(".env")

class Config:
    """Configuration class to manage environment variables"""
    GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')

