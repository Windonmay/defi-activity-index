import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Retrieve configuration from environment variables."""

    # Dune Analytics
    DUNE_API_KEY: str = os.getenv("DUNE_API_KEY", "")
    if not DUNE_API_KEY:
        raise ValueError("DUNE_API_KEY not set in environment or .env file")


# Create configuration instance for use by other modules
config = Config()
