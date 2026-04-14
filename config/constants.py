import os
from pathlib import Path

# Path configuration
PROJECT_ROOT = Path(__file__).parent.parent
DATA_RAW_API_DIR = PROJECT_ROOT / "data" / "raw" / "api"

# API configuration
DEFILLAMA_BASE_URL = "https://api.llama.fi"

# Protocol mapping configuration
# Key: Display name used in reports
# Value: DeFiLlama API slug (must be accurate)
PROTOCOLS = {
    "Aave V3": {"slug": "aave-v3", "type": "Lending"},
    "Compound V3": {"slug": "compound-v3", "type": "Lending"},
    "MakerDAO": {"slug": "makerdao", "type": "Stablecoin"},
    "Uniswap V3": {"slug": "uniswap-v3", "type": "DEX"},
    "Curve": {"slug": "curve-finance", "type": "DEX"},
    "Lido": {"slug": "lido", "type": "Liquid Staking"},
}

DUNE_QUERY_IDS = {
    "Aave V3": 1950309,
}
