# config/constants.py

# # 选定的协议列表 (Slug 需与 DeFiLlama API 对应)
# PROTOCOLS = {
#     "Aave V3": {"slug": "aave-v3", "type": "Lending"},
#     "Compound V3": {"slug": "compound-v3", "type": "Lending"},
#     "MakerDAO": {"slug": "makerdao", "type": "Stablecoin"},
#     "Uniswap V3": {"slug": "uniswap-v3", "type": "DEX"},
#     "Curve": {"slug": "curve-dex", "type": "DEX"},
#     "Lido": {"slug": "lido", "type": "Liquid Staking"},
# }

# # 核心指标定义
# INDICATORS = [
#     "tvl", "revenue", "fees", "active_users", "tx_count", "core_utility", "efficiency"
# ]

# # API 端点
# API_BASE_URL = "https://api.llama.fi"

import os
from pathlib import Path

# 1. 路径配置
# 获取项目根目录 (假设 config/ 在根目录下)
# Path(__file__) 是当前文件，.parent 是 config/，.parent.parent 是根目录
PROJECT_ROOT = Path(__file__).parent.parent
DATA_RAW_API_DIR = PROJECT_ROOT / "data" / "raw" / "api"

# 2. API 配置
DEFILLAMA_BASE_URL = "https://api.llama.fi"

# 3. 协议映射配置
# Key: 你在报告中使用的显示名称
# Value: DeFiLlama API 需要的 slug (必须准确)
# PROTOCOLS = {
#     "Aave V3": "aave-v3",
#     "Compound V3": "compound-v3",
#     "MakerDAO": "makerdao",
#     "Uniswap V3": "uniswap-v3",
#     "Curve": "curve-dex",
#     "Lido": "lido"
# }
PROTOCOLS = {
    "Aave V3": {"slug": "aave-v3", "type": "Lending"},
    "Compound V3": {"slug": "compound-v3", "type": "Lending"},
    "MakerDAO": {"slug": "makerdao", "type": "Stablecoin"},
    "Uniswap V3": {"slug": "uniswap-v3", "type": "DEX"},
    "Curve": {"slug": "curve-finance", "type": "DEX"},
    "Lido": {"slug": "lido", "type": "Liquid Staking"},
}