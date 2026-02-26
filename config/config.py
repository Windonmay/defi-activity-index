import os
from dotenv import load_dotenv

# 加载 .env 文件中的变量到环境变量 os.environ
load_dotenv()

class Config:
    """从环境变量获取配置信息的类。"""
    
    # Dune Analytics
    DUNE_API_KEY: str = os.getenv("DUNE_API_KEY", "")
    if not DUNE_API_KEY:
        raise ValueError("DUNE_API_KEY 未在环境变量或 .env 文件中设置")
    
    DUNE_QUERY_IDS = {
    "Aave V3": 1950309,
    "Compound V3": 1950310,
    "Uniswap V3": 1950311,
    "Curve Finance": 1950312,
    "MakerDAO": 1950313,
    "Lido": 1950314,
}

# 创建一个配置实例，方便其他模块导入使用
config = Config()    