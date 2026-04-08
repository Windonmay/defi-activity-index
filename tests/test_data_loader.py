# tests/test_data_loader.py
import pytest
from src.data_loader.defillama import fetch_historical_tvl

def test_fetch_tvl_returns_dataframe():
    # 测试 Aave 数据获取
    df = fetch_historical_tvl("aave-v3")
    assert not df.empty
    assert "tvl" in df.columns
    assert df.index.dtype == '<M8[ns]' # 检查索引是否为 datetime