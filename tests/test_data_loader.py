# tests/test_data_loader.py
import pytest
from src.data_loader.defillama import fetch_historical_tvl

def test_fetch_tvl_returns_dataframe():
    # Test Aave data fetching
    df = fetch_historical_tvl("aave-v3")
    assert not df.empty
    assert "tvl" in df.columns
    assert df.index.dtype == '<M8[ns]'  # Check if index is datetime
