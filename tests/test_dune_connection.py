import requests
import pandas as pd
from datetime import datetime, timedelta

# ---------------------------
# 1. API 地址
# ---------------------------
url = "https://api.llama.fi/protocol/aave"

# ---------------------------
# 2. 请求数据
# ---------------------------
response = requests.get(url, timeout=30)
response.raise_for_status()

data = response.json()

# ---------------------------
# 3. 提取 TVL 时间序列
# ---------------------------
tvl_list = data["tvl"]

df = pd.DataFrame(tvl_list)

# 转换时间格式
df["date"] = pd.to_datetime(df["date"], unit="s")

# ---------------------------
# 4. 过滤最近7天
# ---------------------------
today = pd.Timestamp.today()
seven_days_ago = today - pd.Timedelta(days=365)

df_last7 = df[df["date"] >= seven_days_ago]

# ---------------------------
# 5. 打印结果
# ---------------------------
print("\nAave TVL - Last 7 Days:\n")
print(df_last7[["date", "totalLiquidityUSD"]])