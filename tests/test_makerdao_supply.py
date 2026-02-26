import pandas as pd
import requests
from pathlib import Path

# 临时保存目录
SAVE_DIR = Path("data/raw/api/")
SAVE_DIR.mkdir(parents=True, exist_ok=True)

def fetch_dai_raw_data():
    asset_id = 5  # MakerDAO / DAI
    url = f"https://stablecoins.llama.fi/stablecoin/{asset_id}"
    print(f"[Request] Fetching DAI stablecoin raw data -> {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()
        print("Raw JSON received:")
       

        # 尝试转换为 DataFrame（保留所有字段）
        df = pd.json_normalize(data)
        print("\nDataFrame preview:")
        print(df.head())

        # 保存到 CSV
        file_path = SAVE_DIR / "stablecoin_dai_raw.csv"
        df.to_csv(file_path, index=False)
        print(f"\nSaved raw data to {file_path}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    fetch_dai_raw_data()