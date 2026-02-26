import sys
import os
from pathlib import Path

# 路径配置
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import requests
import pandas as pd
import time
from datetime import datetime
from config.constants import PROTOCOLS, DEFILLAMA_BASE_URL, DATA_RAW_API_DIR

class DeFiLlamaLoader:
    def __init__(self):
        self.save_dir = Path(DATA_RAW_API_DIR)
        self.save_dir.mkdir(parents=True, exist_ok=True)
        # 初始化 Session，保持连接复用
        self.session = requests.Session()
        # 设置浏览器 Headers，解决 403/429 报错
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://defillama.com/",
            "Origin": "https://defillama.com"
        })

    def fetch_protocol_data(self, protocol_name, protocol_slug):
        """
        对应官方文档: GET /protocol/{slug}
        该接口返回协议的完整历史 TVL 数据。
        """
        url = f"{DEFILLAMA_BASE_URL}/protocol/{protocol_slug}"
        print(f"\n[Request] Fetching {protocol_name} -> {url}")

        try:
            response = self.session.get(url, timeout=30)
            
            # 状态码检查
            if response.status_code == 404:
                print(f"Error 404: Protocol slug '{protocol_slug}' not found.")
                return None
            elif response.status_code == 429:
                print(f"Error 429: Rate limited. Sleeping for 10s...")
                time.sleep(10)
                return self.fetch_protocol_data(protocol_name, protocol_slug) # 重试
            elif response.status_code != 200:
                print(f"Error {response.status_code}: {response.text[:100]}")
                return None

            data = response.json()

            # --- 数据解析逻辑 ---
            
            tvl_data = []
            
            # 1. 优先查找根目录下的 'tvl' (Aggregated TVL)
            if 'tvl' in data and isinstance(data['tvl'], list):
                print(f"Found aggregated TVL data ({len(data['tvl'])} points).")
                tvl_data = data['tvl']
            
            # 2. 如果没有，查找 'chainTvls' (分链 TVL)
            elif 'chainTvls' in data:
                # 优先找 Ethereum，因为它是 DeFi 主战场
                if 'Ethereum' in data['chainTvls'] and 'tvl' in data['chainTvls']['Ethereum']:
                    print("Using 'Ethereum' chain TVL.")
                    tvl_data = data['chainTvls']['Ethereum']['tvl']
                else:
                    # 否则取 TVL 最大的那条链
                    max_chain = None
                    max_val = 0
                    for chain, chain_data in data['chainTvls'].items():
                        # 取最新的一条数据比较大小
                        if 'tvl' in chain_data and len(chain_data['tvl']) > 0:
                            last_tvl = chain_data['tvl'][-1]['totalLiquidityUSD']
                            if last_tvl > max_val:
                                max_val = last_tvl
                                max_chain = chain
                                tvl_data = chain_data['tvl']
                    
                    if max_chain:
                        print(f"Using '{max_chain}' chain TVL (Largest).")

            if not tvl_data:
                print("No TVL data found in response.")
                return None

            # --- 转换为 DataFrame ---
            df = pd.DataFrame(tvl_data)
            
            # 数据清洗：转换时间戳，重命名列
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], unit='s')
            
            if 'totalLiquidityUSD' in df.columns:
                df = df.rename(columns={'totalLiquidityUSD': 'tvl'})
            
            # 筛选最终列
            final_df = df[['date', 'tvl']].copy()
            
            # 过滤掉未来的数据（API有时会返回当天之后的时间戳）
            final_df = final_df[final_df['date'] <= pd.Timestamp.now()]
            
            return final_df

        except requests.exceptions.JSONDecodeError:
            print("Error: API returned invalid JSON. Likely blocked by Cloudflare.")
            print(f"Response snippet: {response.text[:200]}")
            return None
        except Exception as e:
            print(f"Exception: {str(e)}")
            return None
    
    def fetch_dex_volume(self, protocol_name, protocol_slug):

        url = f"{DEFILLAMA_BASE_URL}/summary/dexs/{protocol_slug}"
        print(f"\n[Request] Fetching DEX Volume {protocol_name} -> {url}")

        try:
            response = self.session.get(url, timeout=30)

            if response.status_code == 404:
                print(f"DEX volume not found for {protocol_slug}")
                return None
            elif response.status_code == 429:
                print("Rate limited. Sleeping 10s...")
                time.sleep(10)
                return self.fetch_dex_volume(protocol_name, protocol_slug)
            elif response.status_code != 200:
                print(f"Error {response.status_code}: {response.text[:100]}")
                return None

            data = response.json()

            if "totalDataChart" not in data:
                print("No volume data found.")
                return None

            # -----------------------------------
            # 转换为 DataFrame
            # -----------------------------------
            df = pd.DataFrame(
                data["totalDataChart"],
                columns=["timestamp", "volume"]
            )

            df["date"] = pd.to_datetime(df["timestamp"], unit="s")

            final_df = df[["date", "volume"]].copy()

            final_df = final_df[final_df["date"] <= pd.Timestamp.now()]

            return final_df

        except Exception as e:
            print(f"Exception: {str(e)}")
            return None
        
    def fetch_protocol_fees(self, protocol_name, protocol_slug):
        """
        对应官方文档:
        GET /summary/fees/{protocol}

        返回协议 fees / revenue 的历史数据
        """

        url = f"{DEFILLAMA_BASE_URL}/summary/fees/{protocol_slug}"
        print(f"\n[Request] Fetching Fees {protocol_name} -> {url}")

        try:
            response = self.session.get(url, timeout=30)

            if response.status_code == 404:
                print(f"Fees data not found for {protocol_slug}")
                return None

            elif response.status_code == 429:
                print("Rate limited. Sleeping 10s...")
                time.sleep(10)
                return self.fetch_protocol_fees(protocol_name, protocol_slug)

            elif response.status_code != 200:
                print(f"Error {response.status_code}: {response.text[:100]}")
                return None

            data = response.json()

            # -----------------------------------
            # 数据解析
            # -----------------------------------
            if "totalDataChart" not in data:
                print("No fees data found.")
                return None

            df = pd.DataFrame(
                data["totalDataChart"],
                columns=["timestamp", "fees"]
            )

            df["date"] = pd.to_datetime(df["timestamp"], unit="s")

            final_df = df[["date", "fees"]].copy()

            # 去掉未来时间
            final_df = final_df[final_df["date"] <= pd.Timestamp.now()]

            return final_df

        except Exception as e:
            print(f"Exception: {str(e)}")
            return None
        

    def save_to_csv(self, df, filename):
        file_path = self.save_dir / filename
        df.to_csv(file_path, index=False)
        print(f"Saved to: {file_path}")

    def run_tvl_batch_job(self):
        print("=== Starting DeFiLlama TVL Batch Job ===")
        print(f"Storage Path: {self.save_dir}")
        
        for name, info in PROTOCOLS.items():
            # 获取 slug
            slug = info['slug'] if isinstance(info, dict) else info
            
            df = self.fetch_protocol_data(name, slug)
            
            if df is not None:
                # 生成文件名: tvl_aave_v3.csv
                safe_name = name.lower().replace(" ", "_")
                filename = f"tvl_{safe_name}.csv"
                self.save_to_csv(df, filename)
            
            # 延时，防止封IP
            time.sleep(3)

        print("\n=== Batch Job Completed ===")

    def run_dex_volume_batch_job(self):

        print("\n=== Starting DEX Volume Batch Job ===")

        for name, info in PROTOCOLS.items():

            slug = info['slug']
            protocol_type = info.get("type", "")

            # 只抓 DEX
            if protocol_type != "DEX":
                continue

            df = self.fetch_dex_volume(name, slug)

            if df is not None:
                safe_name = name.lower().replace(" ", "_")
                filename = f"volume_{safe_name}.csv"
                self.save_to_csv(df, filename)

            time.sleep(3)

        print("\n=== DEX Volume Job Completed ===")

    def run_fees_batch_job(self):

        print("\n=== Starting Fees Batch Job ===")

        for name, info in PROTOCOLS.items():

            slug = info['slug']

            df = self.fetch_protocol_fees(name, slug)

            if df is not None:
                safe_name = name.lower().replace(" ", "_")
                filename = f"fees_{safe_name}.csv"
                self.save_to_csv(df, filename)

            time.sleep(3)

        print("\n=== Fees Batch Job Completed ===")


if __name__ == "__main__":
    loader = DeFiLlamaLoader()

    #loader.run_tvl_batch_job()
    #loader.run_dex_volume_batch_job()
    