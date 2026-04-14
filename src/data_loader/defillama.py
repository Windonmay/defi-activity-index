import sys
import os
from pathlib import Path
from defillama_sdk import DefiLlama

# Path configuration
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
        # Initialize session for connection reuse
        self.session = requests.Session()
        # Set browser headers to resolve 403/429 errors
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://defillama.com/",
            "Origin": "https://defillama.com"
        })

    def fetch_protocol_data(self, protocol_name, protocol_slug):
        """
        Fetch protocol TVL data from DeFiLlama API.
        API endpoint: GET /protocol/{slug}
        Returns complete historical TVL data for the protocol.
        """
        url = f"{DEFILLAMA_BASE_URL}/protocol/{protocol_slug}"
        print(f"\n[Request] Fetching {protocol_name} -> {url}")

        try:
            response = self.session.get(url, timeout=30)

            # Status code check
            if response.status_code == 404:
                print(f"Error 404: Protocol slug '{protocol_slug}' not found.")
                return None
            elif response.status_code == 429:
                print(f"Error 429: Rate limited. Sleeping for 10s...")
                time.sleep(10)
                return self.fetch_protocol_data(protocol_name, protocol_slug)
            elif response.status_code != 200:
                print(f"Error {response.status_code}: {response.text[:100]}")
                return None

            data = response.json()

            # Data parsing logic
            tvl_data = []

            # Priority 1: Look for 'tvl' in root (Aggregated TVL)
            if 'tvl' in data and isinstance(data['tvl'], list):
                print(f"Found aggregated TVL data ({len(data['tvl'])} points).")
                tvl_data = data['tvl']

            # Priority 2: Look for 'chainTvls' (per-chain TVL)
            elif 'chainTvls' in data:
                # Prefer Ethereum as the main DeFi battlefield
                if 'Ethereum' in data['chainTvls'] and 'tvl' in data['chainTvls']['Ethereum']:
                    print("Using 'Ethereum' chain TVL.")
                    tvl_data = data['chainTvls']['Ethereum']['tvl']
                else:
                    # Otherwise, take the chain with largest TVL
                    max_chain = None
                    max_val = 0
                    for chain, chain_data in data['chainTvls'].items():
                        # Compare latest data point
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

            # Convert to DataFrame
            df = pd.DataFrame(tvl_data)

            # Data cleaning: convert timestamp, rename columns
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], unit='s')

            if 'totalLiquidityUSD' in df.columns:
                df = df.rename(columns={'totalLiquidityUSD': 'tvl'})

            # Select final columns
            final_df = df[['date', 'tvl']].copy()

            # Filter out future data (API sometimes returns timestamps after today)
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
        """
        Fetch DEX trading volume from DeFiLlama API.
        API endpoint: GET /summary/dexs/{protocol_slug}
        """
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

            # Convert to DataFrame
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
        Fetch protocol fees from DeFiLlama API.
        API endpoint: GET /summary/fees/{protocol}
        Returns historical fees/revenue data.
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

            # Data parsing
            if "totalDataChart" not in data:
                print("No fees data found.")
                return None

            df = pd.DataFrame(
                data["totalDataChart"],
                columns=["timestamp", "fees"]
            )

            df["date"] = pd.to_datetime(df["timestamp"], unit="s")

            final_df = df[["date", "fees"]].copy()

            # Remove future dates
            final_df = final_df[final_df["date"] <= pd.Timestamp.now()]

            return final_df

        except Exception as e:
            print(f"Exception: {str(e)}")
            return None

    def fetch_protocol_revenue(self, protocol_name, protocol_slug):
        """
        Fetch protocol daily revenue from DeFiLlama API.
        API endpoint: GET /summary/fees/{protocol}?dataType=dailyRevenue
        """
        url = f"{DEFILLAMA_BASE_URL}/summary/fees/{protocol_slug}?dataType=dailyRevenue"
        print(f"\n[Request] Fetching Revenue {protocol_name} -> {url}")

        try:
            response = self.session.get(url, timeout=30)

            if response.status_code == 404:
                print(f"Revenue not found for {protocol_slug}")
                return None

            elif response.status_code == 429:
                print("Rate limited. Sleeping 10s...")
                time.sleep(10)
                return self.fetch_protocol_revenue(protocol_name, protocol_slug)

            elif response.status_code != 200:
                print(f"Error {response.status_code}: {response.text[:100]}")
                return None

            data = response.json()

            if "totalDataChart" not in data:
                print("No revenue data found.")
                return None

            # Convert to DataFrame
            df = pd.DataFrame(
                data["totalDataChart"],
                columns=["timestamp", "revenue"]
            )

            df["date"] = pd.to_datetime(df["timestamp"], unit="s")

            final_df = df[["date", "revenue"]].copy()

            final_df = final_df[final_df["date"] <= pd.Timestamp.now()]

            return final_df

        except Exception as e:
            print(f"Exception: {str(e)}")
            return None

    def run_stablecoin_raw_data_job(self):
        """
        Batch job: fetch MakerDAO/DAI raw data and save to CSV.
        """
        print("\n=== Starting Stablecoin Raw Data Batch Job ===")
        print(f"Storage Path: {self.save_dir}")

        # Currently only processes DAI, can be extended to other stablecoins
        stablecoins_to_fetch = [
            {"name": "MakerDAO", "symbol": "DAI", "asset_id": 5}
        ]

        for coin in stablecoins_to_fetch:
            name = coin["name"]
            symbol = coin["symbol"]
            asset_id = coin["asset_id"]

            print(f"\n[Processing Stablecoin]: {name} ({symbol})")
            url = f"https://stablecoins.llama.fi/stablecoin/{asset_id}"
            print(f"[Request] Fetching {symbol} stablecoin raw data -> {url}")

            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()

                # Convert to DataFrame (preserve all fields)
                df = pd.json_normalize(data)

                # Generate filename
                safe_name = name.lower().replace(" ", "_")
                filename = f"stablecoin_raw_{safe_name}.csv"
                self.save_to_csv(df, filename)

            except Exception as e:
                print(f"An error occurred for {name}: {e}")

            # Delay to prevent API IP blocking
            time.sleep(3)

        print("\n=== Stablecoin Raw Data Job Completed ===")

    def save_to_csv(self, df, filename):
        file_path = self.save_dir / filename
        df.to_csv(file_path, index=False)
        print(f"Saved to: {file_path}")

    def run_tvl_batch_job(self):
        print("=== Starting DeFiLlama TVL Batch Job ===")
        print(f"Storage Path: {self.save_dir}")

        for name, info in PROTOCOLS.items():
            # Get slug
            slug = info['slug'] if isinstance(info, dict) else info

            df = self.fetch_protocol_data(name, slug)

            if df is not None:
                # Generate filename: tvl_aave_v3.csv
                safe_name = name.lower().replace(" ", "_")
                filename = f"tvl_{safe_name}.csv"
                self.save_to_csv(df, filename)

            # Delay to prevent IP blocking
            time.sleep(3)

        print("\n=== Batch Job Completed ===")

    def run_dex_volume_batch_job(self):
        print("\n=== Starting DEX Volume Batch Job ===")

        for name, info in PROTOCOLS.items():
            slug = info['slug']
            protocol_type = info.get("type", "")

            # Only fetch DEX data
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

    def run_revenue_batch_job(self):
        print("\n=== Starting Protocol Revenue Batch Job ===")

        for name, info in PROTOCOLS.items():
            slug = info["slug"]

            df = self.fetch_protocol_revenue(name, slug)

            if df is not None:
                safe_name = name.lower().replace(" ", "_")
                filename = f"revenue_{safe_name}.csv"
                self.save_to_csv(df, filename)

            time.sleep(3)

        print("\n=== Revenue Job Completed ===")


if __name__ == "__main__":
    loader = DeFiLlamaLoader()
