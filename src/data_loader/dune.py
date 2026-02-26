import sys
from pathlib import Path
import time
from datetime import datetime
import pandas as pd
from dune_client.client import DuneClient

# -----------------------------------
# 路径配置
# -----------------------------------
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.config import config  # 引入统一配置
from config.constants import DATA_RAW_API_DIR, DUNE_QUERY_IDS  # 查询 ID 配置

class DuneLoader:
    def __init__(self):
        self.save_dir = Path(DATA_RAW_API_DIR)
        self.save_dir.mkdir(parents=True, exist_ok=True)
        self.client = DuneClient(config.DUNE_API_KEY)  # 直接使用统一配置

    def fetch_query_result(self, query_id: int):
        """
        获取 Dune 查询最新结果
        """
        print(f"\n[Request] Fetching Dune Query {query_id}")

        try:
            query_result = self.client.get_latest_result(query_id)
            
            # 使用 result 属性
            if not query_result or not hasattr(query_result, "result"):
                print(f"No data returned for query {query_id}")
                return None

            data = query_result.result.rows  # 正确访问 rows
            if not data:
                print("Query returned 0 rows.")
                return None

            df = pd.DataFrame(data)

            # 如果有日期字段 dt，则转换为 datetime
            if "dt" in df.columns:
                df["dt"] = pd.to_datetime(df["dt"])

            return df

        except Exception as e:
            print(f"Exception fetching query {query_id}: {str(e)}")
            return None

    def save_to_csv(self, df: pd.DataFrame, filename: str):
        """
        保存 DataFrame 到 CSV
        """
        file_path = self.save_dir / filename
        df.to_csv(file_path, index=False)
        print(f"Saved to: {file_path}")

    def run_batch_job(self):
        """
        批量抓取 Dune 查询
        """
        print("\n=== Starting Dune Batch Job ===")

        for name, query_id in DUNE_QUERY_IDS.items():
            print(f"\nProcessing {name} (Query ID: {query_id})")
            df = self.fetch_query_result(query_id)

            if df is not None and not df.empty:
                safe_name = name.lower().replace(" ", "_")
                filename = f"dune_{safe_name}.csv"
                self.save_to_csv(df, filename)

            time.sleep(3)  # 避免 API 限制

        print("\n=== Dune Batch Job Completed ===")