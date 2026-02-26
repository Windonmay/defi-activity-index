import sys
import os
from pathlib import Path

# 获取当前文件的绝对路径 (d:\Grade4GitHub\defi-activity-index\main.py)
current_file = Path(__file__).resolve()

# 获取项目根目录 (d:\Grade4GitHub\defi-activity-index)
project_root = current_file.parent

# 将项目根目录添加到 sys.path 的最前面
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    from src.data_loader.defillama import DeFiLlamaLoader
    print("Successfully imported DeFiLlamaLoader")
except ImportError as e:
    print(f"ImportError: {e}")
    print(f"Current sys.path: {sys.path}")
    sys.exit(1)

def main():
    print("=== DeFi Activity Index Data Pipeline ===")
    
    try:
        # 1. 初始化加载器
        loader = DeFiLlamaLoader()
        
        # 2. 运行 TVL 抓取任务
        #loader.run_tvl_batch_job()

        # 3. 运行 Volume 抓取任务
        #loader.run_dex_volume_batch_job()

        # 4. 抓取fee信息
        #loader.run_fees_batch_job()

        # 5. 抓取revenue信息
        #loader.run_revenue_batch_job()

        # 6. 抓取稳定币supply数据
        loader.run_stablecoin_raw_data_job()
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()