import pandas as pd
import numpy as np
import os

# ==========================================
# 1. 路径配置 (自动适配项目结构)
# ==========================================
# 获取当前脚本绝对路径: .../src/data_processor/feature_engineer.py
CURRENT_SCRIPT = os.path.abspath(__file__)
# 回退3级找到项目根目录: .../defi-activity-index/
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_SCRIPT)))

INPUT_FILE = os.path.join(PROJECT_ROOT, 'data', 'processed', 'master_dataset_clean.csv')
OUTPUT_FILE = os.path.join(PROJECT_ROOT, 'data', 'processed', 'clean_dataset_final.csv')

# ==========================================
# 2. 读取数据
# ==========================================
if not os.path.exists(INPUT_FILE):
    raise FileNotFoundError(f"输入文件缺失: {INPUT_FILE}\n请先运行 Step 3 的清洗脚本！")

print(f"正在读取数据: {INPUT_FILE} ...")
df = pd.read_csv(INPUT_FILE)
df['Date'] = pd.to_datetime(df['Date'])

# ==========================================
# 3. 计算 Capital Turnover Ratio
# ==========================================
# 公式: Turnover = Core Utility / TVL
# 逻辑: 衡量每 $1 TVL 产生了多少业务量

print("正在计算 Capital Turnover Ratio ...")

# 3.1 核心计算
df['Capital_Turnover'] = df['Core_Utility'] / df['TVL']

# 3.2 异常处理 (Data Sanity Check)
# 情况 A: TVL 为 0 导致除以零 (inf) -> 替换为 0
df['Capital_Turnover'] = df['Capital_Turnover'].replace([np.inf, -np.inf], 0)

# 情况 B: 分子分母都为 0 (NaN) -> 填充为 0
df['Capital_Turnover'] = df['Capital_Turnover'].fillna(0)

# 3.3 极端值处理 (可选，防止数据错误导致的超大值影响后续标准化)
# 例如：如果某天 TVL 只有 $1，但 Utility 有 $1M，Ratio 会高达 1,000,000
# 这里我们打印最大值检查一下，暂不暴力截断，留给 Step 5 标准化处理
max_ratio = df['Capital_Turnover'].max()
print(f"  -> 当前最大周转率: {max_ratio:.4f}")

# 新增：保留3位小数
df['Capital_Turnover'] = df['Capital_Turnover'].round(3)

# ==========================================
# 4. 验证与统计 (Validation)
# ==========================================
print("\n=== 各协议周转率均值 (用于逻辑检查) ===")
# 理论上：DEX (Uniswap) 应该很高 (>0.1)，Lending (Aave) 应该较低 (<0.1)
stats = df.groupby('Protocol')['Capital_Turnover'].mean()
print(stats)

# 简单的逻辑检查
if 'uniswap' in stats.index and 'aave' in stats.index:
    if stats['uniswap'] > stats['aave']:
        print("\n逻辑检查通过: DEX 周转率高于借贷协议。")
    else:
        print("\n警告: DEX 周转率低于借贷协议，请检查 Core Utility 数据源单位是否正确！")

# ==========================================
# 5. 保存最终数据集
# ==========================================
df.to_csv(OUTPUT_FILE, index=False)
print(f"\n完成！最终数据集已保存至: {OUTPUT_FILE}")
print(f"包含列: {list(df.columns)}")