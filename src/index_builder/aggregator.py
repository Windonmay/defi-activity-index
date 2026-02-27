import pandas as pd
import numpy as np
import os

# ==========================================
# 1. 路径与配置
# ==========================================
CURRENT_SCRIPT = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_SCRIPT)))

# 读取标准化后的主数据集 (Min-Max)
INPUT_FILE = os.path.join(PROJECT_ROOT, 'data', 'processed', 'normalized_minmax.csv')
OUTPUT_FILE = os.path.join(PROJECT_ROOT, 'data', 'final', 'final_index.csv')

# ==========================================
# 2. 定义权重方案 (Weighting Scheme)
# ==========================================
# 有 4 个维度，总权重 1.0 (100%)
# 每个维度内部均分权重

WEIGHTS = {
    # --- Dimension 1: Capital Efficiency (25%) ---
    'TVL_score':              0.125,  # 25% / 2
    'Capital_Turnover_score': 0.125,  # 25% / 2

    # --- Dimension 2: User Activity (25%) ---
    'DAU_score':              0.125,  # 25% / 2
    'Tx_Count_score':         0.125,  # 25% / 2

    # --- Dimension 3: Economic Output (25%) ---
    'Core_Utility_score':     0.125,  # 25% / 2
    'Fees_score':             0.125,  # 25% / 2 
    
    # --- Dimension 4: Systemic Sustainability (25%) ---
    'Revenue_score':          0.25    # 25% / 1
}

# 维度归属字典 (用于计算分维度得分，方便分析)
DIMENSIONS = {
    'D1_Capital': ['TVL_score', 'Capital_Turnover_score'],
    'D2_User':    ['DAU_score', 'Tx_Count_score'],
    'D3_Output':  ['Core_Utility_score', 'Fees_score'],
    'D4_Risk':    ['Revenue_score']
}

# ==========================================
# 3. 计算指数 (Aggregation)
# ==========================================

if not os.path.exists(INPUT_FILE):
    raise FileNotFoundError(f"输入文件缺失: {INPUT_FILE}\n请先运行 Step 5 (normalizer.py)！")

print(f"正在读取数据: {INPUT_FILE} ...")
df = pd.read_csv(INPUT_FILE)

# 3.1 检查列名是否存在
missing_cols = [col for col in WEIGHTS.keys() if col not in df.columns]
if missing_cols:
    raise ValueError(f"数据集中缺少以下列: {missing_cols}\n请检查 normalizer.py 是否正确生成了这些列。")

print("正在应用权重并计算指数 ...")

# 3.2 计算总指数 (Composite Index)
# DAI = Σ (Weight_i * Score_i)
df['Index_Value'] = 0.0
for col, weight in WEIGHTS.items():
    df['Index_Value'] += df[col] * weight

# 3.3 计算各维度得分 (Sub-Indices)
# 这对论文分析非常重要：你可以说“虽然总分下降，但D2用户活跃度上升了”
for dim_name, cols in DIMENSIONS.items():
    # 维度得分 = (Σ (指标分 * 指标权重)) / 维度总权重 * 100
    # 但为了简单，直接求该维度下指标的平均值即可 (因为维度内是等权的)
    # 或者严格按照权重加总：
    
    dim_weight_sum = sum([WEIGHTS[c] for c in cols])
    
    df[dim_name] = 0.0
    for col in cols:
        # 这里需要归一化到 0-100 的维度分
        # 公式: (指标分 * 指标权重) / 维度总权重
        df[dim_name] += (df[col] * WEIGHTS[col]) / dim_weight_sum

# 保留两位小数
cols_to_round = ['Index_Value'] + list(DIMENSIONS.keys())
df[cols_to_round] = df[cols_to_round].round(2)

# ==========================================
# 4. 结果验证与保存
# ==========================================

print("\n=== 指数计算预览 (前5行) ===")
# 只展示 Date, Protocol, Index, 和 4个维度分
preview_cols = ['Date', 'Protocol', 'Index_Value'] + list(DIMENSIONS.keys())
print(df[preview_cols].head())

print("\n=== 排名预览 (最新日期 Top 5) ===")
latest_date = df['Date'].max()
top_protocols = df[df['Date'] == latest_date].sort_values('Index_Value', ascending=False).head(5)
print(f"日期: {latest_date}")
print(top_protocols[['Protocol', 'Index_Value']])

# 保存
df.to_csv(OUTPUT_FILE, index=False)
print(f"\n最终指数文件已保存至: {OUTPUT_FILE}")
print(f"包含列: {list(df.columns)}")