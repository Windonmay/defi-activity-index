import pandas as pd
import numpy as np
import os

# ==========================================
# 1. 路径与配置
# ==========================================
CURRENT_SCRIPT = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_SCRIPT)))

INPUT_FILE = os.path.join(PROJECT_ROOT, 'data', 'processed', 'clean_dataset_final.csv')

# 定义两个输出文件
OUTPUT_MINMAX = os.path.join(PROJECT_ROOT, 'data', 'processed', 'normalized_minmax.csv')
OUTPUT_ZSCORE = os.path.join(PROJECT_ROOT, 'data', 'processed', 'normalized_zscore.csv')

METRICS = [
    'TVL', 'Fees', 'Revenue', 'DAU', 'Tx_Count', 'Core_Utility', 'Capital_Turnover'
]

# ==========================================
# 2. 读取数据与平滑
# ==========================================
if not os.path.exists(INPUT_FILE):
    raise FileNotFoundError(f"输入文件缺失: {INPUT_FILE}")

print(f"正在读取数据: {INPUT_FILE} ...")
df = pd.read_csv(INPUT_FILE)
df['Date'] = pd.to_datetime(df['Date'])

# 平滑处理 (Common Step)
print("正在应用 7-Day Moving Average 平滑处理 ...")
df_smoothed = df.copy()
for metric in METRICS:
    smooth_col = f"{metric}_smooth"
    df_smoothed[smooth_col] = df.groupby('Protocol')[metric].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    )

# ==========================================
# 3. 方法 A: Min-Max Normalization (主模型)
# ==========================================
print("正在执行 Min-Max 标准化 (0-100) ...")
df_minmax = df_smoothed.copy()

for metric in METRICS:
    smooth_col = f"{metric}_smooth"
    score_col = f"{metric}_score"
    
    # 全局极值
    g_min = df_smoothed[smooth_col].min()
    g_max = df_smoothed[smooth_col].max()
    
    if g_max == g_min:
        df_minmax[score_col] = 0.0
    else:
        df_minmax[score_col] = ((df_smoothed[smooth_col] - g_min) / (g_max - g_min)) * 100
        
    df_minmax[score_col] = df_minmax[score_col].round(2)

# 保存 Min-Max 结果
cols_to_keep = ['Date', 'Protocol'] + [f"{m}_score" for m in METRICS]
df_minmax[cols_to_keep].to_csv(OUTPUT_MINMAX, index=False)
print(f"Min-Max 数据集已保存: {OUTPUT_MINMAX}")

# ==========================================
# 4. 方法 B: Z-Score Standardization (鲁棒性检验)
# ==========================================
print("正在执行 Z-Score 标准化 (Mean=0, Std=1) ...")
df_zscore = df_smoothed.copy()

for metric in METRICS:
    smooth_col = f"{metric}_smooth"
    score_col = f"{metric}_score" # 列名保持一致，方便后续复用加权代码
    
    # 全局均值与标准差
    g_mean = df_smoothed[smooth_col].mean()
    g_std = df_smoothed[smooth_col].std()
    
    if g_std == 0:
        df_zscore[score_col] = 0.0
    else:
        # Z-Score 公式
        df_zscore[score_col] = (df_smoothed[smooth_col] - g_mean) / g_std
    
    # Z-Score 保留3位小数
    df_zscore[score_col] = df_zscore[score_col].round(3)

# 保存 Z-Score 结果
df_zscore[cols_to_keep].to_csv(OUTPUT_ZSCORE, index=False)
print(f"Z-Score 数据集已保存: {OUTPUT_ZSCORE}")