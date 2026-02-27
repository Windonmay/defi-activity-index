import pandas as pd
import numpy as np
import os

# ==========================================
# 1. 路径与配置
# ==========================================
CURRENT_SCRIPT = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_SCRIPT)))

INPUT_FILE = os.path.join(PROJECT_ROOT, 'data', 'processed', 'clean_dataset_final.csv')
# 为了区分，我们保存为 normalized_log.csv，aggregator_optimized 读取这个文件
OUTPUT_FILE = os.path.join(PROJECT_ROOT, 'data', 'processed', 'normalized_minmax_log.csv')
# 新增：Z-Score 标准化输出文件
OUTPUT_ZSCORE = os.path.join(PROJECT_ROOT, 'data', 'processed', 'normalized_zscore_log.csv')

# 定义需要处理的指标
METRICS = ['TVL', 'Fees', 'Revenue', 'DAU', 'Tx_Count', 'Core_Utility', 'Capital_Turnover']

# 需要取对数的指标
LOG_METRICS = ['TVL', 'Fees', 'Revenue', 'DAU', 'Tx_Count', 'Core_Utility']

# ==========================================
# 2. 读取数据
# ==========================================
if not os.path.exists(INPUT_FILE):
    raise FileNotFoundError(f"输入文件缺失: {INPUT_FILE}")

print(f"正在读取数据: {INPUT_FILE} ...")
df = pd.read_csv(INPUT_FILE)
df['Date'] = pd.to_datetime(df['Date'])

# ==========================================
# 3. 对数转换 (Log Transformation) - 解决 Compound 的低分问题
# ==========================================
print("正在应用对数转换 (Log1p) 以压缩长尾数据 ...")

# 创建一个副本
df_processed = df.copy()

for metric in LOG_METRICS:
    # np.log1p 计算 log(x + 1)，避免 x=0 时报错
    df_processed[metric] = np.log1p(df_processed[metric])

# ==========================================
# 4. 平滑处理 (7-Day MA)
# ==========================================
print("正在应用 7-Day Moving Average ...")
# 对经过对数处理的数据进行平滑
for metric in METRICS:
    smooth_col = f"{metric}_smooth"
    df_processed[smooth_col] = df_processed.groupby('Protocol')[metric].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    )

# ==========================================
# 5. 方法 A: Min-Max 标准化 (0-100)
# ==========================================
print("正在进行 Min-Max 标准化 ...")
df_norm = df_processed.copy()

for metric in METRICS:
    smooth_col = f"{metric}_smooth"
    norm_col = f"{metric}_score"
    
    # 全局极值
    g_min = df_processed[smooth_col].min()
    g_max = df_processed[smooth_col].max()
    
    if g_max == g_min:
        df_norm[norm_col] = 0.0
    else:
        df_norm[norm_col] = ((df_processed[smooth_col] - g_min) / (g_max - g_min)) * 100
    
    df_norm[norm_col] = df_norm[norm_col].round(2)

# ==========================================
# 6. 方法 B: Z-Score Standardization (鲁棒性检验)
# ==========================================
print("正在执行 Z-Score 标准化 (Mean=0, Std=1) ...")
df_zscore = df_processed.copy()

for metric in METRICS:
    smooth_col = f"{metric}_smooth"
    score_col = f"{metric}_score"  # 列名保持一致，方便后续复用加权代码
    
    # 全局均值与标准差
    g_mean = df_processed[smooth_col].mean()
    g_std = df_processed[smooth_col].std()
    
    if g_std == 0:
        df_zscore[score_col] = 0.0
    else:
        # Z-Score 公式
        df_zscore[score_col] = (df_processed[smooth_col] - g_mean) / g_std
    
    # Z-Score 保留3位小数
    df_zscore[score_col] = df_zscore[score_col].round(3)

# ==========================================
# 7. 保存
# ==========================================
cols_to_keep = ['Date', 'Protocol'] + [f"{m}_score" for m in METRICS]

# 保存 Min-Max 结果
df_norm[cols_to_keep].to_csv(OUTPUT_FILE, index=False)
print(f"\nMin-Max 标准化完成！文件已保存至: {OUTPUT_FILE}")

# 保存 Z-Score 结果
df_zscore[cols_to_keep].to_csv(OUTPUT_ZSCORE, index=False)
print(f"Z-Score 标准化完成！文件已保存至: {OUTPUT_ZSCORE}")