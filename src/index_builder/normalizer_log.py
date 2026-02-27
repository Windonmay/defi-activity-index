import pandas as pd
import numpy as np
import os

# ==========================================
# 1. 路径与配置
# ==========================================
CURRENT_SCRIPT = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_SCRIPT)))

INPUT_FILE = os.path.join(PROJECT_ROOT, 'data', 'processed', 'clean_dataset_final.csv')
# 为了区分，我们保存为 normalized_log.csv，aggregator 读取这个文件
OUTPUT_FILE = os.path.join(PROJECT_ROOT, 'data', 'processed', 'normalized_log.csv')

# 定义需要处理的指标
METRICS = ['TVL', 'Fees', 'Revenue', 'DAU', 'Tx_Count', 'Core_Utility', 'Capital_Turnover']

# ⚠️ 关键设置：哪些指标需要取对数？
# 通常 "数量级" 指标需要取对数。 "比率" (Turnover) 通常不需要，但如果方差极大也可以取。
# 这里建议全部取对数，或者除了 Turnover 以外都取。
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
# 3. 对数转换 (Log Transformation) - 解决 Compound 低分问题的核心
# ==========================================
print("正在应用对数转换 (Log1p) 以压缩长尾数据 ...")

# 创建一个副本
df_processed = df.copy()

for metric in LOG_METRICS:
    # np.log1p 计算 log(x + 1)，避免 x=0 时报错
    # 这样 100万 和 1万 的差距会从 100倍 变成 1.5倍左右
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
# 5. Min-Max 标准化 (0-100)
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
# 6. 保存
# ==========================================
cols_to_keep = ['Date', 'Protocol'] + [f"{m}_score" for m in METRICS]
df_norm[cols_to_keep].to_csv(OUTPUT_FILE, index=False)
print(f"\n对数标准化完成！文件已保存至: {OUTPUT_FILE}")