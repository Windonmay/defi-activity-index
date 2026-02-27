import pandas as pd
import os
import numpy as np
import sys

# ==========================================
# 1. 配置与初始化
# ==========================================
# 获取当前脚本绝对路径: .../defi-activity-index/src/data_processor/cleaner.py
CURRENT_SCRIPT = os.path.abspath(__file__)

# 向上回退 3 级找到项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_SCRIPT)))

# 设定时间范围 (根据你的Specification)
start_date = '2025-02-26'
end_date = '2026-02-26'
date_range = pd.date_range(start=start_date, end=end_date, freq='D')

# 定义数据路径
API_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw', 'api')
MANUAL_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw', 'manual')
OUTPUT_PATH = os.path.join(PROJECT_ROOT, 'data', 'processed')

# 确保输出目录存在
os.makedirs(OUTPUT_PATH, exist_ok=True)

# 定义协议列表
protocols = ['aave_v3', 'compound_v3', 'uniswap_v3', 'curve', 'makerdao', 'lido']

# 定义标准列名（目标列名）
standard_metrics = ['tvl', 'fees', 'revenue', 'dau', 'tx_count', 'core_utility']

# ==========================================
# 2. 辅助函数：读取与标准化
# ==========================================

def read_and_standardize(file_path, value_col_name='value'):
    """
    读取CSV，确保索引为Datetime，并重命名数值列
    假设CSV格式为: date, value (或者类似)
    """
    if not os.path.exists(file_path):
        print(f"文件缺失: {file_path}")
        return None
    
    try:
        # 读取CSV，尝试解析日期。你需要根据实际CSV格式调整 'date' 列名
        # 假设第一列是日期，第二列是数值
        df = pd.read_csv(file_path)
        
        # 自动识别日期列（通常包含 date, time, day 等字眼）
        date_col = [c for c in df.columns if 'date' in c.lower() or 'day' in c.lower() or 'time' in c.lower()][0]
        # 自动识别数值列 (排除日期列后的第一列，或者根据名字)
        val_col = [c for c in df.columns if c != date_col][0]
        
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col)
        
        # 强制截取我们需要的时间段
        df = df.sort_index()
        
        # 重命名数值列
        df = df[[val_col]].rename(columns={val_col: value_col_name})
        
        # 去重（防止同一天有多条数据）
        df = df[~df.index.duplicated(keep='last')]
        
        return df
    except Exception as e:
        print(f"读取错误 {file_path}: {e}")
        return None

# ==========================================
# 3. 主处理循环
# ==========================================

all_data = []

print("开始处理数据...")

for protocol in protocols:
    print(f"\n正在处理协议: {protocol.upper()}...")
    
    # 创建一个空的DataFrame，索引为完整日期范围，确保时间对齐
    protocol_df = pd.DataFrame(index=date_range)
    protocol_df.index.name = 'Date'
    
    # -------------------------------------------------
    # 3.1 读取通用 API 数据 (TVL, Fees, Revenue)
    # -------------------------------------------------
    # 注意：这里假设你的文件名格式为 {protocol}_{metric}.csv
    # 如果你的文件名是 fees.csv 放在 aave 文件夹下，请修改路径为 f"{API_PATH}/{protocol}/fees.csv"
    
    # TVL
    path_tvl = f"{API_PATH}/tvl_{protocol}.csv" # 请根据实际文件名修改
    df_tvl = read_and_standardize(path_tvl, 'TVL')
    protocol_df = protocol_df.join(df_tvl)
    
    # Fees
    path_fees = f"{API_PATH}/fees_{protocol}.csv" 
    df_fees = read_and_standardize(path_fees, 'Fees')
    protocol_df = protocol_df.join(df_fees)
    
    # Revenue
    path_rev = f"{API_PATH}/revenue_{protocol}.csv"
    df_rev = read_and_standardize(path_rev, 'Revenue')
    protocol_df = protocol_df.join(df_rev)
    
    # -------------------------------------------------
    # 3.2 读取通用 Manual 数据 (DAU, Tx Count)
    # -------------------------------------------------
    
    # DAU
    path_dau = f"{MANUAL_PATH}/{protocol}_dau.csv"
    df_dau = read_and_standardize(path_dau, 'DAU')
    protocol_df = protocol_df.join(df_dau)
    
    # Tx Count
    path_tx = f"{MANUAL_PATH}/{protocol}_tx_count.csv"
    df_tx = read_and_standardize(path_tx, 'Tx_Count')
    protocol_df = protocol_df.join(df_tx)
    
    # -------------------------------------------------
    # 3.3 处理 Core Utility Metric (特殊映射)
    # -------------------------------------------------
    # 根据 Specification 中的 "Core Utility Metric" 逻辑进行映射
    
    utility_path = None
    
    if protocol in ['uniswap_v3', 'curve']:
        # DEX: Volume (来自 API 文件夹)
        utility_path = f"{API_PATH}/volume_{protocol}.csv"
        
    elif protocol in ['aave_v3', 'compound_v3']:
        # Lending: Active Loans / Total Borrowed (来自 Manual 文件夹)
        utility_path = f"{MANUAL_PATH}/{protocol}_active_loans.csv"
        
    elif protocol == 'lido':
        # Liquid Staking: Assets Staked (来自 Manual 文件夹)
        utility_path = f"{MANUAL_PATH}/lido_assets_staked.csv"
        
    elif protocol == 'makerdao':
        # Stablecoin: Circulating Supply (来自 Manual 文件夹)
        utility_path = f"{MANUAL_PATH}/makerdao_circulating_supply.csv"
    
    # 读取并合并 Core Utility
    if utility_path:
        df_util = read_and_standardize(utility_path, 'Core_Utility')
        protocol_df = protocol_df.join(df_util)
    else:
        print(f"⚠️ 未找到 {protocol} 的 Core Utility 定义路径")
        protocol_df['Core_Utility'] = np.nan

    # -------------------------------------------------
    # 3.4 缺失值处理 (Data Cleaning)
    # -------------------------------------------------
    
    # 策略 1: Forward Fill (ffill) - 新语法
    protocol_df = protocol_df.ffill()
    
    # 策略 2: Backward Fill (bfill) - 新语法
    protocol_df = protocol_df.bfill()
    
    # 策略 3: 剩余缺失值处理
    # 如果还有缺失 (通常是 Core Utility 在早期确实为0，或者数据源完全缺失)，填 0
    protocol_df = protocol_df.fillna(0)
    
    # 添加协议名称列
    protocol_df['Protocol'] = protocol
    
    # 重置索引，让 Date 变成一列
    protocol_df = protocol_df.reset_index()
    
    # 添加到总列表
    all_data.append(protocol_df)

# ==========================================
# 4. 合并与保存
# ==========================================

if all_data:
    # 纵向合并所有协议数据
    master_df = pd.concat(all_data, ignore_index=True)
    
    # 重新排列列顺序
    cols = ['Date', 'Protocol', 'TVL', 'Fees', 'Revenue', 'DAU', 'Tx_Count', 'Core_Utility']
    # 确保只包含存在的列
    cols = [c for c in cols if c in master_df.columns]
    master_df = master_df[cols]
    
    # 保留3位小数 (Core Utility)
    if 'Core_Utility' in master_df.columns:
        master_df['Core_Utility'] = master_df['Core_Utility'].round(3)
        
    # -------------------------------------------------
    # 4.1 单位检查 (关键步骤)
    # -------------------------------------------------
    # 提醒：你需要检查数据是否为 Wei (10^18)。如果 TVL 看起来像 10^20+，则需要除以 1e18
    # 这里我们打印概览供你检查
    print("\n数据概览 (请检查数值大小是否符合 USD 单位):")
    print(master_df.groupby('Protocol')[['TVL', 'Revenue', 'Core_Utility']].mean())
    
    # 保存结果
    output_file = f"{OUTPUT_PATH}/master_dataset_clean.csv"
    master_df.to_csv(output_file, index=False)
    
    print(f"\n成功! 清洗后的数据已保存至: {output_file}")
    print(f"总行数: {len(master_df)}")
    
else:
    print("未处理任何数据。")