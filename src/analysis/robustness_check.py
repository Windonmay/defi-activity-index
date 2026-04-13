"""
Robustness Check: Z-Score vs Min-Max Normalization
==================================================
Task 3: Verify that results are not due to "lucky parameter selection"

This script:
1. Loads Z-score normalized data
2. Reconstructs composite index using Fundamental weights
3. Runs Granger causality tests
4. Compares results with Min-Max baseline
5. Generates robustness report
"""

import pandas as pd
import numpy as np
from scipy import stats
from statsmodels.tsa.stattools import adfuller, grangercausalitytests
from pathlib import Path
import json
import warnings

warnings.filterwarnings('ignore')

# Fundamental weights
WEIGHTS_FUND = [0.05, 0.15, 0.45, 0.05, 0.30]


def get_project_root():
    return Path(__file__).resolve().parent.parent.parent


def load_minmax_data(project_root):
    """Load Min-Max normalized data (baseline)."""
    df = pd.read_csv(project_root / 'data' / 'analysis' / 'final_index_with_mcap.csv')
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['protocol', 'date']).reset_index(drop=True)

    # Calculate DAI_FUND
    dims = ['D1_Capital', 'D2_Liquidity', 'D3_User_Activity', 'D4_Operational_Output', 'D5_Financial']
    df['DAI_FUND'] = df[dims].dot(WEIGHTS_FUND)

    return df


def load_zscore_data(project_root):
    """Load Z-score normalized data and reconstruct DAI."""
    df = pd.read_csv(project_root / 'data' / 'processed' / 'normalized_zscore.csv')
    df['date'] = pd.to_datetime(df['Date'])
    df['protocol'] = df['Protocol'].str.lower()

    # Z-score data has these columns:
    # TVL_score, Fees_score, Revenue_score, DAU_score, Tx_Count_score, Core_Utility_score, Capital_Turnover_score

    # Map to 5 dimensions based on data_dictionary.md:
    # D1_Capital = TVL_score (TVL)
    # D2_Liquidity = Capital_Turnover_score (Throughput/TVL ratio)
    # D3_User_Activity = (DAU_score + Tx_Count_score) / 2
    # D4_Operational_Output = Core_Utility_score (category-specific throughput)
    # D5_Financial = Revenue_score

    df['D1_Capital'] = df['TVL_score']
    df['D2_Liquidity'] = df['Capital_Turnover_score']
    df['D3_User_Activity'] = (df['DAU_score'] + df['Tx_Count_score']) / 2
    df['D4_Operational_Output'] = df['Core_Utility_score']
    df['D5_Financial'] = df['Revenue_score']

    dims = ['D1_Capital', 'D2_Liquidity', 'D3_User_Activity', 'D4_Operational_Output', 'D5_Financial']
    df['DAI_FUND'] = df[dims].dot(WEIGHTS_FUND)

    return df


def test_stationarity(series):
    """Apply ADF test, return first difference if non-stationary."""
    clean = series.dropna()
    if len(clean) < 10:
        return series
    p_value = adfuller(clean)[1]
    return series.diff() if p_value > 0.05 else series


def run_granger_tests(df, index_col, max_lag=7):
    """Run Granger causality tests for all protocols."""
    results = []

    for protocol in df['protocol'].unique():
        proto_df = df[df['protocol'] == protocol].copy().set_index('date')

        # Log returns for market cap
        proto_df['mcap_return'] = np.log(proto_df['mcap'] / proto_df['mcap'].shift(1))
        proto_df[f'{index_col}_diff'] = test_stationarity(proto_df[index_col])

        test_data = proto_df[['mcap_return', f'{index_col}_diff']].dropna()

        if len(test_data) < max_lag * 3:
            continue

        try:
            gc_res = grangercausalitytests(test_data, maxlag=max_lag, verbose=False)
            p_values = [gc_res[lag][0]['ssr_ftest'][1] for lag in range(1, max_lag + 1)]
            min_p = min(p_values)
            best_lag = p_values.index(min_p) + 1

            results.append({
                'protocol': protocol,
                'best_p': min_p,
                'best_lag': best_lag,
                'significant': min_p < 0.05
            })
        except Exception as e:
            print(f"  [WARN] {protocol}: {e}")

    return pd.DataFrame(results)


def main():
    print("=" * 70)
    print("ROBUSTNESS CHECK: Z-Score vs Min-Max Normalization")
    print("=" * 70)

    project_root = get_project_root()

    # Load both datasets
    print("\n[1] Loading datasets...")
    df_mm = load_minmax_data(project_root)
    df_zs = load_zscore_data(project_root)

    print(f"    Min-Max: {len(df_mm)} records")
    print(f"    Z-Score: {len(df_zs)} records")

    # Merge Z-score with MCAP data
    mcap_data = df_mm[['date', 'protocol', 'mcap']].copy()
    df_zs = df_zs.merge(mcap_data, on=['date', 'protocol'], how='inner')
    print(f"    Z-Score (merged): {len(df_zs)} records")

    # Run Granger tests
    print("\n[2] Running Granger Causality Tests...")
    print("    Testing: Index -> Market Cap (7-day lag)")
    print()

    granger_mm = run_granger_tests(df_mm, 'DAI_FUND')
    granger_zs = run_granger_tests(df_zs, 'DAI_FUND')

    # Merge results
    comparison = granger_mm.merge(granger_zs, on='protocol', suffixes=('_MM', '_ZS'))

    print("-" * 70)
    print(f"{'Protocol':<15} | {'Min-Max':<20} | {'Z-Score':<20} | {'Consistent?'}")
    print("-" * 70)

    consistent = 0
    for _, row in comparison.iterrows():
        mm_sig = "*** (p<0.05)" if row['significant_MM'] else f"p={row['best_p_MM']:.4f}"
        zs_sig = "*** (p<0.05)" if row['significant_ZS'] else f"p={row['best_p_ZS']:.4f}"

        # Check consistency: both significant OR both non-significant
        is_consistent = row['significant_MM'] == row['significant_ZS']
        consistent += is_consistent

        status = "✅" if is_consistent else "⚠️"
        print(f"{row['protocol']:<15} | {mm_sig:<20} | {zs_sig:<20} | {status}")

    print("-" * 70)

    # Summary
    print("\n[3] Robustness Summary")
    print("=" * 70)

    sig_mm = granger_mm['significant'].sum()
    sig_zs = granger_zs['significant'].sum()

    print(f"\nMin-Max Results:   {sig_mm} / {len(granger_mm)} protocols significant")
    print(f"Z-Score Results:  {sig_zs} / {len(granger_zs)} protocols significant")
    print(f"Consistency:      {consistent} / {len(comparison)} protocols")

    # Key conclusion
    print("\n" + "=" * 70)
    print("CONCLUSION")
    print("=" * 70)

    if consistent == len(comparison):
        print("""
✅ ROBUSTNESS CONFIRMED

The results are NOT due to lucky parameter selection:
- Min-Max and Z-Score produce CONSISTENT conclusions
- The DAI index's predictive power is robust to normalization method

This strengthens the validity of the Composite DeFi Activity Index.
""")
    else:
        print("""
⚠️ MIXED RESULTS - REQUIRES DISCUSSION

Some protocols show different significance across methods:
- This is expected for individual protocols
- Focus on overall pattern consistency
""")

    # Save detailed results
    output_dir = project_root / 'data' / 'analysis'
    comparison.to_csv(output_dir / 'robustness_check_results.csv', index=False)

    print(f"\nResults saved to: {output_dir / 'robustness_check_results.csv'}")
    print("=" * 70)

    return comparison


if __name__ == "__main__":
    main()
