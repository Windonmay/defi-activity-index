"""
Predictive Power Analysis (Granger Causality)
Evaluates whether the DAI composite index acts as a leading indicator 
for protocol market cap, compared to TVL.
"""

import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import adfuller, grangercausalitytests
import json
import warnings
from pathlib import Path

# Suppress statsmodels warnings for cleaner console output
warnings.filterwarnings('ignore')


def get_project_root():
    """Get project root directory."""
    current_file = Path(__file__).resolve()
    return current_file.parent.parent.parent


def load_merged_data(project_root):
    """Load the final dataset containing both index scores and market cap."""
    data_path = project_root / 'data' / 'analysis' / 'final_index_with_mcap.csv'
    
    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found: {data_path}. Run construct_validity.py first.")
        
    df = pd.read_csv(data_path)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by=['protocol', 'date']).reset_index(drop=True)
    
    print(f"[1] Loaded final_index_with_mcap.csv")
    print(f"    Records: {len(df)}")
    print(f"    Protocols: {df['protocol'].unique().tolist()}")
    
    return df


def test_stationarity_and_difference(series, variable_name):
    """
    Perform Augmented Dickey-Fuller (ADF) test.
    If non-stationary (p > 0.05), return the first difference.
    """
    # Drop NaNs just for the test
    clean_series = series.dropna()
    if len(clean_series) < 10:
        return series
        
    result = adfuller(clean_series)
    p_value = result[1]
    
    if p_value > 0.05:
        # Non-stationary, apply first difference
        return series.diff()
    else:
        # Stationary, return as is
        return series


def prepare_stationary_data(df):
    """Ensure all time series are stationary for Granger Causality."""
    print("\n[2] Preparing Stationary Time Series (ADF Test & Differencing)")
    stationary_dfs = []
    
    for protocol in df['protocol'].unique():
        proto_df = df[df['protocol'] == protocol].copy()
        proto_df = proto_df.set_index('date')
        
        # 1. Market Cap: Use Log Returns (standard financial practice)
        proto_df['mcap_return'] = np.log(proto_df['mcap'] / proto_df['mcap'].shift(1))
        
        # 2. DAI Index: First difference if non-stationary
        proto_df['dai_diff'] = test_stationarity_and_difference(
            proto_df['composite_index'], 'DAI'
        )
        
        # 3. TVL Score: First difference if non-stationary
        proto_df['tvl_diff'] = test_stationarity_and_difference(
            proto_df['tvl_score'], 'TVL'
        )
        
        # Drop rows with NaN (caused by diff/shift)
        proto_df = proto_df.dropna(subset=['mcap_return', 'dai_diff', 'tvl_diff'])
        stationary_dfs.append(proto_df.reset_index())
        
    stationary_df = pd.concat(stationary_dfs, ignore_index=True)
    print(f"    Processed {len(stationary_df)} stationary records.")
    return stationary_df


def run_granger_for_protocol(proto_df, protocol_name, max_lag=7):
    """Run Granger Causality tests for a specific protocol."""
    results = {
        'protocol': protocol_name,
        'DAI_predicts_MCAP': {},
        'TVL_predicts_MCAP': {}
    }
    
    # Target (Y) must be the first column, Predictor (X) is the second column
    data_dai = proto_df[['mcap_return', 'dai_diff']]
    data_tvl = proto_df[['mcap_return', 'tvl_diff']]
    
    try:
        # Test: Does DAI -> Market Cap?
        gc_dai = grangercausalitytests(data_dai, maxlag=max_lag, verbose=False)
        for lag in range(1, max_lag + 1):
            # Using SSR based F-test p-value
            p_val = gc_dai[lag][0]['ssr_ftest'][1]
            results['DAI_predicts_MCAP'][f'lag_{lag}'] = p_val
            
        # Test: Does TVL -> Market Cap?
        gc_tvl = grangercausalitytests(data_tvl, maxlag=max_lag, verbose=False)
        for lag in range(1, max_lag + 1):
            p_val = gc_tvl[lag][0]['ssr_ftest'][1]
            results['TVL_predicts_MCAP'][f'lag_{lag}'] = p_val
            
    except Exception as e:
        print(f"    [WARN] Granger test failed for {protocol_name}: {e}")
        
    return results


def run_all_granger_tests(stationary_df, max_lag=7):
    """Iterate through all protocols and run tests."""
    print(f"\nGranger Causality Analysis (Max Lag = {max_lag} days)")
    
    all_results = []
    summary_data = []

    for protocol in stationary_df['protocol'].unique():
        proto_df = stationary_df[stationary_df['protocol'] == protocol]
        
        res = run_granger_for_protocol(proto_df, protocol, max_lag)
        all_results.append(res)
        
        # Extract best (minimum) p-value across all lags to see if ANY lag predicts
        if res['DAI_predicts_MCAP'] and res['TVL_predicts_MCAP']:
            min_p_dai = min(res['DAI_predicts_MCAP'].values())
            min_p_tvl = min(res['TVL_predicts_MCAP'].values())
            
            # Find which lag gave the best prediction
            best_lag_dai = min(res['DAI_predicts_MCAP'], key=res['DAI_predicts_MCAP'].get)
            best_lag_tvl = min(res['TVL_predicts_MCAP'], key=res['TVL_predicts_MCAP'].get)
            
            summary_data.append({
                'protocol': protocol,
                'DAI_best_p': min_p_dai,
                'DAI_best_lag': best_lag_dai.replace('lag_', ''),
                'TVL_best_p': min_p_tvl,
                'TVL_best_lag': best_lag_tvl.replace('lag_', ''),
                'DAI_Significant': min_p_dai < 0.05,
                'TVL_Significant': min_p_tvl < 0.05
            })

    summary_df = pd.DataFrame(summary_data)

    # Print clean summary table
    print(f"\n{'Protocol':<15} | {'DAI -> MCAP (p-val)':<20} | {'TVL -> MCAP (p-val)':<20}")
    print(f"{'-' * 65}")
    for _, row in summary_df.iterrows():
        sig_dai = "***" if row['DAI_Significant'] else ""
        sig_tvl = "***" if row['TVL_Significant'] else ""

        dai_str = f"{row['DAI_best_p']:.4f} (L{row['DAI_best_lag']}){sig_dai}"
        tvl_str = f"{row['TVL_best_p']:.4f} (L{row['TVL_best_lag']}){sig_tvl}"

        print(f"{row['protocol']:<15} | {dai_str:<20} | {tvl_str:<20}")
    print("Note: *** indicates statistical significance (p < 0.05)")

    return all_results, summary_df


def save_granger_results(all_results, summary_df, project_root):
    """Save Granger test results to JSON and CSV."""
    output_dir = project_root / 'data' / 'analysis'
    
    # Save detailed JSON
    json_path = output_dir / 'granger_causality_full.json'
    with open(json_path, 'w') as f:
        json.dump(all_results, f, indent=4)
        
    # Save summary CSV
    csv_path = output_dir / 'granger_causality_summary.csv'
    summary_df.to_csv(csv_path, index=False)
    
    print("\n[Output Files]")
    print(f"  - {json_path.name}: Full p-values for all lags")
    print(f"  - {csv_path.name}: Best lag summary per protocol")


def run_analysis():
    """Main execution pipeline."""
    project_root = get_project_root()

    print("\nPredictive Power Analysis (Granger Causality)")
    
    # 1. Load Data
    df = load_merged_data(project_root)
    
    # 2. Make data stationary (Log Returns & Differencing)
    stationary_df = prepare_stationary_data(df)
    
    # 3. Run Granger Causality
    # Testing up to 7 days of lag (one week)
    all_results, summary_df = run_all_granger_tests(stationary_df, max_lag=7)
    
    # 4. Save Results
    save_granger_results(all_results, summary_df, project_root)

    # 5. Conclusion Printout
    dai_wins = sum(summary_df['DAI_Significant'])
    tvl_wins = sum(summary_df['TVL_Significant'])

    print(f"\nPredictive Power Summary:")
    print(f"  DAI predicts MCAP: {dai_wins} / {len(summary_df)} protocols")
    print(f"  TVL predicts MCAP: {tvl_wins} / {len(summary_df)} protocols")


if __name__ == "__main__":
    run_analysis()