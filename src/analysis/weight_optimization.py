"""
Sensitivity Analysis & Weight Optimization
===========================================
Tests different weighting schemes (Equal, Fundamental, PCA) for the DAI composite index 
to maximize its construct validity and predictive power (Granger Causality).
"""

import pandas as pd
import numpy as np
from scipy import stats
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.stattools import adfuller, grangercausalitytests
import warnings
from pathlib import Path

# Suppress warnings for cleaner console output
warnings.filterwarnings('ignore')

# The 5 core dimensions in your dataset
DIMENSIONS = [
    'D1_Capital', 
    'D2_Liquidity', 
    'D3_User_Activity', 
    'D4_Operational_Output', 
    'D5_Financial'
]

def get_project_root():
    """Get project root directory."""
    current_file = Path(__file__).resolve()
    return current_file.parent.parent.parent


def load_data(project_root):
    """Load the merged dataset containing dimension scores and market cap."""
    data_path = project_root / 'data' / 'analysis' / 'final_index_with_mcap.csv'
    
    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found: {data_path}")
        
    df = pd.read_csv(data_path)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by=['protocol', 'date']).reset_index(drop=True)
    
    # Ensure mcap_log exists for Pearson correlation
    df['mcap_log'] = np.log10(df['mcap'])
    
    print(f"[1] Loaded dataset: {len(df)} records across {len(df['protocol'].unique())} protocols")
    return df


def calculate_pca_weights(df):
    """Use Principal Component Analysis (PCA) to derive data-driven weights."""
    print("\n[2] Calculating PCA Data-Driven Weights...")
    
    # Drop NAs to fit PCA
    clean_data = df[DIMENSIONS].dropna()
    
    # Standardize the data before PCA
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(clean_data)
    
    # Fit PCA
    pca = PCA(n_components=1)
    pca.fit(scaled_data)
    
    # Extract absolute loadings from the first principal component
    loadings = np.abs(pca.components_[0])
    
    # Normalize so weights sum to 1
    pca_weights = loadings / np.sum(loadings)
    
    print("    PCA Weights Extracted Successfully:")
    for dim, weight in zip(DIMENSIONS, pca_weights):
        print(f"    - {dim}: {weight:.4f}")
        
    return pca_weights


def generate_new_indices(df, pca_weights):
    """Calculate new composite indices based on 3 different weighting schemes."""
    print("\n[3] Generating 3 Versions of DAI...")
    
    # Scheme 1: Equal Weights (Baseline)
    weights_equal = [0.2, 0.2, 0.2, 0.2, 0.2]
    
    # Scheme 2: Fundamental Weights (Heavy on Activity & Financials)
    weights_fund = [0.05, 0.15, 0.45, 0.05, 0.30]
    
    # Scheme 3: PCA Weights
    weights_pca = pca_weights
    
    # Calculate indices using dot product
    df['dai_equal'] = df[DIMENSIONS].dot(weights_equal)
    df['dai_fund'] = df[DIMENSIONS].dot(weights_fund)
    df['dai_pca'] = df[DIMENSIONS].dot(weights_pca)
    
    print("    [Done] Added 'dai_equal', 'dai_fund', and 'dai_pca' to dataset.")
    return df


def evaluate_construct_validity(df):
    """Evaluate Pearson & Spearman correlation for all 3 schemes."""
    print("\n" + "-" * 60)
    print("Phase 1: Construct Validity (Correlation with Market Cap)")
    print("-" * 60)
    
    schemes = ['dai_equal', 'dai_fund', 'dai_pca', 'tvl_score']
    results = []
    
    for scheme in schemes:
        clean_df = df[[scheme, 'mcap_log']].dropna()
        pearson_r, _ = stats.pearsonr(clean_df[scheme], clean_df['mcap_log'])
        spearman_rho, _ = stats.spearmanr(clean_df[scheme], clean_df['mcap_log'])
        
        results.append({
            'Scheme': scheme.upper(),
            'Pearson (r)': pearson_r,
            'Spearman (rho)': spearman_rho
        })
        
    res_df = pd.DataFrame(results).set_index('Scheme')
    print(res_df.round(4).to_string())
    return res_df


def make_stationary(series):
    """Helper to apply first difference if series is non-stationary."""
    clean_series = series.dropna()
    if len(clean_series) < 10:
        return series
    p_value = adfuller(clean_series)[1]
    return series.diff() if p_value > 0.05 else series


def evaluate_predictive_power(df, max_lag=7):
    """Run Granger causality for all schemes and count significant predictions."""
    print("\n" + "-" * 60)
    print(f"Phase 2: Predictive Power (Granger Causality, Max Lag={max_lag})")
    print("-" * 60)
    
    schemes = ['dai_equal', 'dai_fund', 'dai_pca', 'tvl_score']
    
    # Track how many protocols each scheme successfully predicts
    success_counts = {scheme: 0 for scheme in schemes}
    
    # Iterate over each protocol
    for protocol in df['protocol'].unique():
        proto_df = df[df['protocol'] == protocol].copy().set_index('date')
        
        # Calculate stationary target (Y)
        proto_df['mcap_return'] = np.log(proto_df['mcap'] / proto_df['mcap'].shift(1))
        
        # Test each scheme (X) against Y
        for scheme in schemes:
            proto_df[f'{scheme}_diff'] = make_stationary(proto_df[scheme])
            test_data = proto_df[['mcap_return', f'{scheme}_diff']].dropna()
            
            if len(test_data) < max_lag * 3:
                continue
                
            try:
                gc_res = grangercausalitytests(test_data, maxlag=max_lag, verbose=False)
                # Find the minimum p-value across all lags
                min_p_val = min([gc_res[lag][0]['ssr_ftest'][1] for lag in range(1, max_lag + 1)])
                
                # If p < 0.05, it counts as a successful prediction
                if min_p_val < 0.05:
                    success_counts[scheme] += 1
            except:
                pass
                
    # Print results summary
    print(f"{'Weighting Scheme':<20} | {'Protocols Predicted (p < 0.05)':<30}")
    print("-" * 55)
    for scheme, count in success_counts.items():
        total = len(df['protocol'].unique())
        print(f"{scheme.upper():<20} | {count} / {total}")
    print("-" * 55)
    
    return success_counts


def run_optimization():
    """Main execution pipeline."""
    project_root = get_project_root()
    
    print("=" * 65)
    print("Weight Optimization & Sensitivity Analysis")
    print("=" * 65)
    
    # 1. Load data
    df = load_data(project_root)
    
    # 2. Derive PCA weights
    pca_weights = calculate_pca_weights(df)
    
    # 3. Generate new indices
    df = generate_new_indices(df, pca_weights)
    
    # 4. Phase 1: Test correlation
    evaluate_construct_validity(df)
    
    # 5. Phase 2: Test predictive power
    success_counts = evaluate_predictive_power(df)
    
    # 6. Final Conclusion
    best_scheme = max(success_counts, key=success_counts.get)
    print(f"-> The strongest predictive model is: {best_scheme.upper()}")


if __name__ == "__main__":
    run_optimization()