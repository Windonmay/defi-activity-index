"""
Construct Validity Analysis
Validates whether the DAI composite index has better correlation with market cap than TVL alone.
"""

import pandas as pd
import numpy as np
from scipy import stats
import json
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')

def get_project_root():
    """Get project root directory."""
    current_file = Path(__file__).resolve()
    return current_file.parent.parent.parent

def load_final_index(project_root):
    """Load the final composite index data."""
    index_path = project_root / 'data' / 'final' / 'final_index_5dim.csv'

    if not index_path.exists():
        raise FileNotFoundError(f"Index file not found: {index_path}")

    df = pd.read_csv(index_path)
    print(f"[1] Loaded final_index_5dim.csv")
    print(f"    Records: {len(df)}")
    print(f"    Protocols: {df['protocol'].unique().tolist()}")
    print(f"    Date range: {df['date'].min()} to {df['date'].max()}")

    return df

def load_market_cap_data(project_root):
    """Load and merge market cap data for all protocols."""
    mcap_dir = project_root / 'data' / 'raw' / 'mcap'

    mcap_files = {
        'aave_v3': 'aave_v3_mcap.csv',
        'compound_v3': 'compound_v3_mcap.csv',
        'curve': 'curve_mcap.csv',
        'lido': 'lido_mcap.csv',
        'makerdao': 'makerdao_mcap.csv',
        'uniswap_v3': 'uniswap_v3_mcap.csv'
    }

    mcap_dfs = []

    for protocol, filename in mcap_files.items():
        filepath = mcap_dir / filename

        if not filepath.exists():
            print(f"[WARN] Market cap file not found: {filepath}")
            continue

        df = pd.read_csv(filepath)

        # Find market cap column
        col_name = [c for c in df.columns if 'market cap' in c.lower()][0]

        df = df.rename(columns={
            'Date': 'date',
            col_name: f'{protocol}_mcap'
        })

        df['date'] = pd.to_datetime(df['date']).dt.normalize()

        df[f'{protocol}_mcap'] = pd.to_numeric(
            df[f'{protocol}_mcap'].astype(str).str.replace(r'[$,]', '', regex=True), 
            errors='coerce'
        )

        mcap_dfs.append(df[['date', f'{protocol}_mcap']])
        print(f"[2] Loaded {filename}: {len(df)} records")

    # Merge all market cap data
    mcap_combined = mcap_dfs[0]
    for df in mcap_dfs[1:]:
        mcap_combined = pd.merge(mcap_combined, df, on='date', how='outer')

    mcap_combined = mcap_combined.sort_values('date').set_index('date')

    mcap_combined = mcap_combined.interpolate(method='linear')
    mcap_combined = mcap_combined.ffill().bfill()

    mcap_combined = mcap_combined.reset_index()
    mcap_combined['date'] = mcap_combined['date'].dt.strftime('%Y-%m-%d')

    print(f"[3] Merged market cap data: {len(mcap_combined.columns)} columns")

    # Convert to long format 
    mcap_long = mcap_combined.melt(
        id_vars=['date'],
        var_name='protocol_mcap',
        value_name='mcap'
    )
    mcap_long['protocol'] = mcap_long['protocol_mcap'].str.replace('_mcap', '')
    mcap_long = mcap_long[['date', 'protocol', 'mcap']]

    print(f"[4] Market cap data (long format): {len(mcap_long)} records")

    return mcap_long


def merge_and_save_data(index_df, mcap_long, project_root):
    """Merge index and market cap data, save combined dataset."""
    merged_df = pd.merge(index_df, mcap_long, on=['date', 'protocol'], how='inner')

    print(f"[5] Merged index + market cap: {len(merged_df)} records")
    print(f"    Date range: {merged_df['date'].min()} to {merged_df['date'].max()}")

    # Save merged data
    output_dir = project_root / 'data' / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / 'final_index_with_mcap.csv'
    merged_df.to_csv(output_path, index=False)
    print(f"[6] Saved merged data: {output_path.name}")

    return merged_df


def global_correlation_analysis(analysis_df):
    """Compute global correlation: DAI vs Market Cap vs TVL vs Market Cap."""
    print("\n" + "-" * 50)
    print("Global Correlation Analysis")
    print("-" * 50)

    # Pearson correlation
    pearson_dai, pearson_dai_p = stats.pearsonr(
        analysis_df['composite_index'], analysis_df['mcap_log']
    )
    pearson_tvl, pearson_tvl_p = stats.pearsonr(
        analysis_df['tvl_score'], analysis_df['mcap_log']
    )

    # Spearman correlation
    spearman_dai, spearman_dai_p = stats.spearmanr(
        analysis_df['composite_index'], analysis_df['mcap_log']
    )
    spearman_tvl, spearman_tvl_p = stats.spearmanr(
        analysis_df['tvl_score'], analysis_df['mcap_log']
    )

    # Print results
    print("\n" + "=" * 70)
    print("Core Result: DAI vs TVL Correlation with Market Cap")
    print("=" * 70)
    print(f"\n{'Metric':<25} {'DAI':<20} {'TVL':<20} {'DAI Advantage':<15}")
    print("-" * 70)
    print(f"{'Pearson r':<20} {pearson_dai:>18.4f} {pearson_tvl:>18.4f} {pearson_dai - pearson_tvl:>+14.4f}")
    print(f"{'Spearman rho':<20} {spearman_dai:>18.4f} {spearman_tvl:>18.4f} {spearman_dai - spearman_tvl:>+14.4f}")
    print("-" * 70)

    # Significance levels
    print(f"\nStatistical Significance:")
    print(f"DAI Pearson:  r = {pearson_dai:.4f}, p = {pearson_dai_p:.2e}")
    print(f"DAI Spearman: rho = {spearman_dai:.4f}, p = {spearman_dai_p:.2e}")
    print(f"TVL Pearson:  r = {pearson_tvl:.4f}, p = {pearson_tvl_p:.2e}")
    print(f"TVL Spearman: rho = {spearman_tvl:.4f}, p = {spearman_tvl_p:.2e}")

    return {
        'pearson_dai': pearson_dai,
        'pearson_dai_p': pearson_dai_p,
        'spearman_dai': spearman_dai,
        'spearman_dai_p': spearman_dai_p,
        'pearson_tvl': pearson_tvl,
        'pearson_tvl_p': pearson_tvl_p,
        'spearman_tvl': spearman_tvl,
        'spearman_tvl_p': spearman_tvl_p,
        'pearson_advantage': pearson_dai - pearson_tvl,
        'spearman_advantage': spearman_dai - spearman_tvl
    }


def protocol_correlation_analysis(analysis_df):
    """Compute correlation by protocol."""
    print("\n" + "-" * 50)
    print("Correlation by Protocol")
    print("-" * 50)

    protocol_results = []

    for protocol in analysis_df['protocol'].unique():
        proto_df = analysis_df[analysis_df['protocol'] == protocol]

        if len(proto_df) < 10:
            continue

        try:
            r_dai, p_dai = stats.pearsonr(proto_df['composite_index'], proto_df['mcap_log'])
            rho_dai, sp_dai = stats.spearmanr(proto_df['composite_index'], proto_df['mcap_log'])
        except:
            r_dai, p_dai, rho_dai, sp_dai = np.nan, np.nan, np.nan, np.nan

        try:
            r_tvl, p_tvl = stats.pearsonr(proto_df['tvl_score'], proto_df['mcap_log'])
            rho_tvl, sp_tvl = stats.spearmanr(proto_df['tvl_score'], proto_df['mcap_log'])
        except:
            r_tvl, p_tvl, rho_tvl, sp_tvl = np.nan, np.nan, np.nan, np.nan

        protocol_results.append({
            'protocol': protocol,
            'n_obs': len(proto_df),
            'r_dai': r_dai,
            'p_dai': p_dai,
            'rho_dai': rho_dai,
            'r_tvl': r_tvl,
            'p_tvl': p_tvl,
            'rho_tvl': rho_tvl,
            'advantage': r_dai - r_tvl if not np.isnan(r_dai) and not np.isnan(r_tvl) else np.nan
        })

    results_df = pd.DataFrame(protocol_results)
    results_df = results_df.sort_values('advantage', ascending=False)

    print(f"\n{'Protocol':<15} {'N':<6} {'r(DAI)':<12} {'r(TVL)':<12} {'Advantage':<10}")
    print("-" * 55)
    for _, row in results_df.iterrows():
        sig_dai = '*' if row['p_dai'] < 0.05 else ''
        sig_tvl = '*' if row['p_tvl'] < 0.05 else ''
        print(f"{row['protocol']:<15} {row['n_obs']:<6} {row['r_dai']:>10.4f}{sig_dai:<2} "
              f"{row['r_tvl']:>10.4f}{sig_tvl:<2} {row['advantage']:>+8.4f}")
    print("-" * 55)

    return results_df


def dimension_correlation_analysis(analysis_df):
    """Analyze correlation for each dimension with market cap."""
    print("\n" + "-" * 50)
    print("Correlation by Dimension")
    print("-" * 50)

    dimensions = [
        'tvl_score', 'fees_score', 'revenue_score', 'dau_score', 'tx_count_score',
        'core_utility_score', 'liquidity_metric_score',
        'D1_Capital', 'D2_Liquidity', 'D3_User_Activity', 'D4_Operational_Output', 'D5_Financial'
    ]

    dim_results = []

    for dim in dimensions:
        if dim not in analysis_df.columns:
            continue

        try:
            r, p = stats.pearsonr(analysis_df[dim], analysis_df['mcap_log'])
            rho, sp = stats.spearmanr(analysis_df[dim], analysis_df['mcap_log'])
            dim_results.append({
                'dimension': dim,
                'r': r,
                'p': p,
                'rho': rho,
                'sp': sp
            })
        except:
            pass

    results_df = pd.DataFrame(dim_results).sort_values('r', ascending=False)

    print(f"\n{'Dimension':<25} {'r':<12} {'rho':<12}")
    print("-" * 50)
    for _, row in results_df.iterrows():
        sig = '*' if row['p'] < 0.05 else ''
        print(f"{row['dimension']:<25} {row['r']:>10.4f}{sig:<2} {row['rho']:>10.4f}")
    print("-" * 50)

    return results_df


def save_results(global_results, proto_df, dim_df, analysis_df, project_root):
    """Save analysis results to files."""
    output_dir = project_root / 'data' / 'analysis'

    # Save JSON summary
    results_summary = {
        'global_correlation': global_results,
        'n_observations': len(analysis_df),
        'protocols': analysis_df['protocol'].unique().tolist(),
        'date_range': [str(analysis_df['date'].min()), str(analysis_df['date'].max())]
    }

    json_path = output_dir / 'construct_validity_results.json'
    with open(json_path, 'w') as f:
        json.dump(results_summary, f, indent=2)

    # Save protocol results
    proto_path = output_dir / 'protocol_correlation_results.csv'
    proto_df.to_csv(proto_path, index=False)

    # Save dimension results
    dim_path = output_dir / 'dimension_correlation_results.csv'
    dim_df.to_csv(dim_path, index=False)

    print("\n[Output Files]")
    print(f"  - {json_path.name}: Global correlation results")
    print(f"  - {proto_path.name}: Per-protocol results")
    print(f"  - {dim_path.name}: Per-dimension results")


def run_analysis():
    """Main analysis pipeline."""
    project_root = get_project_root()

    print("=" * 60)
    print("Construct Validity Analysis")
    print("=" * 60)

    # Load data
    index_df = load_final_index(project_root)
    mcap_long = load_market_cap_data(project_root)
    merged_df = merge_and_save_data(index_df, mcap_long, project_root)

    # Prepare analysis dataset
    analysis_df = merged_df[(merged_df['mcap'] > 0) & (merged_df['mcap'].notna())].copy()
    analysis_df['mcap_log'] = np.log10(analysis_df['mcap'])

    print(f"\n[Data Preparation]")
    print(f"  Valid records: {len(analysis_df)}")
    print(f"  Protocols: {analysis_df['protocol'].unique().tolist()}")

    # Analysis stages
    global_results = global_correlation_analysis(analysis_df)
    proto_df = protocol_correlation_analysis(analysis_df)
    dim_df = dimension_correlation_analysis(analysis_df)

    # Conclusion
    print("\n" + "=" * 60)
    print("Construct Validity Conclusion")
    print("=" * 60)
    print(f"""
Hypothesis Test:
H0: DAI-market cap correlation <= TVL-market cap correlation
H1: DAI-market cap correlation > TVL-market cap correlation

Results:
1. Global Pearson: DAI = {global_results['pearson_dai']:.4f}, TVL = {global_results['pearson_tvl']:.4f}
   DAI advantage: {global_results['pearson_advantage']:+.4f}

2. Global Spearman: DAI = {global_results['spearman_dai']:.4f}, TVL = {global_results['spearman_tvl']:.4f}
   DAI advantage: {global_results['spearman_advantage']:+.4f}

Conclusion:
- DAI shows {'better' if global_results['pearson_advantage'] > 0 else 'weaker'} correlation with market cap than TVL alone
- This {'validates' if global_results['pearson_advantage'] > 0 else 'questions'} the structural validity of our composite index
""")

    # Save results
    save_results(global_results, proto_df, dim_df, analysis_df, project_root)

    print("\n" + "=" * 60)
    print("Analysis Complete")
    print("=" * 60)


if __name__ == "__main__":
    run_analysis()
"""
Construct Validity Analysis
Validates whether the DAI composite index has better correlation with market cap than TVL alone.
"""

import pandas as pd
import numpy as np
from scipy import stats
import json
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')


def get_project_root():
    """Get project root directory."""
    current_file = Path(__file__).resolve()
    return current_file.parent.parent.parent


def load_final_index(project_root):
    """Load the final composite index data."""
    index_path = project_root / 'data' / 'final' / 'final_index_5dim.csv'

    if not index_path.exists():
        raise FileNotFoundError(f"Index file not found: {index_path}")

    df = pd.read_csv(index_path)
    print(f"[1] Loaded final_index_5dim.csv")
    print(f"    Records: {len(df)}")
    print(f"    Protocols: {df['protocol'].unique().tolist()}")
    print(f"    Date range: {df['date'].min()} to {df['date'].max()}")

    return df


def load_market_cap_data(project_root):
    """Load and merge market cap data for all protocols."""
    mcap_dir = project_root / 'data' / 'raw' / 'mcap'

    mcap_files = {
        'aave_v3': 'aave_v3_mcap.csv',
        'compound_v3': 'compound_v3_mcap.csv',
        'curve': 'curve_mcap.csv',
        'lido': 'lido_mcap.csv',
        'makerdao': 'makerdao_mcap.csv',
        'uniswap_v3': 'uniswap_v3_mcap.csv'
    }

    mcap_dfs = []

    for protocol, filename in mcap_files.items():
        filepath = mcap_dir / filename

        if not filepath.exists():
            print(f"[WARN] Market cap file not found: {filepath}")
            continue

        df = pd.read_csv(filepath)

        # Find market cap column
        col_name = [c for c in df.columns if 'market cap' in c.lower()][0]

        df = df.rename(columns={
            'Date': 'date',
            col_name: f'{protocol}_mcap'
        })

        mcap_dfs.append(df[['date', f'{protocol}_mcap']])
        print(f"[2] Loaded {filename}: {len(df)} records")

    # Merge all market cap data
    mcap_combined = mcap_dfs[0]
    for df in mcap_dfs[1:]:
        mcap_combined = pd.merge(mcap_combined, df, on='date', how='outer')

    print(f"[3] Merged market cap data: {len(mcap_combined.columns)} columns")

    # Convert to long format
    mcap_long = mcap_combined.melt(
        id_vars=['date'],
        var_name='protocol_mcap',
        value_name='mcap'
    )
    mcap_long['protocol'] = mcap_long['protocol_mcap'].str.replace('_mcap', '')
    mcap_long = mcap_long[['date', 'protocol', 'mcap']]

    print(f"[4] Market cap data (long format): {len(mcap_long)} records")

    return mcap_long


def merge_and_save_data(index_df, mcap_long, project_root):
    """Merge index and market cap data, save combined dataset."""
    merged_df = pd.merge(index_df, mcap_long, on=['date', 'protocol'], how='inner')

    print(f"[5] Merged index + market cap: {len(merged_df)} records")
    print(f"    Date range: {merged_df['date'].min()} to {merged_df['date'].max()}")

    # Save merged data
    output_dir = project_root / 'data' / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / 'final_index_with_mcap.csv'
    merged_df.to_csv(output_path, index=False)
    print(f"[6] Saved merged data: {output_path.name}")

    return merged_df


def global_correlation_analysis(analysis_df):
    """Compute global correlation: DAI vs Market Cap vs TVL vs Market Cap."""
    print("\n" + "-" * 50)
    print("Global Correlation Analysis")
    print("-" * 50)

    # Pearson correlation
    pearson_dai, pearson_dai_p = stats.pearsonr(
        analysis_df['composite_index'], analysis_df['mcap_log']
    )
    pearson_tvl, pearson_tvl_p = stats.pearsonr(
        analysis_df['tvl_score'], analysis_df['mcap_log']
    )

    # Spearman correlation
    spearman_dai, spearman_dai_p = stats.spearmanr(
        analysis_df['composite_index'], analysis_df['mcap_log']
    )
    spearman_tvl, spearman_tvl_p = stats.spearmanr(
        analysis_df['tvl_score'], analysis_df['mcap_log']
    )

    # Print results
    print("\n" + "=" * 70)
    print("Core Result: DAI vs TVL Correlation with Market Cap")
    print("=" * 70)
    print(f"\n{'Metric':<25} {'DAI':<20} {'TVL':<20} {'DAI Advantage':<15}")
    print("-" * 70)
    print(f"{'Pearson r':<20} {pearson_dai:>18.4f} {pearson_tvl:>18.4f} {pearson_dai - pearson_tvl:>+14.4f}")
    print(f"{'Spearman rho':<20} {spearman_dai:>18.4f} {spearman_tvl:>18.4f} {spearman_dai - spearman_tvl:>+14.4f}")
    print("-" * 70)

    # Significance levels
    print(f"\nStatistical Significance:")
    print(f"DAI Pearson:  r = {pearson_dai:.4f}, p = {pearson_dai_p:.2e}")
    print(f"DAI Spearman: rho = {spearman_dai:.4f}, p = {spearman_dai_p:.2e}")
    print(f"TVL Pearson:  r = {pearson_tvl:.4f}, p = {pearson_tvl_p:.2e}")
    print(f"TVL Spearman: rho = {spearman_tvl:.4f}, p = {spearman_tvl_p:.2e}")

    return {
        'pearson_dai': pearson_dai,
        'pearson_dai_p': pearson_dai_p,
        'spearman_dai': spearman_dai,
        'spearman_dai_p': spearman_dai_p,
        'pearson_tvl': pearson_tvl,
        'pearson_tvl_p': pearson_tvl_p,
        'spearman_tvl': spearman_tvl,
        'spearman_tvl_p': spearman_tvl_p,
        'pearson_advantage': pearson_dai - pearson_tvl,
        'spearman_advantage': spearman_dai - spearman_tvl
    }


def protocol_correlation_analysis(analysis_df):
    """Compute correlation by protocol."""
    print("\n" + "-" * 50)
    print("Correlation by Protocol")
    print("-" * 50)

    protocol_results = []

    for protocol in analysis_df['protocol'].unique():
        proto_df = analysis_df[analysis_df['protocol'] == protocol]

        if len(proto_df) < 10:
            continue

        try:
            r_dai, p_dai = stats.pearsonr(proto_df['composite_index'], proto_df['mcap_log'])
            rho_dai, sp_dai = stats.spearmanr(proto_df['composite_index'], proto_df['mcap_log'])
        except:
            r_dai, p_dai, rho_dai, sp_dai = np.nan, np.nan, np.nan, np.nan

        try:
            r_tvl, p_tvl = stats.pearsonr(proto_df['tvl_score'], proto_df['mcap_log'])
            rho_tvl, sp_tvl = stats.spearmanr(proto_df['tvl_score'], proto_df['mcap_log'])
        except:
            r_tvl, p_tvl, rho_tvl, sp_tvl = np.nan, np.nan, np.nan, np.nan

        protocol_results.append({
            'protocol': protocol,
            'n_obs': len(proto_df),
            'r_dai': r_dai,
            'p_dai': p_dai,
            'rho_dai': rho_dai,
            'r_tvl': r_tvl,
            'p_tvl': p_tvl,
            'rho_tvl': rho_tvl,
            'advantage': r_dai - r_tvl if not np.isnan(r_dai) and not np.isnan(r_tvl) else np.nan
        })

    results_df = pd.DataFrame(protocol_results)
    results_df = results_df.sort_values('advantage', ascending=False)

    print(f"\n{'Protocol':<15} {'N':<6} {'r(DAI)':<12} {'r(TVL)':<12} {'Advantage':<10}")
    print("-" * 55)
    for _, row in results_df.iterrows():
        sig_dai = '*' if row['p_dai'] < 0.05 else ''
        sig_tvl = '*' if row['p_tvl'] < 0.05 else ''
        print(f"{row['protocol']:<15} {row['n_obs']:<6} {row['r_dai']:>10.4f}{sig_dai:<2} "
              f"{row['r_tvl']:>10.4f}{sig_tvl:<2} {row['advantage']:>+8.4f}")
    print("-" * 55)

    return results_df


def dimension_correlation_analysis(analysis_df):
    """Analyze correlation for each dimension with market cap."""
    print("\n" + "-" * 50)
    print("Correlation by Dimension")
    print("-" * 50)

    dimensions = [
        'tvl_score', 'fees_score', 'revenue_score', 'dau_score', 'tx_count_score',
        'core_utility_score', 'liquidity_metric_score',
        'D1_Capital', 'D2_Liquidity', 'D3_User_Activity', 'D4_Operational_Output', 'D5_Financial'
    ]

    dim_results = []

    for dim in dimensions:
        if dim not in analysis_df.columns:
            continue

        try:
            r, p = stats.pearsonr(analysis_df[dim], analysis_df['mcap_log'])
            rho, sp = stats.spearmanr(analysis_df[dim], analysis_df['mcap_log'])
            dim_results.append({
                'dimension': dim,
                'r': r,
                'p': p,
                'rho': rho,
                'sp': sp
            })
        except:
            pass

    results_df = pd.DataFrame(dim_results).sort_values('r', ascending=False)

    print(f"\n{'Dimension':<25} {'r':<12} {'rho':<12}")
    print("-" * 50)
    for _, row in results_df.iterrows():
        sig = '*' if row['p'] < 0.05 else ''
        print(f"{row['dimension']:<25} {row['r']:>10.4f}{sig:<2} {row['rho']:>10.4f}")
    print("-" * 50)

    return results_df


def save_results(global_results, proto_df, dim_df, analysis_df, project_root):
    """Save analysis results to files."""
    output_dir = project_root / 'data' / 'analysis'

    # Save JSON summary
    results_summary = {
        'global_correlation': global_results,
        'n_observations': len(analysis_df),
        'protocols': analysis_df['protocol'].unique().tolist(),
        'date_range': [str(analysis_df['date'].min()), str(analysis_df['date'].max())]
    }

    json_path = output_dir / 'construct_validity_results.json'
    with open(json_path, 'w') as f:
        json.dump(results_summary, f, indent=2)

    # Save protocol results
    proto_path = output_dir / 'protocol_correlation_results.csv'
    proto_df.to_csv(proto_path, index=False)

    # Save dimension results
    dim_path = output_dir / 'dimension_correlation_results.csv'
    dim_df.to_csv(dim_path, index=False)

    print("\n[Output Files]")
    print(f"  - {json_path.name}: Global correlation results")
    print(f"  - {proto_path.name}: Per-protocol results")
    print(f"  - {dim_path.name}: Per-dimension results")


def run_analysis():
    """Main analysis pipeline."""
    project_root = get_project_root()

    print("=" * 60)
    print("Construct Validity Analysis")
    print("=" * 60)

    # Load data
    index_df = load_final_index(project_root)
    mcap_long = load_market_cap_data(project_root)
    merged_df = merge_and_save_data(index_df, mcap_long, project_root)

    # Prepare analysis dataset
    analysis_df = merged_df[(merged_df['mcap'] > 0) & (merged_df['mcap'].notna())].copy()
    analysis_df['mcap_log'] = np.log10(analysis_df['mcap'])

    print(f"\n[Data Preparation]")
    print(f"  Valid records: {len(analysis_df)}")
    print(f"  Protocols: {analysis_df['protocol'].unique().tolist()}")

    # Analysis stages
    global_results = global_correlation_analysis(analysis_df)
    proto_df = protocol_correlation_analysis(analysis_df)
    dim_df = dimension_correlation_analysis(analysis_df)

    # Conclusion
    print("\n" + "=" * 60)
    print("Construct Validity Conclusion")
    print("=" * 60)
    print(f"""
Hypothesis Test:
H0: DAI-market cap correlation <= TVL-market cap correlation
H1: DAI-market cap correlation > TVL-market cap correlation

Results:
1. Global Pearson: DAI = {global_results['pearson_dai']:.4f}, TVL = {global_results['pearson_tvl']:.4f}
   DAI advantage: {global_results['pearson_advantage']:+.4f}

2. Global Spearman: DAI = {global_results['spearman_dai']:.4f}, TVL = {global_results['spearman_tvl']:.4f}
   DAI advantage: {global_results['spearman_advantage']:+.4f}

Conclusion:
- DAI shows {'better' if global_results['pearson_advantage'] > 0 else 'weaker'} correlation with market cap than TVL alone
- This {'validates' if global_results['pearson_advantage'] > 0 else 'questions'} the structural validity of our composite index
""")

    # Save results
    save_results(global_results, proto_df, dim_df, analysis_df, project_root)

    print("\n" + "=" * 60)
    print("Analysis Complete")
    print("=" * 60)


if __name__ == "__main__":
    run_analysis()
