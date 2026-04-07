"""
aggregator_optimized.py
Index aggregation with optimized 5-dimension weighting scheme.

Key Change from v1:
- Separated Capital and Liquidity into independent dimensions
- This avoids mathematical correlation: Liquidity = Throughput/TVL
- When combined in same dimension: TVL ↑ → TVL Score ↑, Liquidity Score ↓
  (assuming constant Throughput) → Signal cancellation

5-Dimension Framework (Equal Weighting: 20% each):
----------------------------------------------------
D1: Capital Scale (TVL)
  - TVL: 20% (scale only, no efficiency metrics)

D2: Liquidity (Capital Utilization Efficiency)
  - Liquidity = Operational Output / TVL
  - Protocol-specific metric selection:
    * Lending (Aave, Compound): Borrow Utilization Ratio = Total Borrowed / TVL
    * DEX (Uniswap, Curve): Trading Liquidity Utilization = Trading Volume / TVL
    * Stablecoin (MakerDAO): Capital Deployment Ratio = Circulating Supply / TVL
    * LSD (Lido): Staking Liquidity Flow = Net Inflow / TVL
  - Weight: 20%

D3: User Activity
  - DAU: 10%
  - Tx Count: 10%
  - Total: 20%

D4: Operational Output (Core Utility)
  - Core Utility: 20%
  - Protocol-specific mapping:
    * DEX: Trading Volume
    * Lending: Total Borrowed
    * LSD: Net Inflow (changed from Assets Staked)
    * Stablecoin: Circulating Supply

D5: Financial Performance
  - Total Fees: 10%
  - Protocol Revenue: 10%
  - Total: 20%
"""

import pandas as pd
import numpy as np
from pathlib import Path


class OptimizedIndexAggregator:
    """
    Build composite index using optimized 5-dimension weights.

    Parameters
    ----------
    input_path : str
        Path to normalized_minmax_log.csv
    output_dir : str
        Directory to save final index
    """

    def __init__(self, input_path, output_dir):
        self.input_path = Path(input_path)
        self.output_dir = Path(output_dir)

        # 5-Dimension Equal Weighting Scheme (20% each)
        self.weights = {
            # D1: Capital Scale (20%)
            'tvl_score': 0.20,

            # D2: Liquidity (20%) - Liquidity = Operational Output / TVL
            'liquidity_metric_score': 0.20,

            # D3: User Activity (20%)
            'dau_score': 0.10,
            'tx_count_score': 0.10,

            # D4: Operational Output (20%)
            'core_utility_score': 0.20,

            # D5: Financial Performance (20%)
            'fees_score': 0.10,
            'revenue_score': 0.10
        }

        # Dimension grouping for sub-score computation
        self.dimensions = {
            'D1_Capital': ['tvl_score'],
            'D2_Liquidity': ['liquidity_metric_score'],
            'D3_User_Activity': ['dau_score', 'tx_count_score'],
            'D4_Operational_Output': ['core_utility_score'],
            'D5_Financial': ['fees_score', 'revenue_score']
        }

        self.output_dir.mkdir(parents=True, exist_ok=True)


    def build_optimized_index(self):
        """
        Compute composite index and dimension scores.

        Returns
        -------
        pd.DataFrame
            Index with composite score and dimension breakdown
        """
        # Load normalized data
        df = pd.read_csv(self.input_path)
        df['date'] = pd.to_datetime(df['date'])

        # Validate columns
        required_cols = set(self.weights.keys())
        available_cols = set(df.columns)
        missing_cols = required_cols - available_cols

        if missing_cols:
            # Try with capitalized column names (e.g., TVL_score instead of tvl_score)
            df.columns = [c.lower() for c in df.columns]
            available_cols = set(df.columns)
            missing_cols = required_cols - available_cols

            if missing_cols:
                raise ValueError(f"Missing columns: {missing_cols}\nAvailable: {list(df.columns)}")

        print("Computing composite index (5-Dimension Framework)...")
        print(f"\nWeighting Scheme:")
        print(f"  D1 Capital:           {self.weights['tvl_score']*100:.0f}%")
        print(f"  D2 Liquidity:         {self.weights['liquidity_metric_score']*100:.0f}%")
        print(f"  D3 User Activity:     {(self.weights['dau_score']+self.weights['tx_count_score'])*100:.0f}%")
        print(f"  D4 Operational Output: {self.weights['core_utility_score']*100:.0f}%")
        print(f"  D5 Financial:         {(self.weights['fees_score']+self.weights['revenue_score'])*100:.0f}%")

        print("\nLiquidity Indicators (Liquidity = Operational Output / TVL):")
        print("  Lending (Aave, Compound): Borrow Utilization Ratio = Total Borrowed / TVL")
        print("  DEX (Uniswap, Curve): Trading Liquidity Utilization = Trading Volume / TVL")
        print("  Stablecoin (MakerDAO): Capital Deployment Ratio = Circulating Supply / TVL")
        print("  LSD (Lido): Staking Liquidity Flow = Net Inflow / TVL")

        # Compute composite index
        df['composite_index'] = 0.0
        for col, weight in self.weights.items():
            df['composite_index'] += df[col] * weight

        # Compute dimension scores (normalized within each dimension)
        for dim_name, cols in self.dimensions.items():
            dim_weight_sum = sum([self.weights[c] for c in cols])
            df[dim_name] = 0.0
            for col in cols:
                df[dim_name] += (df[col] * self.weights[col]) / dim_weight_sum

        # Round scores
        score_cols = ['composite_index'] + list(self.dimensions.keys())
        df[score_cols] = df[score_cols].round(4)

        # Move composite_index to the last column for better readability
        cols = [c for c in df.columns if c != 'composite_index'] + ['composite_index']
        df = df[cols]

        # Preview
        print("\n=== Index Preview (First 5 rows) ===")
        preview_cols = ['date', 'protocol', 'composite_index'] + list(self.dimensions.keys())
        print(df[preview_cols].head())

        print("\n=== Latest Ranking ===")
        latest_date = df['date'].max()
        top_protocols = df[df['date'] == latest_date].sort_values(
            'composite_index', ascending=False
        )
        print(f"Date: {latest_date}")
        print(top_protocols[['protocol', 'composite_index']].head(10).to_string(index=False))

        # Dimension breakdown for top protocols
        print("\n=== Dimension Breakdown (Latest) ===")
        dim_cols = ['protocol'] + list(self.dimensions.keys())
        print(top_protocols[dim_cols].head(10).to_string(index=False))

        return df


    def compute_correlation_analysis(self, df):
        """
        Verify that dimensions are not highly correlated.

        This validates that the 5-dimension separation avoids
        mathematical correlation issues.

        Parameters
        ----------
        df : pd.DataFrame
            Normalized data with dimension scores

        Returns
        -------
        pd.DataFrame
            Correlation matrix
        """
        print("\n=== Dimension Correlation Analysis ===")
        print("Checking for mathematical correlation (should be < 0.7)...")

        dim_cols = list(self.dimensions.keys())
        corr_matrix = df[dim_cols].corr()

        print("\nCorrelation Matrix:")
        print(corr_matrix.round(3))

        # Check for high correlations
        high_corr_pairs = []
        for i in range(len(dim_cols)):
            for j in range(i+1, len(dim_cols)):
                corr_val = corr_matrix.iloc[i, j]
                if abs(corr_val) > 0.7:
                    high_corr_pairs.append((dim_cols[i], dim_cols[j], corr_val))

        if high_corr_pairs:
            print("\nWARNING: High correlation detected:")
            for pair in high_corr_pairs:
                print(f"   {pair[0]} ↔ {pair[1]}: {pair[2]:.3f}")
        else:
            print("\n✓ No high correlations detected (all < 0.7)")

        return corr_matrix


# Standalone execution

if __name__ == "__main__":
    CURRENT_SCRIPT = Path(__file__).resolve()
    PROJECT_ROOT = CURRENT_SCRIPT.parent.parent.parent

    INPUT_FILE = PROJECT_ROOT / 'data' / 'processed' / 'normalized_minmax_log.csv'
    OUTPUT_DIR = PROJECT_ROOT / 'data' / 'final'

    aggregator = OptimizedIndexAggregator(
        input_path=str(INPUT_FILE),
        output_dir=str(OUTPUT_DIR)
    )

    index_df = aggregator.build_optimized_index()

    # Verify dimension independence
    aggregator.compute_correlation_analysis(index_df)

    output_file = OUTPUT_DIR / 'final_index_5dim.csv'
    index_df.to_csv(output_file, index=False)
    print(f"\n✓ Saved: {output_file}")
