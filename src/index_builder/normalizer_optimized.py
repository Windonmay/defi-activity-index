"""
normalizer_optimized.py
Normalization module for DeFi Activity Index with 5-dimension framework.

Normalizes raw metrics to [0, 100] scale using log transformation + Min-Max scaling.
Includes smoothing and handles the new unified Liquidity dimension.

5-Dimension Framework (v2.1):
- D1: Capital (TVL) - log transform + minmax
- D2: Liquidity (Liquidity = Operational Output / TVL) - minmax only
- D3: User Activity (DAU, Tx Count) - log transform + minmax
- D4: Operational Output (Core Utility) - log transform + minmax
- D5: Financial Performance (Fees, Revenue) - log transform + minmax
"""

import pandas as pd
import numpy as np
from pathlib import Path


class OptimizedNormalizer:
    """
    Normalize indicators using optimized log-transformation method.

    Normalization Process (v2.1 Unified Framework):
    1. Log transformation: log(1 + x) applied to skewed absolute metrics
       - Applied to: TVL, Fees, Revenue, DAU, Tx_Count, Core_Utility
       - NOT applied to: Liquidity (already a ratio = Operational Output / TVL)

    2. Smoothing: 7-day rolling average to reduce noise

    3. Scaling:
       - Min-Max: Maps values to [0, 100] range (primary)
       - Z-Score: Standardizes to mean=0, std=1 (robustness check)

    Protocol-Specific Liquidity Metrics:
    - DEX (Uniswap, Curve): Trading Volume / TVL
    - Lending (Aave, Compound): Total Borrowed / TVL
    - Stablecoin (MakerDAO): Circulating Supply / TVL
    - LSD (Lido): Net Inflow / TVL

    Parameters
    ----------
    input_path : str
        Path to clean_dataset_final.csv
    output_dir : str
        Directory to save normalized data
    method : str
        'minmax' (primary) or 'zscore' (backup for sensitivity tests)
    """

    def __init__(self, input_path, output_dir, method='minmax'):
        self.input_path = Path(input_path)
        self.output_dir = Path(output_dir)
        self.method = method

        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Define which metrics need log transformation
        # Log transform is applied to absolute values (USD, counts)
        self.log_metrics = [
            'tvl', 'fees', 'revenue', 'dau', 'tx_count', 'core_utility'
        ]

        # Define liquidity metrics (no log transformation needed)
        # These are already ratios: Liquidity = Operational Output / TVL
        # Protocol-specific liquidity metrics (v2.1 unified framework):
        # - DEX: Trading Liquidity Utilization = Trading Volume / TVL
        # - Lending: Borrow Utilization Ratio = Total Borrowed / TVL
        # - Stablecoin: Capital Deployment Ratio = Circulating Supply / TVL
        # - LSD: Staking Liquidity Flow = Net Inflow / TVL
        self.liquidity_metrics = [
            'liquidity_metric'  # Unified liquidity = Operational Output / TVL
        ]

        # All metrics to normalize
        self.all_metrics = self.log_metrics + self.liquidity_metrics


    def apply_log_transform(self, df, column):
        """
        Apply log(1+x) transformation to compress right-skewed distribution.

        Parameters
        ----------
        df : pd.DataFrame
            Input data
        column : str
            Column name to transform

        Returns
        -------
        pd.Series
            Log-transformed values
        """
        return np.log1p(df[column])


    def apply_smoothing(self, series, window=7):
        """
        Apply rolling average smoothing to reduce noise.

        Parameters
        ----------
        series : pd.Series
            Time series data
        window : int
            Rolling window size (default: 7 days)

        Returns
        -------
        pd.Series
            Smoothed series
        """
        return series.rolling(window=window, min_periods=1).mean()


    def normalize_minmax(self, df, column):
        """
        Apply Min-Max scaling to [0, 100] range.

        Parameters
        ----------
        df : pd.DataFrame
            Input data (should have the column)
        column : str
            Column name to scale

        Returns
        -------
        pd.Series
            Min-Max scaled values
        """
        col_data = df[column].copy()

        min_val = col_data.min()
        max_val = col_data.max()

        if max_val == min_val:
            # Avoid division by zero - return uniform values
            return pd.Series([50.0] * len(col_data), index=col_data.index)

        scaled = (col_data - min_val) / (max_val - min_val) * 100
        return scaled


    def normalize_zscore(self, df, column):
        """
        Apply Z-Score standardization to mean=0, std=1.

        Parameters
        ----------
        df : pd.DataFrame
            Input data
        column : str
            Column name to standardize

        Returns
        -------
        pd.Series
            Z-score standardized values
        """
        mean_val = df[column].mean()
        std_val = df[column].std()

        if std_val == 0:
            # Avoid division by zero
            return pd.Series([0.0] * len(df[column]), index=df[column].index)

        return (df[column] - mean_val) / std_val


    def normalize_with_log(self, exclude_cols=None):
        """
        Main normalization pipeline.

        Parameters
        ----------
        exclude_cols : list
            Columns to exclude from normalization (e.g., ['date', 'protocol'])

        Returns
        -------
        pd.DataFrame
            Normalized dataset with _score suffix
        """
        if exclude_cols is None:
            exclude_cols = ['date', 'protocol']

        print(f"Loading data: {self.input_path}")
        df = pd.read_csv(self.input_path)
        df['date'] = pd.to_datetime(df['date'])

        print(f"Normalization method: {self.method.upper()}")
        print(f"Metrics to normalize: {len(self.all_metrics)}")

        # Create output dataframe
        output_df = df[['date', 'protocol']].copy()

        for metric in self.all_metrics:
            if metric not in df.columns:
                print(f"  Warning: {metric} not found in data, skipping")
                continue

            print(f"  Processing: {metric}")

            # Step 1: Log transform (for specific metrics)
            if metric in self.log_metrics:
                working_col = f"{metric}_log"
                output_df[working_col] = self.apply_log_transform(df, metric)
            else:
                working_col = metric
                output_df[working_col] = df[metric]

            # Step 2: Smoothing
            output_df[working_col] = output_df.groupby('protocol')[working_col].transform(
                lambda x: self.apply_smoothing(x, window=7)
            )

            # Step 3: Scale
            if self.method == 'minmax':
                output_df[f"{metric}_score"] = self.normalize_minmax(output_df, working_col)
            elif self.method == 'zscore':
                output_df[f"{metric}_score"] = self.normalize_zscore(output_df, working_col)

        # Summary statistics
        print("\n=== Normalization Summary ===")
        score_cols = [c for c in output_df.columns if c.endswith('_score')]
        print(f"Generated {len(score_cols)} score columns")

        for col in score_cols:
            print(f"  {col}: mean={output_df[col].mean():.2f}, "
                  f"std={output_df[col].std():.2f}, "
                  f"min={output_df[col].min():.2f}, "
                  f"max={output_df[col].max():.2f}")

        return output_df


# Standalone execution

if __name__ == "__main__":
    from pathlib import Path

    CURRENT_SCRIPT = Path(__file__).resolve()
    PROJECT_ROOT = CURRENT_SCRIPT.parent.parent.parent

    INPUT_FILE = PROJECT_ROOT / 'data' / 'processed' / 'clean_dataset_final.csv'
    OUTPUT_DIR = PROJECT_ROOT / 'data' / 'processed'

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file missing: {INPUT_FILE}")

    # Min-Max Normalization (Primary)
    print("=" * 60)
    print("Method A: Min-Max Normalization")
    print("=" * 60)

    normalizer_mm = OptimizedNormalizer(
        input_path=str(INPUT_FILE),
        output_dir=str(OUTPUT_DIR),
        method='minmax'
    )
    df_mm = normalizer_mm.normalize_with_log()

    output_mm = OUTPUT_DIR / 'normalized_minmax_log.csv'
    df_mm.to_csv(output_mm, index=False)
    print(f"\n  Saved: {output_mm}")

    # Z-Score Normalization (Backup)
    print("\n" + "=" * 60)
    print("Method B: Z-Score Normalization (Backup)")
    print("=" * 60)

    normalizer_zs = OptimizedNormalizer(
        input_path=str(INPUT_FILE),
        output_dir=str(OUTPUT_DIR),
        method='zscore'
    )
    df_zs = normalizer_zs.normalize_with_log()

    output_zs = OUTPUT_DIR / 'normalized_zscore_log.csv'
    df_zs.to_csv(output_zs, index=False)
    print(f"\n  Saved: {output_zs}")
