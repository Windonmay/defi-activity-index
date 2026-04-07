"""
main.py
Master pipeline for DeFi Activity Index construction.

This pipeline builds a composite DeFi Activity Index that captures protocol
vitality across five dimensions: Capital, Liquidity, User Activity,
Operational Output, and Financial Performance.

5-Dimension Framework (v2.1):
---------------------
D1: Capital Scale (20%) - TVL as baseline scale measure
D2: Liquidity (20%) - Liquidity = Operational Output / TVL
    * Lending: Borrow Utilization Ratio = Total Borrowed / TVL
    * DEX: Trading Liquidity Utilization = Trading Volume / TVL
    * Stablecoin: Capital Deployment Ratio = Circulating Supply / TVL
    * LSD: Staking Liquidity Flow = Net Inflow / TVL
D3: User Activity (20%) - DAU, Transaction Count
D4: Operational Output (20%) - Core Utility (Throughput)
    * DEX: Trading Volume
    * Lending: Total Borrowed
    * LSD: Net Inflow
    * Stablecoin: Circulating Supply
D5: Financial Performance (20%) - Fees, Revenue

Key Design Principle:
--------------------
Liquidity dimension is SEPARATED from Capital dimension to avoid
mathematical correlation:
- Liquidity = Operational Output / TVL
- When TVL increases: TVL Score increases, but Liquidity Score may decrease
- This creates mathematical negative correlation if combined in same dimension

Pipeline Stages:
    Stage 1: Data Collection (API + Manual data loading)
    Stage 2: Data Cleaning & Feature Engineering
    Stage 3: Normalization (Log-transformed + Min-Max/Z-Score)
    Stage 4: Index Construction (5-dimension optimized weighting)

Version: 2.1 (2026-04-02)
Author: [Your Name]
"""

import sys
import os
from pathlib import Path
from datetime import datetime


# PATH CONFIGURATION

# Get project root directory
current_file = Path(__file__).resolve()
project_root = current_file.parent

# Add project root to Python path for imports
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


# MODULE IMPORTS
print("=" * 70)
print("INITIALIZING MODULES")
print("=" * 70)

try:
    from src.data_loader.defillama import DeFiLlamaLoader
    print("  DeFiLlamaLoader Ready")

    from src.data_processor.cleaner import DataCleaner
    print("  DataCleaner Ready")

    from src.data_processor.feature_engineer import FeatureEngineer
    print("  FeatureEngineer Ready")

    from src.index_builder.normalizer_optimized import OptimizedNormalizer
    print("  OptimizedNormalizer Ready")

    from src.index_builder.aggregator_optimized import OptimizedIndexAggregator
    print("  OptimizedIndexAggregator Ready")

    print("\n  All modules imported successfully\n")

except ImportError as e:
    print(f"\nImportError: {e}")
    print(f"\nCurrent sys.path:")
    for p in sys.path[:5]:
        print(f"  - {p}")
    print("\nPlease ensure all modules exist in src/ directory.")
    sys.exit(1)


# DIRECTORY CONFIGURATION

# Define data directories
RAW_API_DIR = project_root / "data" / "raw" / "api"
RAW_MANUAL_DIR = project_root / "data" / "raw" / "manual"
PROCESSED_DIR = project_root / "data" / "processed"
FINAL_DIR = project_root / "data" / "final"

# Ensure output directories exist
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
FINAL_DIR.mkdir(parents=True, exist_ok=True)

# CONFIGURATION PARAMETERS

# Analysis time window
START_DATE = '2025-02-26'
END_DATE = '2026-02-26'

# Protocol list
PROTOCOLS = ['aave_v3', 'compound_v3', 'uniswap_v3', 'curve', 'makerdao', 'lido']

# 5-Dimension Weighting Scheme Documentation (v2.1)
WEIGHTING_SCHEME = """
5-Dimension Equal-Weight DeFi Activity Index (v2.1)
=====================================================

The composite index is constructed using equal weights across five dimensions (20% each).
This approach ensures structural neutrality and avoids mathematical correlation issues.

Dimension 1: Capital Scale (20%)
---------------------------------
  - TVL: 20%
  - Rationale: Baseline scale measure, captures absolute capital size

Dimension 2: Liquidity (20%)
-----------------------------
  - Unified Formula: Liquidity = Operational Output / TVL
  - Protocol-Specific Indicators:
    * Lending (Aave, Compound): Borrow Utilization Ratio = Total Borrowed / TVL
    * DEX (Uniswap, Curve): Trading Liquidity Utilization = Trading Volume / TVL
    * Stablecoin (MakerDAO): Capital Deployment Ratio = Circulating Supply / TVL
    * LSD (Lido): Staking Liquidity Flow = Net Inflow / TVL
  - Rationale: Capital utilization efficiency (flow/stock ratio)
  - CRITICAL: Separated from Capital to avoid Liquidity=Throughput/TVL correlation

Dimension 3: User Activity (20%)
---------------------------------
  - Daily Active Users (DAU): 10%
  - Transaction Count (Tx Count): 10%

Dimension 4: Operational Output (20%)
-------------------------------------
  - Core Utility Metric: 20%
  - Protocol-Specific Mapping:
    * DEX: Trading Volume
    * Lending: Total Borrowed (Active Loans)
    * LSD: Net Inflow
    * Stablecoin: Circulating Supply

Dimension 5: Financial Performance (20%)
----------------------------------------
  - Total Fees: 10%
  - Protocol Revenue: 10%

Total Weight = 100%

Why Separate Liquidity from Capital?
------------------------------------
Problem: Liquidity = Operational Output / TVL

If Liquidity and TVL are in the same dimension:
  - When TVL increases: TVL Score increases, but Liquidity Score decreases
    (assuming constant Operational Output)
  - This creates mathematical negative correlation
  - Result: Signal cancellation in weighted average

Solution: Separate into independent dimensions
  - D1 Capital: TVL only (scale)
  - D2 Liquidity: Operational Output / TVL (efficiency)
  - Each dimension weighted at 20%

Version History:
----------------
  - v1.0 (2026-02-26): Initial framework with 4 dimensions
  - v2.0 (2026-03-17): Updated to 5 dimensions; Defined Liquidity as Throughput/TVL
  - v2.1 (2026-04-02): Unified Liquidity = Operational Output / TVL for all protocols
"""


# HELPER FUNCTIONS

def print_stage_header(stage_num: int, stage_name: str) -> None:
    """Print formatted stage header."""
    print("\n" + "=" * 70)
    print(f"STAGE {stage_num}: {stage_name}")
    print("=" * 70)


def print_substep(step: str, total: int, description: str) -> None:
    """Print substep progress."""
    print(f"\n[{step}/{total}] {description}...")


def print_file_saved(filepath: Path, extra_info: str = "") -> None:
    """Print file save confirmation with metadata."""
    if filepath.exists():
        size_kb = filepath.stat().st_size // 1024
        print(f"    Saved: {filepath.name} ({size_kb} KB) {extra_info}")
    else:
        print(f"    File not created: {filepath.name}")


def print_dataframe_info(df, name: str) -> None:
    """Print basic DataFrame information."""
    print(f"\n    {name}:")
    print(f"      Shape: {df.shape[0]:,} rows x {df.shape[1]} columns")
    print(f"      Columns: {list(df.columns)}")
    if 'date' in df.columns:
        print(f"      Date range: {df['date'].min()} to {df['date'].max()}")
    if 'protocol' in df.columns:
        print(f"      Protocols: {sorted(df['protocol'].unique())}")


def check_raw_data_exists() -> dict:
    """
    Check which raw data files exist.

    Returns
    -------
    dict
        Summary of data availability
    """
    api_files = list(RAW_API_DIR.glob("*.csv")) if RAW_API_DIR.exists() else []
    manual_files = list(RAW_MANUAL_DIR.glob("*.csv")) if RAW_MANUAL_DIR.exists() else []

    return {
        'api_count': len(api_files),
        'manual_count': len(manual_files),
        'api_files': [f.name for f in api_files],
        'manual_files': [f.name for f in manual_files]
    }


# PIPELINE STAGES

def stage_1_data_collection(skip_api: bool = False) -> None:
    """
    Stage 1: Fetch raw data from DeFiLlama API.

    This stage collects:
    - TVL (Total Value Locked) for all protocols
    - Trading volume for DEXes (Uniswap, Curve)
    - Protocol fees and revenue
    - Stablecoin supply data (MakerDAO)

    Parameters
    ----------
    skip_api : bool
        If True, skip API calls and use existing raw data.
        Useful for development or when API rate limits apply.

    Outputs
    -------
    data/raw/api/*.csv : Raw time-series data from DeFiLlama
    """
    print_stage_header(1, "DATA COLLECTION")

    # Check existing data
    data_status = check_raw_data_exists()
    print(f"\nExisting raw data:")
    print(f"  API files: {data_status['api_count']}")
    print(f"  Manual files: {data_status['manual_count']}")

    if skip_api:
        print("\n  Skipping API data collection (skip_api=True)")
        print(f"  Using existing data from:")
        print(f"    - {RAW_API_DIR}")
        print(f"    - {RAW_MANUAL_DIR}")

        # Validate critical files exist
        critical_files = [
            RAW_API_DIR / "tvl_aave_v3.csv",
            RAW_API_DIR / "tvl_uniswap_v3.csv",
            RAW_MANUAL_DIR / "aave_v3_dau.csv"
        ]
        missing = [f.name for f in critical_files if not f.exists()]
        if missing:
            print(f"\n  WARNING: Critical files missing: {missing}")
            print("    Consider running with skip_api=False")

        print("\n  Stage 1 complete (using cached data)")
        return

    try:
        loader = DeFiLlamaLoader()

        print_substep("1", "5", "Fetching TVL data for all protocols")
        loader.run_tvl_batch_job()

        print_substep("2", "5", "Fetching DEX volume data (Uniswap, Curve)")
        loader.run_dex_volume_batch_job()

        print_substep("3", "5", "Fetching protocol fees data")
        loader.run_fees_batch_job()

        print_substep("4", "5", "Fetching protocol revenue data")
        loader.run_revenue_batch_job()

        print_substep("5", "5", "Fetching stablecoin supply data (MakerDAO)")
        loader.run_stablecoin_raw_data_job()

        # Verify outputs
        data_status_after = check_raw_data_exists()
        print(f"\n  Data collected:")
        print(f"    API files: {data_status_after['api_count']}")

        print("\n  Stage 1 complete: All API data fetched")

    except Exception as e:
        print(f"\n  Error in Stage 1: {e}")
        print("    Check your network connection and API availability.")
        raise


def stage_2_data_cleaning() -> None:
    """
    Stage 2: Clean raw data and engineer features.

    This stage performs:
    1. Load and merge all raw data sources (API + manual exports)
    2. Standardize date formats and column names
    3. Handle missing values (forward-fill, backward-fill, then zero-fill)
    4. Compute Liquidity metrics:
       - Utilization Rate = Active Loans / TVL (Lending protocols)
       - Turnover Ratio = Core_Utility / TVL (DEX, LSD protocols)
       - Revenue Efficiency = Revenue / TVL (Stablecoin protocols)

    Outputs
    -------
    data/processed/master_dataset_clean.csv : Merged raw data
    data/processed/clean_dataset_final.csv : With engineered features
    """
    print_stage_header(2, "DATA CLEANING & FEATURE ENGINEERING")

    try:
        # Step 2.1: Data Cleaning
        print_substep("1", "2", "Merging and cleaning raw data sources")

        cleaner = DataCleaner(
            raw_api_dir=str(RAW_API_DIR),
            raw_manual_dir=str(RAW_MANUAL_DIR),
            output_dir=str(PROCESSED_DIR),
            start_date=START_DATE,
            end_date=END_DATE
        )

        master_df = cleaner.create_master_dataset()

        # Save intermediate result
        master_output = PROCESSED_DIR / "master_dataset_clean.csv"
        master_df.to_csv(master_output, index=False)
        print_file_saved(master_output)
        print_dataframe_info(master_df, "Master Dataset")

        # Step 2.2: Feature Engineering

        print_substep("2", "2", "Engineering Liquidity features")
        print("  Computing:")
        print("    - Utilization Rate (Lending protocols)")
        print("    - Turnover Ratio (DEX protocols)")
        print("    - Revenue Efficiency (LSD, Stablecoin protocols)")

        engineer = FeatureEngineer()
        final_df = engineer.add_features(master_df)

        # Save final cleaned dataset
        final_output = PROCESSED_DIR / "clean_dataset_final.csv"
        final_df.to_csv(final_output, index=False)
        print_file_saved(final_output)
        print_dataframe_info(final_df, "Final Clean Dataset")

        # Data quality summary
        print("\n  Data Quality Check:")
        missing_pct = (final_df.isnull().sum().sum() / final_df.size) * 100
        print(f"    Missing values: {missing_pct:.2f}%")
        print(f"    Negative values: {(final_df.select_dtypes('number') < 0).sum().sum()}")

        print("\n  Stage 2 complete: Data cleaning and feature engineering finished")

    except Exception as e:
        print(f"\n  Error in Stage 2: {e}")
        raise


def stage_3_normalization() -> None:
    """
    Stage 3: Normalize indicators using optimized log-transformation method.

    Normalization Process:
    1. Log transformation: log(1 + x) applied to skewed metrics
       - Applied to: TVL, Fees, Revenue, DAU, Tx_Count, Core_Utility
       - NOT applied to: Liquidity metrics (already ratios)

    2. Smoothing: 7-day rolling average to reduce noise

    3. Scaling:
       - Min-Max: Maps values to [0, 100] range (primary)
       - Z-Score: Standardizes to mean=0, std=1 (robustness check)

    Outputs
    -------
    data/processed/normalized_minmax_log.csv : Primary normalized data
    data/processed/normalized_zscore_log.csv : Alternative for sensitivity tests
    """
    print_stage_header(3, "NORMALIZATION (5-Dimension Framework)")

    try:
        input_path = PROCESSED_DIR / "clean_dataset_final.csv"

        # Verify input exists
        if not input_path.exists():
            raise FileNotFoundError(
                f"Input file missing: {input_path}\n"
                "Please run Stage 2 first."
            )


        # Method A: Log + Min-Max (Primary)
        print_substep("1", "2", "Applying Log + Min-Max normalization")
        print("  Transformation steps:")
        print("    1. log(1+x) for: TVL, Fees, Revenue, DAU, Tx_Count, Core_Utility")
        print("    2. No log for liquidity metrics (ratios already bounded)")
        print("    3. 7-day moving average smoothing")
        print("    4. Min-Max scaling to [0, 100]")

        normalizer_minmax = OptimizedNormalizer(
            input_path=str(input_path),
            output_dir=str(PROCESSED_DIR),
            method='minmax'
        )
        norm_minmax_df = normalizer_minmax.normalize_with_log()

        output_minmax = PROCESSED_DIR / "normalized_minmax_log.csv"
        norm_minmax_df.to_csv(output_minmax, index=False)
        print_file_saved(output_minmax)

        # Preview score distributions
        score_cols = [c for c in norm_minmax_df.columns if c.endswith('_score')]
        print("\n  Score statistics (Min-Max):")
        for col in score_cols[:5]:  # Show first 5
            mean_val = norm_minmax_df[col].mean()
            std_val = norm_minmax_df[col].std()
            print(f"    {col}: mean={mean_val:.2f}, std={std_val:.2f}")


        # Method B: Log + Z-Score (Backup for robustness tests)
        print_substep("2", "2", "Applying Log + Z-Score normalization (backup)")
        print("  This output is reserved for sensitivity analysis.")

        normalizer_zscore = OptimizedNormalizer(
            input_path=str(input_path),
            output_dir=str(PROCESSED_DIR),
            method='zscore'
        )
        norm_zscore_df = normalizer_zscore.normalize_with_log()

        output_zscore = PROCESSED_DIR / "normalized_zscore_log.csv"
        norm_zscore_df.to_csv(output_zscore, index=False)
        print_file_saved(output_zscore)

        print("\n  Stage 3 complete: Normalization finished")

    except Exception as e:
        print(f"\n  Error in Stage 3: {e}")
        raise


def stage_4_index_construction() -> None:
    """
    Stage 4: Aggregate normalized indicators into composite DeFi Activity Index.

    5-Dimension Equal-Weighting Scheme (20% each)
    ----------------------------------------------

    D1: Capital Scale (20%)
      - TVL: 20%

    D2: Liquidity (20%)
      - Primary liquidity metric: 20%
      - Protocol-specific (Utilization/Turnover/Revenue Efficiency)

    D3: User Activity (20%)
      - DAU: 10%
      - Tx Count: 10%

    D4: Operational Output (20%)
      - Core Utility Metric: 20%

    D5: Financial Performance (20%)
      - Total Fees: 10%
      - Protocol Revenue: 10%

    Aggregation Formula:
        Index = Sigma (Weight_i x Score_i)

    Outputs
    -------
    data/final/final_index_5dim.csv : Composite index with dimension scores
    """
    print_stage_header(4, "INDEX CONSTRUCTION (5-Dimension Framework)")

    try:
        input_path = PROCESSED_DIR / "normalized_minmax_log.csv"

        # Verify input exists
        if not input_path.exists():
            raise FileNotFoundError(
                f"Input file missing: {input_path}\n"
                "Please run Stage 3 first."
            )

        print("\n  Index Configuration:")
        print(WEIGHTING_SCHEME)

        print_substep("1", "1", "Computing composite index and dimension scores")

        aggregator = OptimizedIndexAggregator(
            input_path=str(input_path),
            output_dir=str(FINAL_DIR)
        )

        index_df = aggregator.build_optimized_index()

        # Correlation analysis to validate dimension independence
        aggregator.compute_correlation_analysis(index_df)

        # Save final index
        output_file = FINAL_DIR / "final_index_5dim.csv"
        index_df.to_csv(output_file, index=False)
        print_file_saved(output_file)


        # Index Summary Statistics

        print("\n  Index Summary Statistics:")

        # Identify score column
        score_col = 'composite_index' if 'composite_index' in index_df.columns else 'Index_Value'

        print(f"    Overall mean: {index_df[score_col].mean():.4f}")
        print(f"    Overall std:  {index_df[score_col].std():.4f}")
        print(f"    Min score:    {index_df[score_col].min():.4f}")
        print(f"    Max score:    {index_df[score_col].max():.4f}")

        # Per-protocol averages
        print("\n  Average Index by Protocol:")
        protocol_means = index_df.groupby('protocol')[score_col].mean().sort_values(ascending=False)
        for protocol, mean_score in protocol_means.items():
            print(f"    {protocol:15s}: {mean_score:.4f}")

        # Dimension breakdown
        print("\n  Dimension Breakdown (Latest):")
        latest_date = index_df['date'].max()
        latest_data = index_df[index_df['date'] == latest_date].copy()
        dim_cols = [c for c in latest_data.columns if c.startswith('D')]
        # Include score_col in display and sort
        display_cols = ['protocol', score_col] + dim_cols
        print(latest_data[display_cols].sort_values(score_col, ascending=False).to_string(index=False))

        # Latest rankings
        print("\n  Latest Date Rankings:")
        latest_ranking = latest_data.sort_values(
            score_col, ascending=False
        )[['protocol', score_col]]
        print(f"    Date: {latest_date}")
        for _, row in latest_ranking.iterrows():
            print(f"      {row['protocol']:15s}: {row[score_col]:.4f}")

        print("\n  Stage 4 complete: Index construction finished")

    except Exception as e:
        print(f"\n  Error in Stage 4: {e}")
        raise


def print_pipeline_summary() -> None:
    """Print comprehensive summary of pipeline execution."""
    print("\n" + "=" * 70)
    print("PIPELINE EXECUTION SUMMARY")
    print("=" * 70)

    # Output Files
    print("\n  Generated Files:")

    files_to_check = [
        ("Cleaned Data", PROCESSED_DIR / "clean_dataset_final.csv"),
        ("Normalized (Log+MinMax)", PROCESSED_DIR / "normalized_minmax_log.csv"),
        ("Normalized (Log+ZScore)", PROCESSED_DIR / "normalized_zscore_log.csv"),
        ("Final Index (5-Dim)", FINAL_DIR / "final_index_5dim.csv"),
    ]

    for name, path in files_to_check:
        if path.exists():
            size_kb = path.stat().st_size // 1024
            print(f"      {name:25s}: {path.name} ({size_kb} KB)")
        else:
            print(f"      X {name:25s}: NOT FOUND")


    # Data Summary
    final_index_path = FINAL_DIR / "final_index_5dim.csv"
    if final_index_path.exists():
        import pandas as pd
        df = pd.read_csv(final_index_path)

        print("\n  Data Summary:")
        print(f"    Protocols analyzed:  {df['protocol'].nunique()}")
        print(f"    Time range:          {df['date'].min()} to {df['date'].max()}")
        print(f"    Total observations:  {len(df):,}")

        score_col = 'composite_index' if 'composite_index' in df.columns else 'Index_Value'
        print(f"\n  Index Score Range:")
        print(f"    Minimum: {df[score_col].min():.4f}")
        print(f"    Maximum: {df[score_col].max():.4f}")
        print(f"    Mean:    {df[score_col].mean():.4f}")


    # File Locations
    print("\n  Output Directories:")
    print(f"    Processed data: {PROCESSED_DIR}")
    print(f"    Final index:    {FINAL_DIR}")

    # 5-Dimension Framework Summary
    print("\n  5-Dimension Framework:")
    print("    D1: Capital Scale (20%)")
    print("    D2: Liquidity (20%)")
    print("    D3: User Activity (20%)")
    print("    D4: Operational Output (20%)")
    print("    D5: Financial Performance (20%)")

    # Timestamp
    print("\n" + "=" * 70)
    print(f"Pipeline completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)



# MAIN EXECUTION

def main():
    """
    Execute the complete DeFi Activity Index construction pipeline.

    Pipeline Flow:
        Stage 1 (Data Collection)
            v
        Stage 2 (Cleaning & Feature Engineering)
            v
        Stage 3 (Normalization)
            v
        Stage 4 (Index Aggregation)
            v
        Summary Report

    Configuration:
        Set SKIP_API_CALLS = True to reuse existing raw data (faster).
        Set SKIP_API_CALLS = False to fetch fresh data from APIs.
    """


    # CONFIGURATION
    SKIP_API_CALLS = True  # Set to False to fetch fresh data


    # PIPELINE EXECUTION
    print("\n" + "=" * 70)
    print("       DeFi ACTIVITY INDEX - CONSTRUCTION PIPELINE")
    print("                  5-Dimension Framework")
    print("=" * 70)
    print(f"\nProject root:  {project_root}")
    print(f"Start time:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Date range:   {START_DATE} to {END_DATE}")
    print(f"Protocols:    {', '.join(PROTOCOLS)}")
    print(f"Skip API:     {SKIP_API_CALLS}")

    try:
        # Stage 1: Data Collection
        stage_1_data_collection(skip_api=SKIP_API_CALLS)

        # Stage 2: Data Cleaning & Feature Engineering
        stage_2_data_cleaning()

        # Stage 3: Normalization
        stage_3_normalization()

        # Stage 4: Index Construction
        stage_4_index_construction()

        # Print final summary
        print_pipeline_summary()

        print("\n" + "=" * 70)
        print("   ALL STAGES COMPLETED SUCCESSFULLY")
        print("=" * 70 + "\n")

        return 0  # Success exit code

    except KeyboardInterrupt:
        print("\n\n  Pipeline interrupted by user (Ctrl+C)")
        print("    Partial outputs may have been saved.")
        return 130  # Standard interrupt exit code

    except FileNotFoundError as e:
        print(f"\n{'=' * 70}")
        print("  PIPELINE FAILED: File Not Found")
        print(f"{'=' * 70}")
        print(f"\nError: {e}")
        print("\nPossible solutions:")
        print("  1. Ensure raw data files exist in data/raw/")
        print("  2. Run stages in order (1 -> 2 -> 3 -> 4)")
        print("  3. Set SKIP_API_CALLS = False to fetch fresh data")
        return 1

    except Exception as e:
        print(f"\n{'=' * 70}")
        print("  PIPELINE FAILED: Unexpected Error")
        print(f"{'=' * 70}")
        print(f"\nError type: {type(e).__name__}")
        print(f"Error message: {e}")
        print("\nFull traceback:")
        import traceback
        traceback.print_exc()
        return 1

# ENTRY POINT
if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
