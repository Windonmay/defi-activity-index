"""
feature_engineer.py
Feature engineering module for DeFi Activity Index.

Responsibilities:
- Compute derived metrics (Capital Turnover Ratio)
"""

import pandas as pd
import numpy as np
from pathlib import Path


class FeatureEngineer:
    """
    Add derived features to cleaned dataset.
    
    Methods
    -------
    add_features(df)
        Compute Capital Turnover Ratio and other derived metrics
    """
    
    def __init__(self):
        pass
    
    
    def compute_capital_turnover(self, df):
        """
        Compute Capital Turnover Ratio.
        
        Formula: Turnover = Core_Utility / TVL
        
        Measures how many dollars of throughput are generated per dollar of TVL.
        
        Parameters
        ----------
        df : pd.DataFrame
            Must contain 'core_utility' and 'tvl' columns
        
        Returns
        -------
        pd.DataFrame
            DataFrame with added 'capital_turnover' column
        """
        print("Computing Capital Turnover Ratio...")
        
        # Compute ratio
        df['capital_turnover'] = df['core_utility'] / df['tvl']
        
        # Handle edge cases
        # Case A: Division by zero (TVL = 0) → inf
        df['capital_turnover'] = df['capital_turnover'].replace([np.inf, -np.inf], 0)
        
        # Case B: Both numerator and denominator are 0 → NaN
        df['capital_turnover'] = df['capital_turnover'].fillna(0)
        
        # Round to 3 decimal places
        df['capital_turnover'] = df['capital_turnover'].round(3)
        
        # Validation: Check for extreme values
        max_ratio = df['capital_turnover'].max()
        print(f"  Max turnover ratio: {max_ratio:.4f}")
        
        if max_ratio > 10:
            print(f"    WARNING: Unusually high turnover detected (>{10})")
            print("     Check if core_utility units are correct (should be USD)")
        
        return df
    
    
    def validate_logic(self, df):
        """
        Sanity check: DEX turnover should be higher than Lending.
        
        Parameters
        ----------
        df : pd.DataFrame
            Must contain 'protocol' and 'capital_turnover' columns
        """
        print("\n=== Logic Validation: Protocol Turnover Comparison ===")
        
        stats = df.groupby('protocol')['capital_turnover'].mean().sort_values(ascending=False)
        print(stats)
        
        # Expected pattern: DEX (Uniswap, Curve) >> Lending (Aave, Compound)
        dex_protocols = ['uniswap_v3', 'curve']
        lending_protocols = ['aave_v3', 'compound_v3']
        
        dex_mean = stats[stats.index.isin(dex_protocols)].mean()
        lending_mean = stats[stats.index.isin(lending_protocols)].mean()
        
        print(f"\nDEX average turnover: {dex_mean:.4f}")
        print(f"Lending average turnover: {lending_mean:.4f}")
        
        if dex_mean > lending_mean:
            print("✓ Logic check passed: DEX turnover > Lending turnover")
        else:
            print("  WARNING: DEX turnover ≤ Lending turnover")
            print("  Check if 'core_utility' units are consistent across protocols")
    
    
    def add_features(self, df):
        """
        Main feature engineering pipeline.
        
        Parameters
        ----------
        df : pd.DataFrame
            Cleaned master dataset
        
        Returns
        -------
        pd.DataFrame
            Dataset with added features
        """
        df = df.copy()
        
        # Compute Capital Turnover
        df = self.compute_capital_turnover(df)
        
        # Validate
        self.validate_logic(df)
        
        return df

# Standalone execution (for testing)

if __name__ == "__main__":
    CURRENT_SCRIPT = Path(__file__).resolve()
    PROJECT_ROOT = CURRENT_SCRIPT.parent.parent.parent
    
    INPUT_FILE = PROJECT_ROOT / 'data' / 'processed' / 'master_dataset_clean.csv'
    OUTPUT_FILE = PROJECT_ROOT / 'data' / 'processed' / 'clean_dataset_final.csv'
    
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file missing: {INPUT_FILE}")
    
    print(f"Reading data: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    df['date'] = pd.to_datetime(df['date'])
    
    engineer = FeatureEngineer()
    df_final = engineer.add_features(df)
    
    df_final.to_csv(OUTPUT_FILE, index=False)
    print(f"\n Saved: {OUTPUT_FILE}")
    print(f"Columns: {list(df_final.columns)}")