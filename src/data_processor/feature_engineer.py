"""
feature_engineer.py
Feature engineering module for DeFi Activity Index.

Responsibilities:
- Compute Liquidity metrics (Liquidity = Operational Output / TVL)
- Compute derived metrics while avoiding mathematical correlation issues
"""

import pandas as pd
import numpy as np
from pathlib import Path


class FeatureEngineer:
    """
    Add derived features to cleaned dataset.

    Key Design Principle:
    ---------------------
    The Liquidity dimension is SEPARATED from Capital dimension to avoid
    mathematical correlation.

    Formula: Liquidity = Operational Output / TVL

    Problem if combined with TVL in same dimension:
    - Liquidity = Throughput/TVL contains TVL in denominator
    - When TVL increases: TVL Score increases, but Liquidity Score decreases (assuming Throughput constant)
    - This creates mathematical negative correlation causing signal cancellation

    Solution:
    - D1 Capital: TVL only (scale measure, 20%)
    - D2 Liquidity: Throughput/TVL ratio (efficiency measures, 20%)
    - These are computed separately and aggregated independently

    Protocol-Specific Liquidity Indicators:
    --------------------------------------
    - DEX (Uniswap, Curve): Trading Liquidity Utilization = Trading Volume / TVL
    - Lending (Aave, Compound): Borrow Utilization Ratio = Total Borrowed / TVL
    - Stablecoin (MakerDAO): Capital Deployment Ratio = Circulating Supply / TVL
    - LSD (Lido): Staking Liquidity Flow = Net Inflow / TVL

    Methods
    -------
    add_features(df)
        Compute all derived metrics for 5-dimension framework
    """

    def __init__(self):
        pass


    def compute_net_inflow(self, df):
        """
        Compute Net Inflow from Assets Staked time series.

        Formula: Net Inflow_t = Staked Assets_t - Staked Assets_{t-1}

        This captures the daily change in staked assets, representing capital
        flows in (positive) or out (negative) of the staking protocol.

        Note: Must be computed per protocol, sorted by date, to correctly
        calculate the time difference.

        Parameters
        ----------
        df : pd.DataFrame
            Must contain 'protocol', 'date', 'assets_staked' columns

        Returns
        -------
        pd.DataFrame
            DataFrame with added 'net_inflow' column
        """
        print("Computing Net Inflow from Assets Staked...")

        df = df.copy()

        # Sort by protocol and date to ensure correct diff calculation
        df = df.sort_values(['protocol', 'date']).reset_index(drop=True)

        # Compute diff per protocol
        df['net_inflow'] = df.groupby('protocol')['assets_staked'].diff()

        # First day of each protocol has no previous value, set to 0
        df['net_inflow'] = df['net_inflow'].fillna(0)

        # Handle edge cases
        df['net_inflow'] = df['net_inflow'].replace([np.inf, -np.inf], 0)

        # Round to 2 decimal places (ETH/USD values)
        df['net_inflow'] = df['net_inflow'].round(2)

        return df


    def compute_borrow_utilization(self, df):
        """
        Compute Borrow Utilization Ratio for lending protocols.

        Formula: Borrow Utilization = Total Borrowed / TVL

        Measures the proportion of deposited capital that has been borrowed.
        High utilization indicates strong credit demand and efficient capital deployment.
        Note: This is the same as "Active Loans / TVL" for lending protocols.

        Parameters
        ----------
        df : pd.DataFrame
            Must contain 'tvl', 'active_loans' columns

        Returns
        -------
        pd.DataFrame
            DataFrame with added 'borrow_utilization' column
        """
        print("Computing Borrow Utilization Ratio...")

        # Compute ratio: Borrowed / TVL
        df['borrow_utilization'] = df['active_loans'] / df['tvl']

        # Handle edge cases
        df['borrow_utilization'] = df['borrow_utilization'].replace([np.inf, -np.inf], 0)
        df['borrow_utilization'] = df['borrow_utilization'].fillna(0)

        # Cap at reasonable maximum (100%)
        df['borrow_utilization'] = df['borrow_utilization'].clip(upper=1.0)

        # Round to 4 decimal places
        df['borrow_utilization'] = df['borrow_utilization'].round(4)

        return df


    def compute_trading_utilization(self, df):
        """
        Compute Trading Liquidity Utilization for DEX protocols.

        Formula: Trading Liquidity Utilization = Trading Volume / TVL

        Measures how intensively available liquidity supports trading activity.
        Higher volume relative to TVL indicates deeper and more actively used liquidity pools.

        Note: This is the same as "Turnover Ratio" for DEX protocols.

        Parameters
        ----------
        df : pd.DataFrame
            Must contain 'tvl', 'volume' columns

        Returns
        -------
        pd.DataFrame
            DataFrame with added 'trading_utilization' column
        """
        print("Computing Trading Liquidity Utilization...")

        # Compute ratio: Trading Volume / TVL
        df['trading_utilization'] = df['volume'] / df['tvl']

        # Handle edge cases
        df['trading_utilization'] = df['trading_utilization'].replace([np.inf, -np.inf], 0)
        df['trading_utilization'] = df['trading_utilization'].fillna(0)

        # Round to 4 decimal places
        df['trading_utilization'] = df['trading_utilization'].round(4)

        return df


    def compute_capital_deployment_ratio(self, df):
        """
        Compute Capital Deployment Ratio for stablecoin protocols.

        Formula: Capital Deployment = Circulating Supply / TVL

        Measures the extent to which collateral is converted into circulating stablecoins.
        Reflects the protocol's ability to release liquidity into the broader ecosystem.

        Parameters
        ----------
        df : pd.DataFrame
            Must contain 'tvl', 'circulating_supply' columns

        Returns
        -------
        pd.DataFrame
            DataFrame with added 'capital_deployment' column
        """
        print("Computing Capital Deployment Ratio...")

        # Compute ratio: Circulating Supply / TVL
        df['capital_deployment'] = df['circulating_supply'] / df['tvl']

        # Handle edge cases
        df['capital_deployment'] = df['capital_deployment'].replace([np.inf, -np.inf], 0)
        df['capital_deployment'] = df['capital_deployment'].fillna(0)

        # Round to 4 decimal places
        df['capital_deployment'] = df['capital_deployment'].round(4)

        return df


    def compute_staking_flow_ratio(self, df):
        """
        Compute Staking Liquidity Flow for LSD protocols.

        Formula: Staking Liquidity Flow = Net Inflow / TVL
        Where: Net Inflow = diff(assets_staked)

        Measures the net inflow of capital into staking relative to existing locked assets.
        Captures the dynamic supply of liquidity entering the system over time.

        Note: 'net_inflow' must be computed first using compute_net_inflow() method.

        Parameters
        ----------
        df : pd.DataFrame
            Must contain 'tvl', 'net_inflow' columns

        Returns
        -------
        pd.DataFrame
            DataFrame with added 'staking_flow' column
        """
        print("Computing Staking Liquidity Flow...")

        # Compute ratio: Net Inflow / TVL
        # Net Inflow can be positive or negative
        df['staking_flow'] = df['net_inflow'] / df['tvl']

        # Handle edge cases
        df['staking_flow'] = df['staking_flow'].replace([np.inf, -np.inf], 0)
        df['staking_flow'] = df['staking_flow'].fillna(0)

        # Cap at reasonable range [-2, 2] to handle extreme daily flows
        df['staking_flow'] = df['staking_flow'].clip(lower=-2.0, upper=2.0)

        # Round to 4 decimal places
        df['staking_flow'] = df['staking_flow'].round(4)

        return df


    def compute_liquidity_metrics(self, df):
        """
        Compute liquidity metrics and apply protocol-specific logic.

        Liquidity Dimension Indicators (Liquidity = Operational Output / TVL):
        - Borrow Utilization Ratio: For lending protocols (Aave, Compound)
        - Trading Liquidity Utilization: For DEX protocols (Uniswap, Curve)
        - Capital Deployment Ratio: For stablecoin protocols (MakerDAO)
        - Staking Liquidity Flow: For LSD protocols (Lido)

        Note: For Lido, 'net_inflow' must be computed first via compute_net_inflow()
        which derives it from diff(assets_staked).

        Parameters
        ----------
        df : pd.DataFrame
            Must contain 'protocol', 'tvl', 'volume', 'active_loans',
                   'circulating_supply', 'net_inflow' (for Lido)

        Returns
        -------
        pd.DataFrame
            DataFrame with added liquidity_metric column
        """
        print("Computing Liquidity Metrics (Liquidity = Operational Output / TVL)...")

        df = df.copy()

        # Initialize liquidity column
        df['liquidity_metric'] = np.nan

        # Protocol category definitions
        lending_protocols = ['aave_v3', 'compound_v3']
        dex_protocols = ['uniswap_v3', 'curve']
        stablecoin_protocols = ['makerdao']
        lsd_protocols = ['lido']

        for idx, row in df.iterrows():
            protocol = row['protocol']

            if protocol in lending_protocols:
                # Borrow Utilization Ratio = Total Borrowed / TVL
                if row['tvl'] > 0 and row['active_loans'] > 0:
                    df.at[idx, 'liquidity_metric'] = row['active_loans'] / row['tvl']
                else:
                    df.at[idx, 'liquidity_metric'] = 0.0

            elif protocol in dex_protocols:
                # Trading Liquidity Utilization = Trading Volume / TVL
                if row['tvl'] > 0 and row['volume'] > 0:
                    df.at[idx, 'liquidity_metric'] = row['volume'] / row['tvl']
                else:
                    df.at[idx, 'liquidity_metric'] = 0.0

            elif protocol in stablecoin_protocols:
                # Capital Deployment Ratio = Circulating Supply / TVL
                if row['tvl'] > 0 and row.get('circulating_supply', 0) > 0:
                    df.at[idx, 'liquidity_metric'] = row['circulating_supply'] / row['tvl']
                else:
                    df.at[idx, 'liquidity_metric'] = 0.0

            elif protocol in lsd_protocols:
                # Staking Liquidity Flow = Net Inflow / TVL
                if row['tvl'] > 0 and row.get('net_inflow', 0) is not None:
                    df.at[idx, 'liquidity_metric'] = row['net_inflow'] / row['tvl']
                else:
                    df.at[idx, 'liquidity_metric'] = 0.0

        # Clean up
        df['liquidity_metric'] = df['liquidity_metric'].replace([np.inf, -np.inf], 0)
        df['liquidity_metric'] = df['liquidity_metric'].fillna(0)

        # Cap at reasonable range
        df['liquidity_metric'] = df['liquidity_metric'].clip(lower=-2.0, upper=2.0)

        # Round to 4 decimal places
        df['liquidity_metric'] = df['liquidity_metric'].round(4)

        return df


    def validate_logic(self, df):
        """
        Sanity check: Verify protocol-specific liquidity metrics.

        Expected patterns:
        - DEX trading utilization should be higher than lending borrow utilization
        - Stablecoin capital deployment should be close to 1.0 (full deployment)
        - LSD staking flow should be relatively small (daily net flows vs TVL)

        Parameters
        ----------
        df : pd.DataFrame
            Must contain 'protocol' and liquidity columns
        """
        print("\n=== Logic Validation: Protocol Liquidity Comparison ===")

        # Check Trading Liquidity Utilization for DEX
        dex_protocols = ['uniswap_v3', 'curve']
        if 'trading_utilization' in df.columns:
            dex_util = df[df['protocol'].isin(dex_protocols)].groupby('protocol')['trading_utilization'].mean()
            print("\nDEX Trading Liquidity Utilization (mean):")
            print(dex_util.sort_values(ascending=False))

        # Check Borrow Utilization for Lending
        lending_protocols = ['aave_v3', 'compound_v3']
        if 'borrow_utilization' in df.columns:
            lending_util = df[df['protocol'].isin(lending_protocols)].groupby('protocol')['borrow_utilization'].mean()
            print("\nLending Borrow Utilization Ratio (mean):")
            print(lending_util.sort_values(ascending=False))

        # Check Capital Deployment for Stablecoin
        if 'capital_deployment' in df.columns:
            makerdao_data = df[df['protocol'] == 'makerdao']
            if len(makerdao_data) > 0:
                print("\nMakerDAO Capital Deployment Ratio:")
                print(f"  Mean: {makerdao_data['capital_deployment'].mean():.4f}")
                print(f"  (Should be close to 1.0 for stablecoins)")

        # Check Staking Flow for LSD
        if 'staking_flow' in df.columns:
            lido_data = df[df['protocol'] == 'lido']
            if len(lido_data) > 0:
                print("\nLido Staking Liquidity Flow:")
                print(f"  Mean: {lido_data['staking_flow'].mean():.4f}")
                print(f"  Std:  {lido_data['staking_flow'].std():.4f}")

        print("\n✓ Logic validation complete")


    def add_features(self, df):
        """
        Main feature engineering pipeline.

        Computes all derived metrics for the 5-dimension DeFi Activity Index:
        - D1: Capital (TVL only - from raw data)
        - D2: Liquidity (Liquidity = Operational Output / TVL)
            * Lending: Borrow Utilization Ratio
            * DEX: Trading Liquidity Utilization
            * Stablecoin: Capital Deployment Ratio
            * LSD: Staking Liquidity Flow = Net Inflow / TVL
        - D3: User Activity (DAU, Tx Count - from raw data)
        - D4: Operational Output (Core Utility - from raw data)
        - D5: Financial Performance (Fees, Revenue - from raw data)

        Note: Net Inflow is derived from diff(assets_staked) for LSD protocols.

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

        # Step 1: Compute Net Inflow from Assets Staked (for LSD protocols)
        # Net Inflow_t = Staked Assets_t - Staked Assets_{t-1}
        if 'assets_staked' in df.columns:
            df = self.compute_net_inflow(df)

        # Step 2: Compute Liquidity metrics (Liquidity = Operational Output / TVL)
        df = self.compute_borrow_utilization(df)
        df = self.compute_trading_utilization(df)
        df = self.compute_capital_deployment_ratio(df)
        df = self.compute_staking_flow_ratio(df)
        df = self.compute_liquidity_metrics(df)

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
    print(f"\n  Saved: {OUTPUT_FILE}")
    print(f"Columns: {list(df_final.columns)}")
