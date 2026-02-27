"""
cleaner.py
Data cleaning module for DeFi Activity Index.

Responsibilities:
- Load raw data from API and manual sources
- Standardize date formats and column names
- Merge protocol-specific data into master dataset
- Handle missing values (forward-fill, backward-fill)
"""

import pandas as pd
import os
import numpy as np
from pathlib import Path


class DataCleaner:
    """
    Clean and merge raw DeFi protocol data.
    
    Parameters
    ----------
    raw_api_dir : str
        Path to directory containing API-sourced data (TVL, Fees, Revenue, Volume)
    raw_manual_dir : str
        Path to directory containing manually exported data (DAU, Tx Count, etc.)
    output_dir : str
        Path to save processed data
    start_date : str, optional
        Start of analysis period (default: '2023-01-01')
    end_date : str, optional
        End of analysis period (default: '2024-12-31')
    """
    
    def __init__(self, raw_api_dir, raw_manual_dir, output_dir, 
                 start_date='2023-01-01', end_date='2024-12-31'):
        self.raw_api_dir = Path(raw_api_dir)
        self.raw_manual_dir = Path(raw_manual_dir)
        self.output_dir = Path(output_dir)
        self.start_date = start_date
        self.end_date = end_date
        self.date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Protocol list
        self.protocols = ['aave_v3', 'compound_v3', 'uniswap_v3', 
                          'curve', 'makerdao', 'lido']
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    
    def read_and_standardize(self, file_path, value_col_name='value'):
        """
        Read CSV and standardize to datetime index with single value column.
        
        Parameters
        ----------
        file_path : str or Path
            Path to CSV file
        value_col_name : str
            Name for the standardized value column
        
        Returns
        -------
        pd.DataFrame or None
            Standardized DataFrame with datetime index, or None if file missing
        """
        if not Path(file_path).exists():
            print(f"  File missing: {file_path}")
            return None
        
        try:
            df = pd.read_csv(file_path)
            
            # Auto-detect date column
            date_col = [c for c in df.columns 
                       if any(kw in c.lower() for kw in ['date', 'day', 'time'])][0]
            
            # Auto-detect value column (first non-date column)
            val_col = [c for c in df.columns if c != date_col][0]
            
            # Convert to datetime and set index
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.set_index(date_col).sort_index()
            
            # Rename value column
            df = df[[val_col]].rename(columns={val_col: value_col_name})
            
            # Remove duplicates (keep last)
            df = df[~df.index.duplicated(keep='last')]
            
            return df
            
        except Exception as e:
            print(f" Error reading {file_path}: {e}")
            return None
    
    
    def load_protocol_data(self, protocol):
        """
        Load all data sources for a single protocol.
        
        Parameters
        ----------
        protocol : str
            Protocol identifier (e.g., 'aave_v3')
        
        Returns
        -------
        pd.DataFrame
            Merged DataFrame with all metrics for this protocol
        """
        # Create empty DataFrame with full date range
        protocol_df = pd.DataFrame(index=self.date_range)
        protocol_df.index.name = 'date'
        
        
        # Load API data
        
        
        # TVL
        path_tvl = self.raw_api_dir / f"tvl_{protocol}.csv"
        df_tvl = self.read_and_standardize(path_tvl, 'tvl')
        if df_tvl is not None:
            protocol_df = protocol_df.join(df_tvl)
        
        # Fees
        path_fees = self.raw_api_dir / f"fees_{protocol}.csv"
        df_fees = self.read_and_standardize(path_fees, 'fees')
        if df_fees is not None:
            protocol_df = protocol_df.join(df_fees)
        
        # Revenue
        path_rev = self.raw_api_dir / f"revenue_{protocol}.csv"
        df_rev = self.read_and_standardize(path_rev, 'revenue')
        if df_rev is not None:
            protocol_df = protocol_df.join(df_rev)
        
        
        # Load Manual data
        
        
        # DAU
        path_dau = self.raw_manual_dir / f"{protocol}_dau.csv"
        df_dau = self.read_and_standardize(path_dau, 'dau')
        if df_dau is not None:
            protocol_df = protocol_df.join(df_dau)
        
        # Tx Count
        path_tx = self.raw_manual_dir / f"{protocol}_tx_count.csv"
        df_tx = self.read_and_standardize(path_tx, 'tx_count')
        if df_tx is not None:
            protocol_df = protocol_df.join(df_tx)
        
        
        # Load Core Utility (context-specific)
        
        
        utility_path = None
        
        if protocol in ['uniswap_v3', 'curve']:
            # DEX: Volume
            utility_path = self.raw_api_dir / f"volume_{protocol}.csv"
            
        elif protocol in ['aave_v3', 'compound_v3']:
            # Lending: Active Loans / Total Borrowed
            utility_path = self.raw_manual_dir / f"{protocol}_active_loans.csv"
            
        elif protocol == 'lido':
            # Liquid Staking: Assets Staked
            utility_path = self.raw_manual_dir / "lido_assets_staked.csv"
            
        elif protocol == 'makerdao':
            # Stablecoin: Circulating Supply
            utility_path = self.raw_manual_dir / "makerdao_circulating_supply.csv"
        
        if utility_path:
            df_util = self.read_and_standardize(utility_path, 'core_utility')
            if df_util is not None:
                protocol_df = protocol_df.join(df_util)
        else:
            print(f"  No core_utility mapping defined for {protocol}")
            protocol_df['core_utility'] = np.nan
        
        
        # Missing value handling
        
        
        # Forward fill
        protocol_df = protocol_df.ffill()
        
        # Backward fill
        protocol_df = protocol_df.bfill()
        
        # Fill remaining NaN with 0
        protocol_df = protocol_df.fillna(0)
        
        # Add protocol identifier
        protocol_df['protocol'] = protocol
        
        # Reset index to make date a column
        protocol_df = protocol_df.reset_index()
        
        return protocol_df
    
    
    def create_master_dataset(self):
        """
        Process all protocols and create master dataset.
        
        Returns
        -------
        pd.DataFrame
            Merged dataset containing all protocols
        """
        print("Starting data cleaning pipeline...")
        
        all_data = []
        
        for protocol in self.protocols:
            print(f"\nProcessing: {protocol.upper()}...")
            protocol_df = self.load_protocol_data(protocol)
            all_data.append(protocol_df)
        
        # Concatenate all protocols
        master_df = pd.concat(all_data, ignore_index=True)
        
        # Reorder columns
        cols = ['date', 'protocol', 'tvl', 'fees', 'revenue', 
                'dau', 'tx_count', 'core_utility']
        cols = [c for c in cols if c in master_df.columns]
        master_df = master_df[cols]
        
        # Round core_utility to 3 decimal places
        if 'core_utility' in master_df.columns:
            master_df['core_utility'] = master_df['core_utility'].round(3)
        
        # Data quality check
        print("\n=== Data Quality Summary ===")
        print(f"Total rows: {len(master_df)}")
        print(f"Date range: {master_df['date'].min()} → {master_df['date'].max()}")
        print(f"\nMean values by protocol:")
        print(master_df.groupby('protocol')[['tvl', 'revenue', 'core_utility']].mean())
        
        return master_df



# Standalone execution (for testing)


if __name__ == "__main__":
    # Get project root
    CURRENT_SCRIPT = Path(__file__).resolve()
    PROJECT_ROOT = CURRENT_SCRIPT.parent.parent.parent
    
    cleaner = DataCleaner(
        raw_api_dir=str(PROJECT_ROOT / 'data' / 'raw' / 'api'),
        raw_manual_dir=str(PROJECT_ROOT / 'data' / 'raw' / 'manual'),
        output_dir=str(PROJECT_ROOT / 'data' / 'processed')
    )
    
    master_df = cleaner.create_master_dataset()
    
    output_file = PROJECT_ROOT / 'data' / 'processed' / 'master_dataset_clean.csv'
    master_df.to_csv(output_file, index=False)
    print(f"\n Saved: {output_file}")