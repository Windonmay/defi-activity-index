"""
normalizer_optimized.py
Optimized normalization with log transformation.

Applies log(1+x) to skewed metrics before normalization.
"""

import pandas as pd
import numpy as np
from pathlib import Path


class OptimizedNormalizer:
    """
    Normalize DeFi metrics with log transformation.
    
    Parameters
    ----------
    input_path : str
        Path to clean_dataset_final.csv
    output_dir : str
        Directory to save normalized data
    method : str
        'minmax' or 'zscore'
    """
    
    def __init__(self, input_path, output_dir, method='minmax'):
        self.input_path = Path(input_path)
        self.output_dir = Path(output_dir)
        self.method = method
        
        # Metrics to process
        self.metrics = ['tvl', 'fees', 'revenue', 'dau', 'tx_count', 
                       'core_utility', 'capital_turnover']
        
        # Metrics that need log transformation (exclude turnover)
        self.log_metrics = ['tvl', 'fees', 'revenue', 'dau', 
                           'tx_count', 'core_utility']
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    
    def apply_log_transform(self, df):
        """Apply log(1+x) to skewed metrics."""
        print("Applying log transformation to compress skewed distributions...")
        
        df_transformed = df.copy()
        
        for metric in self.log_metrics:
            if metric in df.columns:
                df_transformed[metric] = np.log1p(df_transformed[metric])
        
        return df_transformed
    
    
    def smooth_data(self, df, window=7):
        """Apply rolling average."""
        print(f"Applying {window}-day moving average...")
        
        df_smooth = df.copy()
        
        for metric in self.metrics:
            if metric in df.columns:
                smooth_col = f"{metric}_smooth"
                df_smooth[smooth_col] = df_smooth.groupby('protocol')[metric].transform(
                    lambda x: x.rolling(window=window, min_periods=1).mean()
                )
        
        return df_smooth
    
    
    def normalize_minmax(self, df):
        """Min-Max normalization to [0, 100]."""
        print("Applying Min-Max normalization (0-100)...")
        
        df_norm = df.copy()
        
        for metric in self.metrics:
            smooth_col = f"{metric}_smooth"
            score_col = f"{metric}_score"
            
            if smooth_col not in df.columns:
                continue
            
            g_min = df[smooth_col].min()
            g_max = df[smooth_col].max()
            
            if g_max == g_min:
                df_norm[score_col] = 0.0
            else:
                df_norm[score_col] = ((df[smooth_col] - g_min) / (g_max - g_min)) * 100
            
            df_norm[score_col] = df_norm[score_col].round(2)
        
        return df_norm
    
    
    def normalize_zscore(self, df):
        """Z-Score normalization (mean=0, std=1)."""
        print("Applying Z-Score normalization...")
        
        df_norm = df.copy()
        
        for metric in self.metrics:
            smooth_col = f"{metric}_smooth"
            score_col = f"{metric}_score"
            
            if smooth_col not in df.columns:
                continue
            
            g_mean = df[smooth_col].mean()
            g_std = df[smooth_col].std()
            
            if g_std == 0:
                df_norm[score_col] = 0.0
            else:
                df_norm[score_col] = (df[smooth_col] - g_mean) / g_std
            
            df_norm[score_col] = df_norm[score_col].round(3)
        
        return df_norm
    
    
    def normalize_with_log(self, indicators):
        """
        Full pipeline: log transform → smooth → normalize.
        
        Parameters
        ----------
        indicators : list
            List of metric names to include (unused, for compatibility)
        
        Returns
        -------
        pd.DataFrame
            Normalized scores
        """
        # Load data
        df = pd.read_csv(self.input_path)
        df['date'] = pd.to_datetime(df['date'])
        
        # Step 1: Log transform
        df = self.apply_log_transform(df)
        
        # Step 2: Smooth
        df = self.smooth_data(df)
        
        # Step 3: Normalize
        if self.method == 'minmax':
            df_norm = self.normalize_minmax(df)
        elif self.method == 'zscore':
            df_norm = self.normalize_zscore(df)
        else:
            raise ValueError(f"Unknown method: {self.method}")
        
        # Keep only necessary columns
        score_cols = [f"{m}_score" for m in self.metrics]
        result = df_norm[['date', 'protocol'] + score_cols]
        
        return result

# Standalone execution

if __name__ == "__main__":
    CURRENT_SCRIPT = Path(__file__).resolve()
    PROJECT_ROOT = CURRENT_SCRIPT.parent.parent.parent
    
    INPUT_FILE = PROJECT_ROOT / 'data' / 'processed' / 'clean_dataset_final.csv'
    OUTPUT_DIR = PROJECT_ROOT / 'data' / 'processed'
    
    # Min-Max version
    normalizer_mm = OptimizedNormalizer(
        input_path=str(INPUT_FILE),
        output_dir=str(OUTPUT_DIR),
        method='minmax'
    )
    df_mm = normalizer_mm.normalize_with_log([])
    output_mm = OUTPUT_DIR / 'normalized_minmax_log.csv'
    df_mm.to_csv(output_mm, index=False)
    print(f"\n✓ Saved: {output_mm}")
    
    # Z-Score version
    normalizer_z = OptimizedNormalizer(
        input_path=str(INPUT_FILE),
        output_dir=str(OUTPUT_DIR),
        method='zscore'
    )
    df_z = normalizer_z.normalize_with_log([])
    output_z = OUTPUT_DIR / 'normalized_zscore_log.csv'
    df_z.to_csv(output_z, index=False)
    print(f"✓ Saved: {output_z}")