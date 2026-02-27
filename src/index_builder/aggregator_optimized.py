"""
aggregator_optimized.py
Index aggregation with optimized weighting scheme.

Key change: Separates Core_Utility into standalone dimension (D3),
moves Fees to Financial dimension (D4) alongside Revenue.
"""

import pandas as pd
from pathlib import Path


class OptimizedIndexAggregator:
    """
    Build composite index using optimized weights.
    
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
        
        # Optimized weights
        self.weights = {
            # D1: Capital Efficiency (25%)
            'tvl_score': 0.125,
            'capital_turnover_score': 0.125,
            
            # D2: User Activity (25%)
            'dau_score': 0.125,
            'tx_count_score': 0.125,
            
            # D3: Operational Output (25% - standalone)
            'core_utility_score': 0.25,
            
            # D4: Financial Performance (25%)
            'fees_score': 0.125,
            'revenue_score': 0.125
        }
        
        # Dimension grouping
        self.dimensions = {
            'D1_Capital': ['tvl_score', 'capital_turnover_score'],
            'D2_User': ['dau_score', 'tx_count_score'],
            'D3_Operational': ['core_utility_score'],
            'D4_Financial': ['fees_score', 'revenue_score']
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
        missing_cols = [col for col in self.weights.keys() if col not in df.columns]
        if missing_cols:
            # Try with capitalized column names (e.g., TVL_score instead of tvl_score)
            capitalized_weights = {k.replace('_score', '_score').upper(): v 
                                  for k, v in self.weights.items()}
            # Check if capitalized versions exist
            if all(col.upper() in [c.upper() for c in df.columns] for col in self.weights.keys()):
                # Rename df columns to lowercase
                df.columns = [c.lower() for c in df.columns]
            else:
                raise ValueError(f"Missing columns: {missing_cols}\nAvailable: {list(df.columns)}")
        
        print("Computing composite index...")
        
        # Compute composite index
        df['composite_index'] = 0.0
        for col, weight in self.weights.items():
            df['composite_index'] += df[col] * weight
        
        # Compute dimension scores
        for dim_name, cols in self.dimensions.items():
            dim_weight_sum = sum([self.weights[c] for c in cols])
            df[dim_name] = 0.0
            for col in cols:
                df[dim_name] += (df[col] * self.weights[col]) / dim_weight_sum
        
        # Round scores
        score_cols = ['composite_index'] + list(self.dimensions.keys())
        df[score_cols] = df[score_cols].round(2)
        
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
        print(top_protocols[['protocol', 'composite_index']].to_string(index=False))
        
        return df

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
    
    output_file = OUTPUT_DIR / 'final_index_optimized.csv'
    index_df.to_csv(output_file, index=False)
    print(f"\n✓ Saved: {output_file}")