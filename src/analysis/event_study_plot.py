"""
Event Study & Cross-Sectional Stability Analysis
Visualizes the responsiveness of the DAI composite index vs. TVL in Equal weights [20%, 20%, 20%, 20%, 20%].
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')

# Configuration & Styling
sns.set_theme(style="whitegrid", context="paper", font_scale=1.2)

# 5 Core Dimensions
DIMENSIONS = [
    'D1_Capital',
    'D2_Liquidity',
    'D3_User_Activity',
    'D4_Operational_Output',
    'D5_Financial'
]

# Equal weights (baseline specification for primary DAI)
# D1=20%, D2=20%, D3=20%, D4=20%, D5=20%
WEIGHTS_EQUAL = [0.20, 0.20, 0.20, 0.20, 0.20]

# Recommended protocols with meaningful events
EVENTS = {
    'lido': [
        ('2025-08-05', 'Flash Crash'),
        ('2025-10-31', 'Ethereum Upgrade')
    ],
    'aave_v3': [
        ('2025-08-05', 'Flash Crash'),
        ('2025-11-15', 'Crypto Market Sell-off')
    ]
}


def get_project_root():
    return Path(__file__).resolve().parent.parent.parent


def load_and_prep_data(project_root):
    """Load data and calculate DAI_EQUAL with Equal weights."""
    data_path = project_root / 'data' / 'analysis' / 'final_index_with_mcap.csv'

    if not data_path.exists():
        raise FileNotFoundError(f"File not found: {data_path}")

    df = pd.read_csv(data_path)
    df['date'] = pd.to_datetime(df['date'])

    # Calculate DAI_EQUAL using Equal weights
    # Weights: D1_Capital=20%, D2_Liquidity=20%, D3_User_Activity=20%, D4_Output=20%, D5_Financial=20%
    df['DAI_EQUAL'] = df[DIMENSIONS].dot(WEIGHTS_EQUAL)

    return df


def min_max_scale(series):
    """Scale series to [0, 100] for visual comparison."""
    return (series - series.min()) / (series.max() - series.min()) * 100


def plot_event_study(df, protocol, events, output_dir):
    """Generate a dual-axis event study plot with statistical annotations."""
    proto_df = df[df['protocol'] == protocol].copy().sort_values('date')

    if len(proto_df) == 0:
        print(f"  [WARN] No data for {protocol}")
        return None

    # Calculate correlation statistics
    corr_dai = proto_df['DAI_EQUAL'].corr(proto_df['mcap'])
    corr_tvl = proto_df['tvl_score'].corr(proto_df['mcap'])
    dai_vol = proto_df['DAI_EQUAL'].std()
    tvl_vol = proto_df['tvl_score'].std()
    dai_adv = corr_dai - corr_tvl

    # Smooth series with 7-day rolling average
    proto_df['mcap_smooth'] = proto_df['mcap'].rolling(window=7, min_periods=1).mean()
    proto_df['dai_smooth'] = proto_df['DAI_EQUAL'].rolling(window=7, min_periods=1).mean()
    proto_df['tvl_smooth'] = proto_df['tvl_score'].rolling(window=7, min_periods=1).mean()

    # Create figure with two subplots (stacked)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10),
                                    gridspec_kw={'height_ratios': [1, 1], 'hspace': 0.3})

    # ========================================
    # Panel A: DAI vs Market Cap
    # ========================================
    ax1_twin = ax1.twinx()

    # Market Cap (grey area)
    ax1.fill_between(proto_df['date'], proto_df['mcap_smooth'] / 1e9,
                     alpha=0.25, color='gray')
    ax1.plot(proto_df['date'], proto_df['mcap_smooth'] / 1e9,
             color='gray', linewidth=2, alpha=0.8, label='Market Cap')
    ax1.set_ylabel('Market Cap ($B)', color='gray', fontsize=11, fontweight='bold')
    ax1.tick_params(axis='y', labelcolor='gray')

    # DAI_EQUAL (green solid)
    ax1_twin.plot(proto_df['date'], proto_df['dai_smooth'],
                  color='#2E7D32', linewidth=2.5,
                  label=f'DAI Index (r={corr_dai:.3f})')
    ax1_twin.set_ylabel('DAI Index', color='#2E7D32', fontsize=11, fontweight='bold')
    ax1_twin.tick_params(axis='y', labelcolor='#2E7D32')

    # Event lines
    for date_str, label in events:
        event_date = pd.to_datetime(date_str)
        if proto_df['date'].min() <= event_date <= proto_df['date'].max():
            ax1.axvline(event_date, color='red', linestyle='--', linewidth=1.5, alpha=0.6)
            y_pos = ax1.get_ylim()[1] * 0.85
            ax1.annotate(label, xy=(event_date, y_pos), fontsize=9,
                        rotation=45, ha='left', color='red', fontweight='bold')

    ax1.set_title(f'{protocol.upper()} — Panel A: DAI vs Market Cap\n'
                  f'Correlation: {corr_dai:.3f} | Volatility: {dai_vol:.2f}',
                  fontweight='bold', fontsize=12, pad=10)
    ax1.legend(loc='upper left', frameon=True)

    # Panel B: TVL vs Market Cap
    ax2_twin = ax2.twinx()

    # Market Cap (grey area)
    ax2.fill_between(proto_df['date'], proto_df['mcap_smooth'] / 1e9,
                     alpha=0.25, color='gray')
    ax2.plot(proto_df['date'], proto_df['mcap_smooth'] / 1e9,
             color='gray', linewidth=2, alpha=0.8, label='Market Cap')
    ax2.set_ylabel('Market Cap ($B)', color='gray', fontsize=11, fontweight='bold')
    ax2.set_xlabel('Date', fontsize=11)
    ax2.tick_params(axis='y', labelcolor='gray')

    # TVL Score (red dashed)
    ax2_twin.plot(proto_df['date'], proto_df['tvl_smooth'],
                  color='#C62828', linewidth=2.5, linestyle='--',
                  label=f'TVL Score (r={corr_tvl:.3f})')
    ax2_twin.set_ylabel('TVL Score', color='#C62828', fontsize=11, fontweight='bold')
    ax2_twin.tick_params(axis='y', labelcolor='#C62828')

    # Event lines
    for date_str, label in events:
        event_date = pd.to_datetime(date_str)
        if proto_df['date'].min() <= event_date <= proto_df['date'].max():
            ax2.axvline(event_date, color='red', linestyle='--', linewidth=1.5, alpha=0.6)

    ax2.set_title(f'{protocol.upper()} — Panel B: TVL vs Market Cap\n'
                  f'Correlation: {corr_tvl:.3f} | Volatility: {tvl_vol:.2f}',
                  fontweight='bold', fontsize=12, pad=10)
    ax2.legend(loc='upper left', frameon=True)

    # Format x-axis
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.xticks(rotation=45)

    # Main title
    fig.suptitle(f'Event Study: DAI Index vs TVL — {protocol.upper()}\n'
                 f'Equal Weights [D1=20%, D2=20%, D3=20%, D4=20%, D5=20%]',
                 fontsize=14, fontweight='bold', y=1.02)

    plt.tight_layout()

    # Save
    output_path = output_dir / f'event_study_{protocol}.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()

    print(f"  Generated: {output_path.name}")
    print(f"    DAI-MCAP: r={corr_dai:.3f} | TVL-MCAP: r={corr_tvl:.3f} | DAI Advantage: {dai_adv:+.3f}")

    return {
        'protocol': protocol,
        'corr_dai': corr_dai,
        'corr_tvl': corr_tvl,
        'dai_advantage': dai_adv
    }


def run_event_study():
    print("\nEvent Study Generator")

    project_root = get_project_root()
    df = load_and_prep_data(project_root)

    plots_dir = project_root / 'data' / 'analysis' / 'plots'
    plots_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for protocol, event_list in EVENTS.items():
        if protocol in df['protocol'].unique():
            result = plot_event_study(df, protocol, event_list, plots_dir)
            if result:
                results.append(result)
        else:
            print(f"  [WARN] Protocol '{protocol}' not found in data.")

    # Summary
    print("\nEvent Study Summary:")
    print(f"{'Protocol':<15} | {'DAI r':<10} | {'TVL r':<10} | {'DAI Advantage':<15}")
    for r in results:
        print(f"{r['protocol']:<15} | {r['corr_dai']:>+.3f}      | {r['corr_tvl']:>+.3f}      | {r['dai_advantage']:>+.3f}")


if __name__ == "__main__":
    run_event_study()
