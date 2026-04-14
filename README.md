# DeFi Activity Index (DAI)

A multi-dimensional composite index for measuring DeFi protocol vitality beyond Total Value Locked (TVL).

---

## Overview

This project constructs a **DeFi Activity Index (DAI)** that captures protocol vitality across **five dimensions**:

| Dimension | Indicators | Weight |
|-----------|------------|--------|
| D1: Capital Scale | TVL | 20% |
| D2: Liquidity | Operational Output / TVL | 20% |
| D3: User Activity | DAU, Transaction Count | 20% |
| D4: Operational Output | Core Utility (protocol-specific) | 20% |
| D5: Financial Performance | Fees, Revenue | 20% |

**Coverage:** 6 protocols (Aave V3, Compound V3, Uniswap V3, Curve, MakerDAO, Lido)

**Period:** February 26, 2025 - February 26, 2026 (daily frequency)

**Key Design:** Liquidity dimension is **separated** from Capital to avoid mathematical correlation.

---

## Quick Start

### Requirements

- Python 3.11+
- Dependencies listed in `requirements.txt`

### Installation

```bash
pip install -r requirements.txt
```

### Run Index Pipeline

```bash
python main.py
```

### Run Analysis Scripts

```bash
# Construct validity analysis (correlation with market cap)
python src/analysis/construct_validity.py

# Granger causality - predictive power
python src/analysis/predict_power_analysis.py

# Weight optimization and sensitivity analysis
python src/analysis/weight_optimization.py

# Robustness check (Z-Score vs Min-Max)
python src/analysis/robustness_check.py

# Event study visualization
python src/analysis/event_study_plot.py
```

---

## Project Structure

```
defi-activity-index/
├── main.py                          # Index construction pipeline
├── src/
│   ├── data_loader/
│   │   └── defillama.py             # DeFiLlama API data fetcher
│   ├── data_processor/
│   │   ├── cleaner.py               # Data cleaning and merging
│   │   └── feature_engineer.py      # Feature engineering (liquidity metrics)
│   └── index_builder/
│       ├── normalizer_optimized.py   # Normalization (Log + Min-Max/Z-Score)
│       └── aggregator_optimized.py    # Index aggregation (5-dimension weighted sum)
│   └── analysis/
│       ├── construct_validity.py     # Correlation analysis with market cap
│       ├── predict_power_analysis.py  # Granger causality tests
│       ├── weight_optimization.py    # Sensitivity analysis for weights
│       ├── robustness_check.py       # Normalization method robustness
│       └── event_study_plot.py      # Event study visualization
├── data/
│   ├── raw/                         # Raw data (API + manual)
│   ├── processed/                  # Cleaned and normalized data
│   ├── final/                      # Final index output
│   └── analysis/                    # Analysis results
└── config/
    ├── config.py                    # Configuration (API keys)
    └── constants.py                 # Protocol mappings
```

---

## Output Files

### Index Pipeline Outputs

| File | Description |
|------|-------------|
| `data/processed/clean_dataset_final.csv` | Cleaned dataset with engineered features |
| `data/processed/normalized_minmax_log.csv` | Normalized scores (Min-Max, primary) |
| `data/processed/normalized_zscore_log.csv` | Normalized scores (Z-Score, backup) |
| `data/final/final_index_5dim.csv` | Final composite index with dimension breakdown |

### Analysis Outputs

| File | Description |
|------|-------------|
| `data/analysis/final_index_with_mcap.csv` | Index merged with market cap data |
| `data/analysis/construct_validity_results.json` | Correlation analysis results |
| `data/analysis/granger_causality_summary.csv` | Granger test summary |
| `data/analysis/robustness_check_results.csv` | Robustness comparison |
| `data/analysis/plots/*.png` | Scatter plots and event study figures |

---

## Pipeline Stages

### Index Construction (main.py)

1. **Stage 1: Data Collection** - Fetch TVL, fees, revenue, volume from DeFiLlama API
2. **Stage 2: Data Cleaning & Feature Engineering** - Merge data, handle missing values, compute liquidity metrics
3. **Stage 3: Normalization** - Log transform + Min-Max/Z-Score scaling
4. **Stage 4: Index Aggregation** - 5-dimension weighted sum

### Analysis (independent scripts)

- **Construct Validity**: Pearson/Spearman correlation with market cap
- **Predictive Power**: Granger causality tests
- **Weight Optimization**: Compare Equal/Fundamental/PCA weights
- **Robustness Check**: Verify results across normalization methods
- **Event Study**: Visualize DAI vs TVL during market events

---

## 5-Dimension Framework

### Liquidity Dimension Indicators

| Protocol Type | Liquidity Indicator | Formula |
|---------------|---------------------|---------|
| Lending (Aave, Compound) | Borrow Utilization Ratio | Total Borrowed / TVL |
| DEX (Uniswap, Curve) | Trading Liquidity Utilization | Trading Volume / TVL |
| Stablecoin (MakerDAO) | Capital Deployment Ratio | Circulating Supply / TVL |
| LSD (Lido) | Staking Liquidity Flow | Net Inflow / TVL |

### Protocol-Specific Metrics

| Protocol | Type | Liquidity Indicator | Core Utility |
|----------|------|---------------------|--------------|
| Aave V3 | Lending | Borrow Utilization | Total Borrowed |
| Compound V3 | Lending | Borrow Utilization | Total Borrowed |
| Uniswap V3 | DEX | Trading Liquidity | Trading Volume |
| Curve | DEX | Trading Liquidity | Trading Volume |
| MakerDAO | Stablecoin | Capital Deployment | Circulating Supply |
| Lido | LSD | Staking Flow | Net Inflow |

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| v1.0 | 2026-02-26 | Initial framework with 4 dimensions |
| v2.0 | 2026-03-17 | Updated to 5 dimensions; Liquidity = Throughput/TVL |
| v2.1 | 2026-04-07 | Unified Liquidity = Operational Output / TVL; Lido uses Staking Flow |
| v3.0 | 2026-04-09 | Added construct validity, Granger causality, weight optimization, and event study |
