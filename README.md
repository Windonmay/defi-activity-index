# DeFi Activity Index (TVL+)

A multi-dimensional composite index for measuring DeFi protocol vitality beyond Total Value Locked (TVL).

---

## Overview

This project constructs a **DeFi Activity Index** that captures protocol "prosperity" across four dimensions:

| Dimension | Indicators | Weight |
|-----------|------------|--------|
| D1: Capital Efficiency | TVL, Capital Turnover | 25% |
| D2: User Activity | DAU, Transaction Count | 25% |
| D3: Operational Output | Core Utility (context-specific) | 25% |
| D4: Financial Performance | Fees, Revenue | 25% |

**Coverage:** 6 protocols (Aave V3, Compound V3, Uniswap V3, Curve, MakerDAO, Lido)  
**Period:** February 26, 2025 – February 26, 2026 (daily frequency)

---

## Quick Start

### Requirements

- Python 3.11+
- Dependencies listed in `requirements.txt`

### Installation

```bash
pip install -r requirements.txt
```
### Run Pipeline

```bash
python main.py
```

### Output Files

| File | Description |
|-----------|------------|
| data/processed/clean_dataset_final.csv | Cleaned dataset with engineered features |
| data/processed/normalized_minmax_log.csv | Normalized indicator scores |
| data/final/final_index_optimized.csv | Final composite index |

### Methodology
### Pipeline Stages

Stage 1: Data Collection (DeFiLlama API + Dune Analytics)
<br>Stage 2: Data Cleaning & Feature Engineering
<br>Stage 3: Normalization (Log + Min-Max)
<br>Stage 4: Index Aggregation (Weighted Sum)

### Protocols

| Protocol | Type | Core Utility Metric |
|-----------|------------|--------|
| Aave V3 |	Lending |	Outstanding borrows |
| Compound V3 | Lending |	Outstanding borrows |
| Uniswap V3 |	DEX |	Trading volume |
| Curve |	DEX |	Trading volume |
| MakerDAO|	Stablecoin|	DAI supply |
| Lido|	Liquid Staking |	ETH staked |
