# DeFi Activity Index (TVL+)

A multi-dimensional composite index for measuring DeFi protocol vitality beyond Total Value Locked (TVL).

---

## Overview

This project constructs a **DeFi Activity Index** that captures protocol "prosperity" across **five dimensions** (v2.1):

| Dimension | Indicators | Weight |
|-----------|------------|--------|
| D1: Capital Scale | TVL | 20% |
| D2: Liquidity | Liquidity = Operational Output / TVL | 20% |
| D3: User Activity | DAU, Transaction Count | 20% |
| D4: Operational Output | Core Utility (context-specific) | 20% |
| D5: Financial Performance | Fees, Revenue | 20% |

**Coverage:** 6 protocols (Aave V3, Compound V3, Uniswap V3, Curve, MakerDAO, Lido)
**Period:** February 26, 2025 – February 26, 2026 (daily frequency)

**Key Design Principle:** The Liquidity dimension is **separated** from Capital to avoid mathematical correlation (Liquidity = Operational Output / TVL contains TVL in denominator).

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
| data/final/final_index_5dim.csv | Final composite index (5-dimension) |

### Pipeline Stages

Stage 1: Data Collection (DeFiLlama API + Tokenterminal)
<br>Stage 2: Data Cleaning & Feature Engineering
<br>Stage 3: Normalization (Log + Min-Max)
<br>Stage 4: Index Aggregation (Weighted Sum, 5-Dimension)

---

## 5-Dimension Framework (v2.1)

### Liquidity Dimension Indicators

| Protocol Type | Liquidity Indicator | Formula |
|---------------|----------------------|---------|
| Lending (Aave, Compound) | Borrow Utilization Ratio | Total Borrowed / TVL |
| DEX (Uniswap, Curve) | Trading Liquidity Utilization | Trading Volume / TVL |
| Stablecoin (MakerDAO) | Capital Deployment Ratio | Circulating Supply / TVL |
| LSD (Lido) | Staking Liquidity Flow | Net Inflow / TVL |

> Note: Net Inflow is **derived** from `diff(assets_staked)`: Net Inflow = Staked Assets - Staked Assets (previous day)

---

## Protocol-Specific Metrics

| Protocol | Type | Liquidity Indicator | Core Utility |
|-----------|------------|--------|------------|
| Aave V3 | Lending | Borrow Utilization | Total Borrowed |
| Compound V3 | Lending | Borrow Utilization | Total Borrowed |
| Uniswap V3 | DEX | Trading Liquidity | Trading Volume |
| Curve | DEX | Trading Liquidity | Trading Volume |
| MakerDAO | Stablecoin | Capital Deployment | Circulating Supply |
| Lido | LSD | Staking Flow | **Net Inflow** (derived from diff of staked assets) |

---

## Version History

| Version | Date | Changes |
|:--------|:-----|:--------|
| v1.0 | 2026-02-26 | Initial framework with 4 dimensions |
| v2.0 | 2026-03-17 | Updated to 5 dimensions; Defined Liquidity as Throughput/TVL |
| v2.1 | 2026-04-02 | Unified Liquidity = Operational Output / TVL; Lido uses Staking Flow (Net Inflow / TVL) |
