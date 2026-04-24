# DeFi Activity Index - Methodology Overview

## Overview

The DeFi Activity Index (DAI) is a composite indicator that measures the multi-dimensional vitality of decentralized finance protocols. Instead of relying solely on TVL (Total Value Locked), the index combines five key dimensions to provide a more comprehensive assessment of protocol health.

## Five-Dimension Framework

| Dimension | What It Measures | Key Indicator |
|-----------|-----------------|--------------|
| D1: Capital Scale | Total capital locked in the protocol | TVL |
| D2: Liquidity | How efficiently capital is being utilized | Liquidity Metric (Core Utility / TVL) |
| D3: User Activity | User engagement and adoption | DAU + Transaction Count |
| D4: Operational Output | Core business throughput | Core Utility (protocol-specific) |
| D5: Financial Performance | Revenue generation ability | Fees + Revenue |

Each dimension contributes equally (20%) to the final index score.

## Indicators Explained

### Core Utility (D4) - Protocol-Specific

Different protocol types have different core functions:

| Protocol Type | Core Utility Metric |
|--------------|-------------------|
| DEX (Uniswap, Curve) | Trading Volume |
| Lending (Aave, Compound) | Outstanding Borrows |
| Liquid Staking (Lido) | Net Inflow |
| Stablecoin (MakerDAO) | Circulating Supply |

### Liquidity Metric (D2)

Liquidity measures capital efficiency: how much economic activity is generated per unit of capital locked.

```
Liquidity = Core Utility / TVL
```

For example:
- DEX: Trading Volume / TVL (how much is traded relative to pool size)
- Lending: Borrowed Amount / TVL (utilization rate)
- LSD: Net Inflow / TVL (staking flow rate)

### Why Separate Capital and Liquidity?

A critical design choice: **TVL and Liquidity are in separate dimensions**.

**Problem:** If both were in the same dimension, when TVL increases (with constant Core Utility), the TVL score goes up but Liquidity score goes down. This creates signal cancellation.

**Solution:** Separate them into independent dimensions (D1 and D2), each weighted at 20%.

## Data Processing Pipeline

### Step 1: Data Collection
- **Sources:** DeFiLlama API (TVL, fees, revenue), TokenTerminal (DAU, transactions)
- **Period:** February 26, 2025 - February 26, 2026
- **Frequency:** Daily

### Step 2: Missing Value Treatment
1. Forward fill (carry last known value forward)
2. Backward fill (fill remaining gaps backward)
3. Zero fill (remaining gaps set to zero)

### Step 3: Log Transformation
Applied to skewed metrics (TVL, DAU, fees, etc.) to compress extreme values.
- Not applied to ratio metrics (Liquidity is already bounded)

### Step 4: Smoothing
7-day rolling average to reduce daily noise while preserving trends.

### Step 5: Min-Max Normalization
Scales all indicators to [0, 100] range using the formula:

```
Normalized Score = (Value - Min) / (Max - Min) × 100
```

Min and Max are computed across all protocols and dates.

## Output Files

| File | Description |
|------|-------------|
| `final_index_5dim.csv` | Main output with composite index and dimension scores |
| `normalized_minmax_log.csv` | Normalized scores before aggregation |
| `clean_dataset_final.csv` | Merged raw data with derived features |

## Protocols Covered

| Protocol | Type |
|----------|------|
| Aave V3 | Lending |
| Compound V3 | Lending |
| Uniswap V3 | DEX |
| Curve | DEX |
| MakerDAO | Stablecoin |
| Lido | Liquid Staking |