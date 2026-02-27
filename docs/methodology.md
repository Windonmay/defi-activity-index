# Index Construction Methodology

## 1. Introduction

This document specifies the methodology for constructing the DeFi Activity Index (DAI), a composite indicator designed to capture the multi-dimensional vitality of decentralized finance protocols. The index moves beyond Total Value Locked (TVL) as a singular metric, incorporating capital efficiency, user engagement, operational throughput, and financial sustainability into a unified, transparent, and reproducible measurement framework.

---

## 2. Indicator Framework

### 2.1 Conceptual Foundation

The index is built upon four theoretically grounded dimensions that collectively capture protocol "prosperity":

| Dimension | Concept | Rationale |
|-----------|---------|-----------|
| **D1: Capital Efficiency** | How effectively is capital deployed? | Moves beyond raw TVL to assess capital utilization |
| **D2: User Activity** | How engaged is the user base? | Captures adoption breadth and interaction intensity |
| **D3: Operational Output** | What is the protocol's business throughput? | Measures core economic function regardless of fee structure |
| **D4: Financial Performance** | Can the protocol capture value? | Assesses revenue generation and long-term sustainability |

This four-dimension structure ensures that the index is not dominated by any single aspect of protocol health while maintaining conceptual coherence.

### 2.2 Indicator Selection

Seven indicators are selected across the four dimensions:

| Dimension | Indicator | Definition | Data Source |
|-----------|-----------|------------|-------------|
| D1: Capital | **TVL** | Total Value Locked in protocol contracts (USD) | DeFiLlama API |
| D1: Capital | **Capital Turnover** | Core Utility ÷ TVL; measures capital utilization rate | Derived |
| D2: User | **DAU** | Daily Active Users; unique addresses with meaningful interactions | Dune Analytics |
| D2: User | **Tx Count** | Daily transaction count on protocol contracts | Dune Analytics |
| D3: Operational | **Core Utility** | Context-specific throughput metric (see Section 2.3) | DeFiLlama / Dune |
| D4: Financial | **Fees** | Total fees paid by users (LP fees + protocol fees) | DeFiLlama API |
| D4: Financial | **Revenue** | Protocol revenue accruing to treasury or token holders | DeFiLlama API |

### 2.3 Core Utility Metric Mapping

The Core Utility indicator is defined contextually to reflect each protocol type's primary economic function:

| Protocol Type | Core Utility Definition | Example Protocols |
|---------------|------------------------|-------------------|
| Decentralized Exchange (DEX) | Daily trading volume (USD) | Uniswap V3, Curve |
| Lending Protocol | Total outstanding borrows (USD) | Aave V3, Compound V3 |
| Liquid Staking Derivative (LSD) | Total assets staked (USD) | Lido |
| Stablecoin Issuer | Circulating supply (USD) | MakerDAO |

This mapping ensures that Core Utility captures the essential "work" performed by each protocol, enabling meaningful cross-protocol comparison.

---

## 3. Data Processing

### 3.1 Data Collection

Raw data is collected from two primary sources:

1. **DeFiLlama API**: TVL, trading volume, fees, and revenue time series
2. **TokenTerminal**: User activity metrics (DAU, transaction count) and protocol-specific data (active loans, staked assets)

The analysis covers the period from **February 26, 2025 – February 26, 2026** at daily frequency.

### 3.2 Missing Value Treatment

Missing values are handled through a three-step imputation strategy:

1. **Forward Fill (ffill)**: Propagate the last valid observation forward (max 3 days)
2. **Backward Fill (bfill)**: Fill remaining gaps with subsequent valid observations
3. **Zero Fill**: Replace any remaining NaN values with zero

This approach preserves temporal continuity while avoiding the introduction of artificial patterns.

### 3.3 Derived Feature: Capital Turnover

Capital Turnover Ratio quantifies how efficiently locked capital generates economic activity:

$$
\text{Capital Turnover}_t = \frac{\text{Core Utility}_t}{\text{TVL}_t}
$$

**Edge Case Handling:**
- Division by zero (TVL = 0): Set turnover to 0
- Both numerator and denominator zero: Set turnover to 0

**Interpretation:** Higher turnover indicates more productive capital utilization. DEX protocols typically exhibit turnover ratios of 0.1–0.5 (10–50% of TVL traded daily), while lending protocols show lower ratios (0.01–0.05) reflecting the longer-term nature of loan positions.

---

## 4. Normalization

### 4.1 Problem Statement

Raw indicators span vastly different scales:
- TVL: Billions of USD
- DAU: Thousands of users
- Capital Turnover: Decimal ratios (0.01–1.0)

Direct aggregation would be dominated by high-magnitude variables. Additionally, DeFi metrics exhibit severe right-skewness, where a few large protocols (e.g., Uniswap) dwarf smaller ones.

### 4.2 Log Transformation

To address skewness, we apply a log transformation to compress right-tailed distributions:

$$
x'_{i,t} = \log(1 + x_{i,t})
$$

**Applied to:** TVL, DAU, Tx Count, Core Utility, Fees, Revenue

**Not applied to:** Capital Turnover (already a bounded ratio)

The `log(1+x)` form (log1p) ensures stability when x = 0.

### 4.3 Smoothing

A 7-day rolling average is applied to reduce daily noise while preserving meaningful trends:

$$
\bar{x}_{i,t} = \frac{1}{7} \sum_{k=0}^{6} x'_{i,t-k}
$$

For observations with fewer than 7 preceding days, the available window is used (min_periods = 1).

### 4.4 Min-Max Scaling

After transformation and smoothing, indicators are scaled to a [0, 100] range using global min-max normalization:

$$
x^{norm}_{i,t} = \frac{\bar{x}_{i,t} - \min(\bar{x}_i)}{\max(\bar{x}_i) - \min(\bar{x}_i)} \times 100
$$

Where $\min(\bar{x}_i)$ and $\max(\bar{x}_i)$ are computed across all protocols and all dates.

**Justification:** Min-max normalization is the standard approach recommended by the OECD for composite indicators. It:
- Preserves the distribution shape
- Produces bounded scores directly interpretable as "percentage of observed range"
- Enables straightforward aggregation

### 4.5 Alternative: Z-Score Normalization

For robustness testing, Z-score normalization is also computed:

$$
x^{z}_{i,t} = \frac{\bar{x}_{i,t} - \mu_i}{\sigma_i}
$$

This alternative is reserved for sensitivity analysis (see Section 7).

---

## 5. Weighting Scheme

### 5.1 Design Principles

The weighting scheme adheres to two principles:

1. **Equal Dimension Weighting:** Each of the four dimensions receives 25% of total weight, reflecting the absence of strong theoretical priors for privileging one dimension over another.

2. **Equal Within-Dimension Weighting:** Indicators within a dimension share weight equally, unless the dimension contains only one indicator.

### 5.2 Weight Specification

| Dimension | Weight | Indicators | Per-Indicator Weight |
|-----------|--------|------------|---------------------|
| D1: Capital Efficiency | 25% | TVL, Capital Turnover | 12.5% each |
| D2: User Activity | 25% | DAU, Tx Count | 12.5% each |
| D3: Operational Output | 25% | Core Utility | 25% (standalone) |
| D4: Financial Performance | 25% | Fees, Revenue | 12.5% each |

**Total:** 100%

### 5.3 Design Rationale

**Isolating Core Utility in D3:**

A critical design choice is placing Core Utility as the sole indicator in D3 (Operational Output). This addresses a specific issue observed in preliminary analysis:

- Protocols like Compound V3 generate substantial lending activity (high Core Utility) but minimal explicit fees, as interest payments flow directly to lenders rather than the protocol.
- Under an alternative scheme where Core Utility and Fees share D3, Compound's strong operational performance would be diluted by its low fee generation.
- By isolating Core Utility, the index fairly credits protocols for their business throughput regardless of their fee structure.

**Combining Fees and Revenue in D4:**

D4 (Financial Performance) combines Fees and Revenue to capture value capture ability comprehensively:
- **Fees** reflect users' willingness to pay for protocol services
- **Revenue** reflects the protocol's ability to retain value from those fees

This structure distinguishes between operational scale (D3) and economic sustainability (D4).

---

## 6. Aggregation

### 6.1 Composite Index Formula

The composite index is computed as a weighted linear sum:

$$
I_{p,t} = \sum_{j=1}^{7} w_j \times x^{norm}_{j,p,t}
$$

Where:
- $I_{p,t}$ = Index score for protocol $p$ on date $t$
- $w_j$ = Weight for indicator $j$
- $x^{norm}_{j,p,t}$ = Normalized score for indicator $j$, protocol $p$, date $t$

**Index Range:** [0, 100], where 100 represents maximum observed vitality across all dimensions.

## 7. Output Specification

### 7.1 Primary Output

**File:** `data/final/final_index_optimized.csv`

| Column | Type | Description |
|--------|------|-------------|
| Date | date | Calendar date (YYYY-MM-DD) |
| Protocol | string | Protocol identifier |
| composite_index | float | Overall index score [0-100] |
| D1_Capital | float | Capital Efficiency sub-index |
| D2_User | float | User Activity sub-index |
| D3_Operational | float | Operational Output sub-index |
| D4_Financial | float | Financial Performance sub-index |
| TVL_score | float | Normalized TVL score |
| Capital_Turnover_score | float | Normalized turnover score |
| DAU_score | float | Normalized DAU score |
| Tx_Count_score | float | Normalized transaction count score |
| Core_Utility_score | float | Normalized core utility score |
| Fees_score | float | Normalized fees score |
| Revenue_score | float | Normalized revenue score |

### 7.2 Intermediate Outputs

| File | Description |
|------|-------------|
| `clean_dataset_final.csv` | Merged raw data with engineered features |
| `normalized_minmax_log.csv` | Log-transformed and Min-Max normalized scores |
| `normalized_zscore_log.csv` | Log-transformed and Z-score normalized scores |

---

## 8. Limitations

1. **Cross-Protocol Comparability:** While Core Utility mapping enables comparison, fundamental business model differences mean that a "50" for a DEX and a "50" for a lending protocol reflect different types of activity.

2. **Data Availability:** Manual data collection (DAU, Tx Count) may introduce measurement inconsistencies across protocols.

3. **Time Period Dependency:** Min-max normalization is sensitive to the observation window. Index scores are relative to the 2025–2026 period and should not be compared to scores from different time ranges without re-normalization.

4. **Equal Weighting Assumption:** The absence of empirical weights means the index reflects a balanced view rather than a predictive optimization.

---

## 9. References

  [1] Luo, Y., Feng, Y., Xu, J., & Tasca, P. (2024). Piercing the Veil of TVL: DeFi Reappraised. arXiv:2404.11745.

  [2] Metelski, D., & Sobieraj, J. (2022). Decentralized Finance (DeFi) Projects: A Study of Key Performance Indicators in Terms of DeFi Protocols' Valuations. *International Journal of Financial Studies*, 10(4), 108.

  [3] Chiu, J., Koeppl, T. V., Yu, H., & Zhang, S. (2023). Understanding DeFi Through the Lens of a Production-Network Model. Bank of Canada Staff Working Paper 2023-42.

---

## Appendix A: Protocol Coverage

| Protocol | Type | Core Utility Metric |
|----------|------|---------------------|
| Aave V3 | Lending | Outstanding borrows (USD) |
| Compound V3 | Lending | Outstanding borrows (USD) |
| Uniswap V3 | DEX | Daily trading volume (USD) |
| Curve Finance | DEX | Daily trading volume (USD) |
| MakerDAO | Stablecoin | DAI circulating supply (USD) |
| Lido | Liquid Staking | Total ETH staked (USD) |

---

## Appendix B: Weighting Scheme Summary

**DeFi Activity Index - Optimized Weighting Structure**

| Dimension | Total Weight | Indicator | Individual Weight | Rationale |
|-----------|--------------|-----------|-------------------|-----------|
| **D1: Capital Efficiency** | **25%** | | | Measures capital scale and utilization |
| | | TVL | 12.5% | Absolute capital commitment |
| | | Capital Turnover | 12.5% | Capital productivity (Utility/TVL) |
| **D2: User Activity** | **25%** | | | Captures adoption and engagement |
| | | DAU | 12.5% | User base breadth |
| | | Tx Count | 12.5% | Interaction intensity |
| **D3: Operational Output** | **25%** | | | Isolates core business throughput |
| | | Core Utility | 25.0% | Context-specific economic activity |
| **D4: Financial Performance** | **25%** | | | Assesses value capture ability |
| | | Fees | 12.5% | User willingness to pay |
| | | Revenue | 12.5% | Protocol value retention |
| **TOTAL** | **100%** | | | |