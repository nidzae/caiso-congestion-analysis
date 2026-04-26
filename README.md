# CAISO Congestion & Reconductoring Opportunity Analysis

Interactive analysis of 12 months of CAISO transmission congestion data (April 2025 - March 2026) cross-referenced against the CAISO Transmission Plan and SB1006 compliance reports to identify where reconductoring with advanced conductors could unlock curtailed renewable energy in California.

**Live dashboard:** [nidzae.github.io/caiso-congestion-analysis](https://nidzae.github.io/caiso-congestion-analysis/)

## What This Analysis Does

1. **Pulls 12 months of real operational congestion data** from the CAISO OASIS API (Day-Ahead Market shadow prices for all binding transmission constraints)
2. **Stack-ranks the most severely constrained transmission lines** by total shadow price sum and estimated congestion cost
3. **Cross-references each constraint** against the CAISO 2024-2025 Transmission Plan and PG&E/SCE SB1006 compliance reports to determine whether interventions (reconductoring, DLR, new build) have been proposed, approved, or rejected
4. **Builds an interactive dashboard** that visualizes the results with filtering, sorting, and detailed constraint-level analysis

## Key Findings

- **441 binding constraints** identified across the CAISO grid over 12 months
- **320 are reconductoring candidates** (branches at 230 kV or below, likely thermally limited)
- Of the top 10 most congested lines, **3 have no proposed intervention** from any source (genuinely overlooked), **4 were studied but deferred or rejected**, **2 are acknowledged but explicitly deprioritized**, and **1 has an approved reconductoring project**
- The Path 15 corridor (Moss Landing, Los Banos, Panoche, Gates) accounts for an estimated **$336M/year in congestion costs** from actual market operations — consistent with the TPP's modeled $389M (2034) to $522M (2039)
- Sub-transmission lines (60-70 kV) in PG&E Kern and SDG&E are among the most persistently congested elements but are structurally invisible in the DLAP-level metrics that drive planning decisions

## Data Sources

| Source | Description | Access |
|--------|-------------|--------|
| CAISO OASIS API | Day-Ahead Market shadow prices (PRC_NOMOGRAM report) | Public, no auth: `http://oasis.caiso.com/oasisapi/SingleZip` |
| CAISO 2024-2025 Transmission Plan | Modeled congestion costs, approved projects, economic alternatives studied | [ISO Board Approved Plan (PDF)](https://stakeholdercenter.caiso.com/RecurringStakeholderProcesses/2024-2025-Transmission-planning-process) |
| PG&E SB1006 Compliance Report | 14 DLR candidate lines, 6 reconductoring candidates | Filed Dec 2025 per SB1006 requirements |
| SCE SB1006 Compliance Report | 11 DLR candidates, 56 reconductoring candidates screened, 2 proposed | Filed Dec 2025 per SB1006 requirements |
| CAISO DMM 2024 Annual Report | Independent market monitor validation data (Table 5.3: top 25 DAM constraints) | [DMM Annual Report (PDF)](https://www.caiso.com/documents/2024-annual-report-on-market-issues-and-performance-aug-07-2025.pdf) |

## Methodology

### Shadow Price Data (OASIS API)

The OASIS `PRC_NOMOGRAM` report provides hourly shadow prices for every binding transmission constraint in the Day-Ahead Market. A shadow price of $X/MWh on a constraint means: "If you could add 1 more MW of capacity to this line, the market would save $X in that hour."

- **Download window:** April 1, 2025 to April 1, 2026 (12 months)
- **API chunking:** 13 sequential requests of ~30 days each (API max is 31 days)
- **Deduplication:** When the same constraint binds under multiple contingency scenarios in the same hour (different `CONSTRAINT_CAUSE` and `GROUP` values), we take the maximum shadow price and count it as one hour. This affected 1,017 of 70,495 raw rows.
- **Result:** 69,478 deduplicated constraint-hours across 441 unique constraints

### Ranking Metrics

**Shadow Price Index (cost_index):** Sum of all shadow prices across all binding hours for a constraint. Units are $/MWh-hours. This is a ranking proxy, not actual dollars.

**Estimated Congestion Cost:** cost_index x estimated binding flow (MW). The binding flow is estimated as 70% of the thermal rating for the line's voltage class (the 70% derating accounts for N-1 contingency margins, stability limits, and voltage constraints that keep actual flow below the thermal limit). Voltage-to-rating mapping:

| Voltage | Thermal Rating | Est. Binding Flow (70%) |
|---------|---------------|------------------------|
| 60-70 kV | 75-100 MW | 53-70 MW |
| 115 kV | 250 MW | 175 MW |
| 230 kV | 700 MW | 490 MW |
| 500 kV | 2,000 MW | 1,400 MW |

**Validation:** Our Path 15 corridor estimated cost ($336M) falls between the TPP's 2034 modeled cost ($389M) and a trajectory consistent with current buildout, providing reasonable confidence in these assumptions.

### Station Name Mapping

OASIS constraint IDs use internal bus numbers and abbreviated station names (e.g., `30750_MOSSLD  _230_30797_LASAGUIL_230_BR_1 _1`). We map these to readable names using:
1. **24 known mappings** from DMM reports and the CAISO Transmission Plan (manually verified)
2. **130+ station abbreviation lookups** (e.g., MOSSLD = Moss Landing, LOSCOCHS = Los Coches)
3. **Utility determination** from constraint cause code prefixes: PG1/PG2 = PG&E, SC1/SC2 = SCE, SD1/SD2 = SDG&E

### Reconductoring Classification

A constraint is classified as a reconductoring candidate if:
- Element type is "branch" (not transformer or nomogram)
- Voltage is 230 kV or below (500 kV lines are typically stability-limited, not thermally limited)

### Cross-Reference (Intervention Status)

For the top 25 constraints, we manually researched whether any intervention has been proposed, approved, or rejected by checking:
- CAISO TPP Chapter 2 (approved projects), Table 4.8-1 (areas studied), Table 4.9-1 (economic alternatives)
- PG&E SB1006 Table 1 (DLR candidates), Table 2 (reconductoring candidates)
- SCE SB1006 Table 3 (DLR candidates), Table 5 (reconductoring proposals)

Constraints not manually researched are labeled "not_searched" (not defaulted to "no intervention").

### Important Caveats

- **Ranking metric vs DMM:** We rank by shadow price sum; the DMM ranks by DLAP price impact (shadow price x shift factors). Sub-transmission lines (69-115 kV) rank high in our analysis but low in DMM because they have small shift factors to aggregate load prices. Both metrics are valid for different purposes.
- **Estimated costs are estimates:** Actual line ratings vary by conductor type, ambient conditions, and span length. The 70% derating is a system-wide average. For authoritative dollar figures, use CAISO TPP Table 4.6-1.
- **Base Case vs Contingency:** A constraint with "Base Case" cause is directly thermally overloaded. A contingency cause (e.g., "PG1 MOSSLD-LOSBNS 500") means the line overloads when another element trips. Both are real congestion but have different implications for reconductoring effectiveness.

## Repository Structure

```
caiso-analysis/
├── README.md                          # This file
├── data/
│   ├── oasis_raw/                     # 13 raw OASIS API response CSVs (one per ~30-day chunk)
│   │   ├── chunk_00_20250401_20250501.csv
│   │   ├── chunk_01_20250501_20250531.csv
│   │   └── ... (through chunk_12)
│   ├── shadow_prices_12mo.csv         # Consolidated 12-month dataset (70,495 rows)
│   ├── congestion_ranked.csv          # Aggregated + deduplicated + ranked (441 constraints)
│   ├── congestion_enriched.csv        # Ranked + station names + utility + reconductoring classification
│   ├── known_mappings.json            # 24 manually verified constraint-to-name mappings
│   ├── cross_reference.json           # Intervention status from TPP and SB1006 reports
│   └── visualization_data.json        # Final merged dataset for the dashboard
├── scripts/
│   ├── download_oasis.py              # Step 1: Pull shadow prices from CAISO OASIS API
│   ├── aggregate.py                   # Step 2: Deduplicate, aggregate, and rank constraints
│   ├── download_cec.py                # Step 2A: Download CEC substations (API was unavailable)
│   ├── map_stations.py                # Step 3: Map OASIS IDs to names, utilities, reconductoring classification
│   ├── merge_crossref.py              # Step 4: Merge with intervention data, compute estimated costs
│   └── build_standalone.py            # Step 5: Build self-contained HTML with embedded data
└── viz/
    ├── dashboard.html                 # Development dashboard (loads data.json via fetch)
    ├── data.json                      # Copy of visualization_data.json for local serving
    └── caiso-congestion-dashboard.html # Standalone dashboard with embedded data (for publishing)
```

## How to Reproduce

```bash
# Step 1: Download 12 months of shadow prices (~5 min, 13 API calls)
python scripts/download_oasis.py

# Step 2: Aggregate and rank
python scripts/aggregate.py

# Step 3: Map station names and classify reconductoring applicability
python scripts/map_stations.py

# Step 4: Merge with cross-reference data and compute estimated costs
python scripts/merge_crossref.py

# Step 5: Build standalone dashboard
python scripts/build_standalone.py

# Serve locally
cd viz && python3 -m http.server 8765
# Open http://localhost:8765/dashboard.html
```

Requirements: Python 3.8+ with `requests` (only external dependency).

## Verification

The analysis was verified through:

1. **Row-by-row comparison** of April 1, 2025 data between our download and a fresh API pull: 197 rows, zero mismatches
2. **Manual aggregation** of El Cajon-Los Coches constraint: all metrics match exactly (hours=4,090, cost_index=360,594.92, avg=88.17, max=1,328.85)
3. **DMM cross-validation:** 22 of 25 DMM top DAM constraints (2024 Annual Report Table 5.3) found in our data; 3 missing ones had zero rows in our time period
4. **Path 15 estimated cost validation:** Our $336M vs TPP modeled $389M (2034) / $522M (2039)
5. **Global sanity check:** 69,478 total constraint-hours across 441 constraints = ~8 constraints binding per hour on average (plausible)
6. **Chunk boundary verification:** No time overlaps or gaps at 30-day chunk seams (OPR_DT date overlap at boundaries is expected due to Pacific/GMT conversion)
