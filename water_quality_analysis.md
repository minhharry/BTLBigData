# 🌊 UK Water Quality Observations — Data Analysis & Insights

> **Source:** [environment.data.gov.uk/water-quality](https://environment.data.gov.uk/water-quality)
> **File:** `observations-2026-4-3.csv` (~741 MB)
> **Analysis Date:** 2026-04-03

---

## 1. Dataset Overview

| Metric | Value |
|---|---|
| **Total observations** | **1,808,301** |
| **Columns** | 17 |
| **Time span** | 2025-04-04 → 2026-02-26 (~11 months) |
| **Unique sampling points** | 16,799 |
| **Unique determinands (parameters)** | 996 |
| **Regions covered** | 7 (all of England) |
| **Geographic extent** | Lon: -5.70 to 1.79 / Lat: 49.89 to 55.82 |

### Columns

| Column | Type | Description |
|---|---|---|
| `id` | string | Unique observation URL/ID |
| `samplingPoint.notation` | string | Sampling point code |
| `samplingPoint.prefLabel` | string | Human-readable site name |
| `samplingPoint.longitude/latitude` | float | GPS coordinates |
| `samplingPoint.region` | string | EA region (7 total) |
| `samplingPoint.area` | string | Sub-region area |
| `samplingPoint.subArea` | string | Further area subdivision |
| `samplingPoint.samplingPointStatus` | string | OPEN / CLOSED |
| `samplingPoint.samplingPointType` | string | Type of water body / discharge |
| `phenomenonTime` | datetime | When the sample was taken |
| `samplingPurpose` | string | Why the sample was taken |
| `sampleMaterialType` | string | Type of material sampled |
| `determinand.notation` | int | Numeric code for parameter |
| `determinand.prefLabel` | string | Name of parameter measured |
| `result` | string | Measurement value (may include `<` prefix or text) |
| `unit` | string | Unit of measurement |

---

## 2. Regional Distribution

The data covers all 7 Environment Agency regions across England:

| Region | Observations | Share |
|---|---|---|
| **SouthWest** | 378,127 | 20.9% |
| **NorthEast** | 277,790 | 15.4% |
| **Anglian** | 267,062 | 14.8% |
| **NorthWest** | 246,326 | 13.6% |
| **Midlands** | 235,383 | 13.0% |
| **Southern** | 216,089 | 11.9% |
| **Thames** | 187,524 | 10.4% |

> [!IMPORTANT]
> **SouthWest dominates** with ~21% of all observations — driven by the Devon & Cornwall area alone (251K obs). This likely reflects the region's extensive coastline (bathing water monitoring) and river network.

---

## 3. What's Being Sampled? (Water Body Types)

| Category | Observations | Share |
|---|---|---|
| Freshwater Rivers | 935,195 | **51.7%** |
| Sewage - Final/Treated Effluent (Water Co.) | 169,039 | 9.3% |
| Groundwater - Borehole | 121,726 | 6.7% |
| Bathing Beaches (Saline) | 115,234 | 6.4% |
| Lakes/Ponds/Reservoirs | 109,495 | 6.1% |

> [!NOTE]
> **Over half of all observations** come from freshwater rivers — making this dataset primarily a **river water quality** dataset. The second largest category is sewage treatment works output, reflecting regulatory compliance monitoring.

---

## 4. Sample Material Types

| Material | Observations | Share |
|---|---|---|
| River / Running Surface Water | 1,040,788 | **57.6%** |
| Final Sewage Effluent | 180,212 | 10.0% |
| Sea Water | 144,868 | 8.0% |
| Groundwater | 140,374 | 7.8% |
| Pond / Lake / Reservoir Water | 119,298 | 6.6% |
| Estuarine Water | 57,395 | 3.2% |

---

## 5. Sampling Purpose

| Purpose | Share |
|---|---|
| National Agency Policy Monitoring | **32.3%** |
| EU Directive Statutory Monitoring | **30.9%** |
| UK Govt Policy Monitoring | 12.2% |
| Planned Local Investigation | 7.5% |
| Operator Self-Monitoring Compliance | 7.2% |
| UWWTD Monitoring | 3.4% |
| Compliance Audit (Permit) | 1.9% |
| Pollution Incident Response | 2.6% |

> [!TIP]
> **~63% of sampling is routine regulatory monitoring** (national + EU), making this dataset excellent for **trend analysis** of baseline water quality across England.

---

## 6. Top Parameters Measured (Determinands)

Out of **996 unique determinands**, the most commonly measured are:

| Rank | Parameter | Count |
|---|---|---|
| 1 | BOD : 5 Day ATU | 79,749 |
| 2 | Ammoniacal Nitrogen as N | 78,803 |
| 3 | Temperature of Water | 75,952 |
| 4 | Solids, Suspended at 105 C | 60,359 |
| 5 | pH | 58,362 |
| 6 | Dissolved Oxygen (% Saturation) | 56,328 |
| 7 | Orthophosphate, reactive as P | 51,661 |
| 8 | Total Oxidised Nitrogen | 51,266 |
| 9 | Nitrite as N | 50,097 |
| 10 | Dissolved Oxygen (as O2) | 50,032 |

Other notable parameters include: **E. coli** (15,559), **Intestinal Enterococci** (14,840), **Salinity** (14,792), **DOC** (14,238), **Iron** (15,690).

---

## 7. Result Types

| Category | Count | Share |
|---|---|---|
| Numeric results | 1,197,626 | **66.2%** |
| Below detection limit (`<`) | 521,831 | **28.9%** |
| Text/coded results | 88,844 | 4.9% |

> [!WARNING]
> **Nearly 29% of results are below detection limits** (prefixed with `<`). For analysis, these need special handling — commonly imputed as half the detection limit or treated as censored data.

---

## 8. Key Water Quality Parameter Statistics

### Core Parameters Summary

| Parameter | Unit | Count | Mean | Median | Min | Max |
|---|---|---|---|---|---|---|
| **pH** | pH units | 58,338 | 7.75 | 7.81 | 2.89 | 14.00 |
| **Temperature** | C | 75,952 | 12.90 | 13.00 | 0.00 | 46.60 |
| **BOD (5-day ATU)** | mg/L | 59,628 | 45.74 | 4.32 | 0.00 | 22,000 |
| **Ammoniacal Nitrogen** | mg/L | 41,640 | 2.16 | 0.17 | 0.00 | 1,300 |
| **Dissolved O2 (% sat)** | % | 56,328 | 90.04 | 94.20 | 0.00 | 447 |
| **Dissolved O2** | mg/L | 50,031 | 9.72 | 9.99 | 0.01 | 108.9 |
| **Nitrate as N** | mg/L | 46,035 | 5.27 | 3.50 | 0.00 | 1,177 |
| **Orthophosphate** | mg/L | 45,469 | 0.28 | 0.07 | 0.001 | 190 |
| **Conductivity** | uS/cm | 46,243 | 850.6 | 576.0 | 0.00 | 58,518 |

### Key Insights from Parameter Statistics

> [!IMPORTANT]
> **BOD has extreme outliers**: Mean of 45.7 mg/L but median of only 4.3 mg/L, with a max of 22,000 mg/L. The massive gap between mean and median signals **severe organic pollution events** at some sites (sewage/industrial). Normal river BOD should be less than 5 mg/L.

> [!WARNING]
> **Ammoniacal Nitrogen** shows concerning levels: median 0.17 mg/L (acceptable) but max of 1,300 mg/L and 75th percentile at 1.1 mg/L. Values above 0.6 mg/L indicate poor water quality. This confirms **ammonia pollution** is a significant issue at many monitoring points.

> [!NOTE]
> **Dissolved Oxygen** levels are generally healthy (median 94.2% saturation) — values above 80% are considered good for aquatic life. However, minimum values of 0% indicate **anoxic conditions** at some locations.

> [!TIP]
> **Nitrate levels** (median 3.5 mg/L as N) are moderate. The WHO drinking water limit is 11.3 mg/L as N, and values above 5.65 mg/L indicate increased eutrophication risk. The 75th percentile (7.4 mg/L) shows a quarter of samples are approaching concerning levels — likely agricultural runoff.

---

## 9. PFAS ("Forever Chemicals") Monitoring

| Metric | Value |
|---|---|
| **Total PFAS observations** | **60,519** |
| **Unique PFAS compounds tested** | **60** |

### Top 10 PFAS Compounds Monitored

| Compound | Observations |
|---|---|
| Perfluorooctylsulphonate anion (PFOS) | 1,818 |
| Perfluorooctanoate anion (PFOA) | 1,768 |
| Perfluorohexanoic acid (PFHxA) | 1,226 |
| Perfluorodecanoic acid (PFDA) | 1,225 |
| Perfluoropentanoic acid (PFPeA) | 1,223 |
| Perfluorooctanoic acid (PFOA) | 1,220 |
| Perfluoroundecanoic acid (PFUnDA) | 1,218 |
| Perfluorododecanoic acid (PFDoDA) | 1,204 |
| Perfluoro-3-methoxypropanoic acid | 1,200 |
| Perfluoro-3,6-dioxaheptanoic acid | 1,200 |

> [!IMPORTANT]
> **60 PFAS compounds** are being actively monitored across **60,519 observations** — this is significant. The UK Environment Agency is clearly investing heavily in "forever chemicals" surveillance. PFOS and PFOA (the two most well-known PFAS) have the highest sampling frequency.

---

## 10. Temporal Distribution

| Month | Observations |
|---|---|
| 2025-04 | 146,925 |
| 2025-05 | 185,962 |
| 2025-06 | 183,106 |
| **2025-07** | **204,981** (peak) |
| 2025-08 | 189,636 |
| 2025-09 | 192,298 |
| 2025-10 | 195,382 |
| 2025-11 | 187,294 |
| 2025-12 | 142,290 |
| 2026-01 | 114,983 |
| 2026-02 | 65,444 *(partial)* |

> [!NOTE]
> **July 2025 has the highest sampling volume** (205K), driven by bathing water season monitoring. Winter months show reduced sampling (~115K-142K). February 2026 appears partial (only ~65K), suggesting data collection was still ongoing.

---

## 11. Summary of Key Findings

1. **Massive nationwide dataset**: 1.8M observations across 16,799 sampling points covering all of England
2. **River-dominated**: 52% of observations are from freshwater rivers, making this primarily a river health monitoring dataset
3. **Pollution hotspots exist**: Extreme BOD outliers (up to 22,000 mg/L) and ammonia values (up to 1,300 mg/L) flag serious pollution at some sites
4. **Generally good oxygen levels**: Median dissolved oxygen at 94.2% suggests most waterbodies are healthy, but pockets of anoxia exist
5. **Nitrate concern**: 25% of samples exceed 7.4 mg/L nitrate-N, indicating eutrophication risk from agricultural inputs
6. **Active PFAS monitoring**: 60 PFAS compounds tracked — a new priority for UK regulators
7. **Nearly 29% of results are below detection limits** — important for analytical pipelines
8. **996 different parameters** measured — extremely comprehensive chemical/biological profiling
9. **Seasonal patterns**: Summer peaks in sampling align with bathing water seasons
10. **SouthWest region** has the most intensive monitoring coverage

---

## 12. Potential Use Cases for This Data

- **Trend analysis**: Track water quality changes month-over-month across regions
- **Pollution detection**: Identify sites with abnormal BOD, ammonia, or nitrate levels
- **PFAS mapping**: Spatial analysis of PFAS contamination hotspots
- **Regulatory compliance**: Compare sewage effluent readings against permit limits
- **Big Data pipeline**: Feed into Kafka/Spark (aligning with your existing `produce.py` / `consume.py` setup)
- **Machine Learning**: Predict water quality classes, detect anomalies, or forecast pollution events
