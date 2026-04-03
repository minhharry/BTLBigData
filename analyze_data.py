"""
Analyze the UK Water Quality observations CSV dataset.
Source: https://environment.data.gov.uk/water-quality
"""

import pandas as pd
import json

CSV_PATH = "data/observations-2026-4-3-sorted.csv"

print("=" * 70)
print("UK WATER QUALITY OBSERVATIONS - DATA ANALYSIS")
print("=" * 70)

# --- 1. Load with chunked reading for memory efficiency ---
print("\n[1] Loading data (this may take a moment for ~1.8M rows)...")
df = pd.read_csv(CSV_PATH, low_memory=False)
print(f"    Total rows: {len(df):,}")
print(f"    Total columns: {len(df.columns)}")
print(f"    Columns: {list(df.columns)}")

# --- 2. Basic shape and dtypes ---
print("\n[2] Data types:")
for col in df.columns:
    print(f"    {col}: {df[col].dtype}  (nulls: {df[col].isna().sum():,})")

# --- 3. Time range ---
print("\n[3] Time range:")
df['phenomenonTime'] = pd.to_datetime(df['phenomenonTime'], errors='coerce')
print(f"    Earliest observation: {df['phenomenonTime'].min()}")
print(f"    Latest observation:   {df['phenomenonTime'].max()}")

# --- 4. Regional coverage ---
print("\n[4] Regional coverage:")
region_counts = df['samplingPoint.region'].value_counts()
print(f"    Number of regions: {region_counts.shape[0]}")
for region, count in region_counts.items():
    print(f"    {region}: {count:,} observations ({count/len(df)*100:.1f}%)")

# --- 5. Areas breakdown ---
print("\n[5] Top 15 areas by observation count:")
area_counts = df['samplingPoint.area'].value_counts().head(15)
for area, count in area_counts.items():
    print(f"    {area}: {count:,}")

# --- 6. Sampling point types ---
print("\n[6] Sampling point types:")
type_counts = df['samplingPoint.samplingPointType'].value_counts()
for stype, count in type_counts.items():
    print(f"    {stype}: {count:,} ({count/len(df)*100:.1f}%)")

# --- 7. Sample material types ---
print("\n[7] Sample material types:")
material_counts = df['sampleMaterialType'].value_counts()
for mat, count in material_counts.items():
    print(f"    {mat}: {count:,} ({count/len(df)*100:.1f}%)")

# --- 8. Sampling purpose ---
print("\n[8] Sampling purposes:")
purpose_counts = df['samplingPurpose'].value_counts()
for purpose, count in purpose_counts.items():
    print(f"    {purpose}: {count:,} ({count/len(df)*100:.1f}%)")

# --- 9. Determinands (what is being measured) ---
print("\n[9] Top 30 determinands (parameters measured):")
det_counts = df['determinand.prefLabel'].value_counts().head(30)
for det, count in det_counts.items():
    print(f"    {det}: {count:,}")

# --- 10. Total unique determinands ---
print(f"\n    Total unique determinands: {df['determinand.prefLabel'].nunique()}")

# --- 11. Units ---
print("\n[10] Measurement units:")
unit_counts = df['unit'].value_counts()
for unit, count in unit_counts.items():
    print(f"    {unit}: {count:,}")

# --- 12. Sampling points ---
print(f"\n[11] Unique sampling points: {df['samplingPoint.notation'].nunique():,}")
print(f"     Unique sampling point names: {df['samplingPoint.prefLabel'].nunique():,}")

# --- 13. Sampling point status ---
print("\n[12] Sampling point statuses:")
status_counts = df['samplingPoint.samplingPointStatus'].value_counts()
for status, count in status_counts.items():
    print(f"    {status}: {count:,}")

# --- 14. Numeric result analysis ---
print("\n[13] Numeric result analysis:")
# Many results start with '<' (below detection limit) or are text codes
result_col = df['result'].astype(str)
below_detection = result_col.str.startswith('<').sum()
text_results = result_col[~result_col.str.match(r'^[\d\.\-]+$') & ~result_col.str.startswith('<')].shape[0]
numeric_results = result_col.str.match(r'^[\d\.\-]+$').sum()
print(f"    Numeric results: {numeric_results:,} ({numeric_results/len(df)*100:.1f}%)")
print(f"    Below detection limit ('<'): {below_detection:,} ({below_detection/len(df)*100:.1f}%)")
print(f"    Text/coded results: {text_results:,} ({text_results/len(df)*100:.1f}%)")

# --- 15. Monthly distribution ---
print("\n[14] Monthly distribution of observations:")
df['month'] = df['phenomenonTime'].dt.to_period('M')
month_counts = df.groupby('month').size().sort_index()
for month, count in month_counts.items():
    print(f"    {month}: {count:,}")

# --- 16. Geographic extent ---
print("\n[15] Geographic extent:")
print(f"    Longitude range: {df['samplingPoint.longitude'].min():.4f} to {df['samplingPoint.longitude'].max():.4f}")
print(f"    Latitude range:  {df['samplingPoint.latitude'].min():.4f} to {df['samplingPoint.latitude'].max():.4f}")

# --- 17. PFAS / "forever chemicals" analysis ---
print("\n[16] PFAS (Forever chemicals) observations:")
pfas_mask = df['determinand.prefLabel'].str.contains(
    'perfluoro|PFOS|PFOA|fluorotelomer|GenX|ADONA|sulfonamid', 
    case=False, na=False
)
pfas_count = pfas_mask.sum()
print(f"    Total PFAS-related observations: {pfas_count:,}")
if pfas_count > 0:
    pfas_determinands = df.loc[pfas_mask, 'determinand.prefLabel'].value_counts()
    print(f"    Unique PFAS determinands: {len(pfas_determinands)}")
    for det, count in pfas_determinands.head(10).items():
        print(f"      {det}: {count:,}")

# --- 18. Key water quality stats for numeric determinands ---
print("\n[17] Key water quality parameter statistics (numeric values only):")
key_params = [
    'pH', 'Temperature of Water', 'BOD : 5 Day ATU', 
    'Ammoniacal Nitrogen as N', 'Oxygen, Dissolved, % Saturation',
    'Oxygen, Dissolved as O2', 'Nitrate as N', 
    'Orthophosphate, reactive as P', 'Conductivity at 25 C'
]
for param in key_params:
    param_data = df[df['determinand.prefLabel'] == param]['result']
    param_numeric = pd.to_numeric(param_data, errors='coerce').dropna()
    if len(param_numeric) > 0:
        unit_val = df[df['determinand.prefLabel'] == param]['unit'].mode().iloc[0] if not df[df['determinand.prefLabel'] == param]['unit'].empty else 'N/A'
        print(f"\n    {param} [{unit_val}]:")
        print(f"      Count: {len(param_numeric):,}")
        print(f"      Mean:  {param_numeric.mean():.3f}")
        print(f"      Std:   {param_numeric.std():.3f}")
        print(f"      Min:   {param_numeric.min():.3f}")
        print(f"      25%:   {param_numeric.quantile(0.25):.3f}")
        print(f"      50%:   {param_numeric.quantile(0.50):.3f}")
        print(f"      75%:   {param_numeric.quantile(0.75):.3f}")
        print(f"      Max:   {param_numeric.max():.3f}")

print("\n" + "=" * 70)
print("ANALYSIS COMPLETE")
print("=" * 70)
