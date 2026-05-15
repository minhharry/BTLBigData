import os
import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection settings
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "app_database")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "your_secure_password")

# Set page config
st.set_page_config(page_title="Water Quality Dashboard", layout="wide")

@st.cache_data(ttl=60)
def fetch_data():
    """Fetch all data from the region_daily_averages table."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        query = """
            SELECT region, sample_material_type, determinand_label, unit,
                   window_start, window_end, avg_result, std_result, num_samples, updated_at
            FROM region_daily_averages
            ORDER BY window_start;
        """
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        cursor.close()
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return pd.DataFrame()

# Main app layout
st.title("💧 Water Quality Daily Averages Dashboard")
st.markdown("Visualize data produced by the `region_consumer.py` pipeline.")

# Fetch data
df = fetch_data()

if df.empty:
    st.warning("No data found in the database or failed to connect. Please ensure `region_consumer.py` is running and has populated the database.")
else:
    # Ensure window_start is datetime
    df['window_start'] = pd.to_datetime(df['window_start'])

    st.header("Data Exploration")
    
    # 1. Region selection
    region_counts = df.groupby('region')['num_samples'].sum().sort_values(ascending=False)
    regions = [f"{r} ({int(c)})" for r, c in region_counts.items()]
    
    select_all_regions = st.checkbox("Select All Regions")
    
    if select_all_regions:
        selected_region_strs = regions
    else:
        selected_region_strs = st.multiselect("1. Select Region(s)", regions)
    
    if selected_region_strs:
        selected_regions = [s.rsplit(' (', 1)[0] for s in selected_region_strs]
        region_df = df[df['region'].isin(selected_regions)]
        
        # 2. Sample Material Type selection
        material_counts = region_df.groupby('sample_material_type')['num_samples'].sum().sort_values(ascending=False)
        materials = [f"{m} ({int(c)})" for m, c in material_counts.items()]
        selected_material_str = st.selectbox("2. Select a Sample Material Type", [""] + materials)
        
        if selected_material_str:
            selected_material = selected_material_str.rsplit(' (', 1)[0]
            material_df = region_df[region_df['sample_material_type'] == selected_material]
            
            # 3. Determinand Label selection
            determinand_counts = material_df.groupby('determinand_label')['num_samples'].sum().sort_values(ascending=False)
            determinands = [f"{d} ({int(c)})" for d, c in determinand_counts.items()]
            selected_determinand_str = st.selectbox("3. Select a Determinand Label", [""] + determinands)
            
            if selected_determinand_str:
                selected_determinand = selected_determinand_str.rsplit(' (', 1)[0]
                final_df = material_df[material_df['determinand_label'] == selected_determinand]
                
                st.subheader(f"Results for {selected_determinand}")
                
                # Plot
                selected_regions_label = "Selected Regions" if len(selected_regions) > 1 else selected_regions[0]
                
                # Extract unit for labeling
                unit_label = final_df['unit'].iloc[0] if not final_df.empty and pd.notna(final_df['unit'].iloc[0]) else "Unit"
                
                fig = px.line(
                    final_df, 
                    x='window_start', 
                    y='avg_result', 
                    color='region',
                    title=f"{selected_determinand} Levels over Time in {selected_regions_label} ({selected_material})",
                    markers=True,
                    error_y='std_result',
                    hover_data=['unit', 'num_samples', 'std_result'],
                    labels={
                        'avg_result': f'Average Result ({unit_label})', 
                        'window_start': 'Date'
                    }
                )
                st.plotly_chart(fig, width="stretch")
                
                # Data table
                with st.expander("View Raw Data"):
                    st.dataframe(final_df.sort_values('window_start', ascending=False))

# Footer
st.markdown("---")
st.caption("Data is cached for 60 seconds. Refresh the page to fetch the latest data.")
