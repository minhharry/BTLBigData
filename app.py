"""
Water Quality Monitoring Dashboard.
Real-time analysis and anomaly detection for water quality across England.
"""

import streamlit as st
import plotly.express as px
import pandas as pd
from db_manager import DatabaseManager

# Initialize Database Manager
db = DatabaseManager()

st.set_page_config(page_title="Water Quality Dashboard", layout="wide")

# --- UI Helper Functions ---

@st.cache_data(ttl=60)
def get_regions():
    return db.get_unique_regions()

@st.cache_data(ttl=60)
def get_materials(regions):
    return db.get_materials_for_regions(regions)

@st.cache_data(ttl=60)
def get_determinands(regions, material):
    return db.get_determinands_for_filter(regions, material)

@st.cache_data(ttl=60)
def get_historical(regions, material, determinand):
    return db.get_historical_data(regions, material, determinand)

@st.cache_data(ttl=60)
def get_predictions(regions, material, determinand, model_name=None):
    return db.get_predictions(regions, material, determinand, model_name)

@st.cache_data(ttl=60)
def get_anomalies(start_date, end_date, material, determinand):
    return db.get_anomalies(start_date, end_date, material, determinand)

@st.cache_data(ttl=60)
def get_gqa(start_date, end_date, material):
    return db.get_gqa_data(start_date, end_date, material)

# --- Main Dashboard ---

st.title("Water Quality Monitoring Dashboard")
st.markdown("Real-time analysis and anomaly detection for water quality across England.")

tab1, tab2, tab3, tab4 = st.tabs(["Historical Trends", "Anomaly Detection Map", "Regional GQA Map", "Model Performance Comparison"])

# --- Tab 1: Historical Trends ---
with tab1:
    st.header("Regional Historical Trends")
    
    region_df = get_regions()
    if region_df.empty:
        st.warning("No data found in the database. Please ensure the pipeline is running.")
    else:
        # 1. Select Regions
        region_options = [f"{r} ({int(c)})" for r, c in zip(region_df['region'], region_df['total_samples'])]
        selected_region_strs = st.multiselect("1. Select Region(s)", region_options)
        
        if selected_region_strs:
            selected_regions = [s.rsplit(' (', 1)[0] for s in selected_region_strs]
            
            # 2. Select Material
            material_df = get_materials(selected_regions)
            material_options = [f"{m} ({int(c)})" for m, c in zip(material_df['sample_material_type'], material_df['total_samples'])]
            selected_material_str = st.selectbox("2. Select a Sample Material Type", [""] + material_options)
            
            if selected_material_str:
                selected_material = selected_material_str.rsplit(' (', 1)[0]
                
                # 3. Select Determinand
                det_df = get_determinands(selected_regions, selected_material)
                det_options = [f"{d} ({int(c)})" for d, c in zip(det_df['determinand_label'], det_df['total_samples'])]
                selected_determinand_str = st.selectbox("3. Select a Determinand Label", [""] + det_options)
                
                if selected_determinand_str:
                    selected_determinand = selected_determinand_str.rsplit(' (', 1)[0]
                    
                    # Fetch final data
                    final_df = get_historical(selected_regions, selected_material, selected_determinand)
                    
                    if not final_df.empty:
                        st.subheader(f"Results for {selected_determinand}")
                        
                        # Prediction controls
                        show_predictions = st.checkbox("Show Predictions", value=True)
                        available_models = db.get_available_models()
                        selected_model = None
                        if show_predictions and available_models:
                            selected_model = st.selectbox("Prediction Model", available_models)
                        
                        unit_label = final_df['unit'].iloc[0] if pd.notna(final_df['unit'].iloc[0]) else "Unit"
                        
                        fig = px.line(
                            final_df, 
                            x='window_start', 
                            y='avg_result', 
                            color='region',
                            title=f"{selected_determinand} Levels over Time",
                            markers=True,
                            error_y='std_result',
                            hover_data=['unit', 'num_samples', 'std_result'],
                            labels={'avg_result': f'Avg ({unit_label})', 'window_start': 'Date'}
                        )
                        
                        if show_predictions and selected_model:
                            pred_df = get_predictions(selected_regions, selected_material, selected_determinand, selected_model)
                            if not pred_df.empty:
                                # Calculate metrics
                                metrics = db.get_model_performance_metrics(selected_regions, selected_material, selected_determinand, selected_model)
                                if metrics:
                                    st.subheader(f"Model Performance: {selected_model}")
                                    m_col1, m_col2, m_col3, m_col4 = st.columns(4)
                                    m_col1.metric("MSE", f"{metrics['mse']:.4f}")
                                    m_col2.metric("RMSE", f"{metrics['rmse']:.4f}")
                                    m_col3.metric("R² Score", f"{metrics['r2']:.4f}")
                                    m_col4.metric("Samples", metrics['count'])

                                pred_df['target_date'] = pd.to_datetime(pred_df['target_date'])
                                latest_preds = pred_df.sort_values('prediction_date', ascending=False).groupby(['region', 'target_date']).first().reset_index()
                                
                                for region in selected_regions:
                                    region_preds = latest_preds[latest_preds['region'] == region]
                                    if not region_preds.empty:
                                        fig.add_scatter(
                                            x=region_preds['target_date'],
                                            y=region_preds['predicted_value'],
                                            mode='lines+markers',
                                            name=f'{region} ({selected_model} Pred)',
                                            line=dict(dash='dash'),
                                        )

                        st.plotly_chart(fig, width='stretch')
                        
                        with st.expander("View Raw Data"):
                            st.dataframe(final_df.sort_values('window_start', ascending=False))

# --- Tab 2: Anomaly Detection Map ---
with tab2:
    st.header("Anomalous Activity Across England")
    
    meta = db.get_anomaly_metadata()
    if meta['min_date'] is None:
        st.info("No anomalies detected yet.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            all_dates = pd.date_range(start=meta['min_date'], end=meta['max_date']).date.tolist()
            if len(all_dates) > 1:
                date_range = st.select_slider("Select Date Range", options=all_dates, value=(min(all_dates), max(all_dates)))
                start_date, end_date = date_range
            else:
                start_date = end_date = all_dates[0]
                st.info(f"Showing data for {start_date}")
        
        with col2:
            mat_options = [f"{m} ({c})" for m, c in zip(meta['materials']['sample_material_type'], meta['materials']['count'])]
            selected_material_map_str = st.selectbox("Select Material Type", ["All"] + mat_options, key="ano_mat")
            selected_material_map = selected_material_map_str.rsplit(' (', 1)[0] if selected_material_map_str != "All" else "All"
            
            det_options = [f"{d} ({c})" for d, c in zip(meta['determinands']['determinand_label'], meta['determinands']['count'])]
            selected_determinand_map_str = st.selectbox("Select Determinand", ["All"] + det_options, key="ano_det")
            selected_determinand_map = selected_determinand_map_str.rsplit(' (', 1)[0] if selected_determinand_map_str != "All" else "All"

        anomaly_df = get_anomalies(start_date, end_date, selected_material_map, selected_determinand_map)
        
        if anomaly_df.empty:
            st.warning("No anomalies found for the selected filters.")
        else:
            fig_map = px.scatter_map(
                anomaly_df, lat="latitude", lon="longitude", color="z_score",
                size=anomaly_df["z_score"].abs(),
                color_continuous_scale=px.colors.diverging.RdBu_r,
                hover_name="station_name",
                hover_data={"window_start": True, "avg_result": True, "unit": True, "z_score": ":.2f", "determinand_label": True},
                zoom=5.5, center={"lat": 52.8, "lon": -1.5}, height=700,
                title=f"Anomalies from {start_date} to {end_date}"
            )
            fig_map.update_layout(map_style="carto-positron")
            st.plotly_chart(fig_map, width='stretch')
            st.subheader("Anomaly Details")
            st.dataframe(anomaly_df.sort_values("z_score", ascending=False))

# --- Tab 3: Regional GQA Map ---
with tab3:
    st.header("Regional General Quality Assessment (GQA)")
    
    gqa_meta = db.get_gqa_metadata()
    if gqa_meta['min_date'] is None:
        st.info("No GQA data available yet.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            all_dates_gqa = pd.date_range(start=gqa_meta['min_date'], end=gqa_meta['max_date']).date.tolist()
            if len(all_dates_gqa) > 1:
                date_range_gqa = st.select_slider("Select Date Range (GQA)", options=all_dates_gqa, value=(min(all_dates_gqa), max(all_dates_gqa)), key="gqa_date")
                start_date_gqa, end_date_gqa = date_range_gqa
            else:
                start_date_gqa = end_date_gqa = all_dates_gqa[0]
                st.info(f"Showing data for {start_date_gqa}")
        
        with col2:
            gqa_mat_options = [f"{m} ({c})" for m, c in zip(gqa_meta['materials']['sample_material_type'], gqa_meta['materials']['count'])]
            selected_material_gqa_str = st.selectbox("Select Material Type", ["All"] + gqa_mat_options, key="gqa_mat")
            selected_material_gqa = selected_material_gqa_str.rsplit(' (', 1)[0] if selected_material_gqa_str != "All" else "All"

        gqa_df = get_gqa(start_date_gqa, end_date_gqa, selected_material_gqa)
        
        if gqa_df.empty:
            st.warning("No GQA data found for the selected filters.")
        else:
            gqa_df = gqa_df.sort_values("gqa_grade")
            color_map = {'A': '#2ecc71', 'B': '#27ae60', 'C': '#f1c40f', 'D': '#e67e22', 'E': '#e74c3c', 'F': '#95a5a6'}
            
            fig_gqa = px.scatter_map(
                gqa_df, lat="latitude", lon="longitude", color="gqa_grade",
                color_discrete_map=color_map, category_orders={"gqa_grade": ["A", "B", "C", "D", "E", "F"]},
                zoom=5.5, center={"lat": 52.8, "lon": -1.5}, hover_name="region",
                hover_data={"window_start": True, "gqa_grade": True, "do_value": ":.2f", "bod_value": ":.2f", "ammonia_value": ":.2f"},
                height=700, title=f"Regional GQA Grades from {start_date_gqa} to {end_date_gqa}"
            )
            fig_gqa.update_layout(map_style="carto-positron")
            fig_gqa.update_traces(marker=dict(size=20))
            st.plotly_chart(fig_gqa, width='stretch')
            st.subheader("Regional GQA Details")
            st.dataframe(gqa_df)

# --- Tab 4: Model Performance Comparison ---
with tab4:
    st.header("Overall Model Performance Comparison")
    st.markdown("""
        Compare the predictive accuracy of all AI models against each other and a **Persistence Baseline** 
        (which simply predicts that tomorrow's value will be the same as today's).
    """)
    
    perf_df = db.get_overall_model_performance()
    
    if perf_df.empty:
        st.info("Insufficient data to calculate overall performance metrics yet.")
    else:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Performance Metrics Summary")
            st.dataframe(perf_df.style.highlight_min(subset=['MSE', 'RMSE'], color='lightgreen')
                                    .highlight_max(subset=['R2 Score'], color='lightgreen'), 
                         width='stretch')
            
        with col2:
            st.subheader("MSE Comparison (Lower is better)")
            fig_mse = px.bar(perf_df, x='Model', y='MSE', color='Model', 
                             title="Mean Squared Error by Model")
            st.plotly_chart(fig_mse, width='stretch')
            
        st.divider()
        
        col3, col4 = st.columns(2)
        with col3:
            st.subheader("R² Score Comparison (Higher is better)")
            fig_r2 = px.bar(perf_df, x='Model', y='R2 Score', color='Model', 
                            title="R² Score by Model")
            st.plotly_chart(fig_r2, width='stretch')
            
        with col4:
            st.subheader("Evaluation Coverage")
            fig_samples = px.pie(perf_df, names='Model', values='Samples', 
                                 title="Data Points used for Evaluation")
            st.plotly_chart(fig_samples, width='stretch')

st.markdown("---")
st.caption("Data is cached for 60 seconds.")
