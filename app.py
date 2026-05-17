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
def get_predictions_unique_regions():
    """Fetch unique prediction regions with cache."""
    return db.get_predictions_unique_regions()

@st.cache_data(ttl=60)
def get_predictions_materials(regions):
    """Fetch unique prediction materials with cache."""
    return db.get_predictions_materials(regions)

@st.cache_data(ttl=60)
def get_predictions_determinands(regions, material):
    """Fetch unique prediction determinands with cache."""
    return db.get_predictions_determinands(regions, material)

@st.cache_data(ttl=60)
def get_overall_model_performance(regions, material, determinand):
    """Fetch overall model performance comparison with cache."""
    return db.get_overall_model_performance(regions, material, determinand)

@st.cache_data(ttl=60)
def get_anomalies(start_date, end_date, material, determinand):
    return db.get_anomalies(start_date, end_date, material, determinand)

@st.cache_data(ttl=60)
def get_total_station_records_count(start_date, end_date, material, determinand):
    return db.get_total_station_records_count(start_date, end_date, material, determinand)

@st.cache_data(ttl=60)
def get_gqa(start_date, end_date, material):
    return db.get_gqa_data(start_date, end_date, material)

@st.cache_data(ttl=60)
def get_station_history(station_id, material, determinand):
    return db.get_station_history(station_id, material, determinand)

# --- Main Dashboard ---

st.title("Water Quality Monitoring Dashboard")
st.markdown("Real-time analysis and anomaly detection for water quality across England.")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Historical Trends", 
    "Anomaly Detection Map", 
    "Regional GQA Map", 
    "Model Performance Comparison",
    "GQA Overall Statistics",
    "AI Predictable Groups Statistics"
])

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
        
        total_records = get_total_station_records_count(start_date, end_date, selected_material_map, selected_determinand_map)
        anomaly_count = len(anomaly_df)
        anomaly_rate = (anomaly_count / total_records * 100) if total_records > 0 else 0.0

        st.subheader("Anomaly Key Performance Indicators")
        kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
        kpi_col1.metric("Total Monitored Records", f"{total_records:,}")
        kpi_col2.metric("Detected Anomalies", f"{anomaly_count:,}")
        kpi_col3.metric("Anomaly Occurrence Rate", f"{anomaly_rate:.2f}%")

        if anomaly_df.empty:
            st.warning("No anomalies found for the selected filters.")
        else:
            # Create the map
            fig_map = px.scatter_map(
                anomaly_df, lat="latitude", lon="longitude", color="z_score",
                size=anomaly_df["z_score"].abs(),
                color_continuous_scale=px.colors.diverging.RdBu_r,
                hover_name="station_name",
                hover_data={
                    "window_start": True, 
                    "avg_result": True, 
                    "unit": True, 
                    "z_score": ":.2f", 
                    "determinand_label": True,
                    "station_id": True,
                    "sample_material_type": True
                },
                zoom=5.5, center={"lat": 52.8, "lon": -1.5}, height=600,
                title=f"Anomalies from {start_date} to {end_date}"
            )
            fig_map.update_layout(map_style="carto-positron")
            
            st.info("Click on a point on the map to view that station's history.")
            
            # Use on_select to capture clicks
            event_data = st.plotly_chart(fig_map, on_select="rerun", selection_mode="points", width='stretch')
            
            # Determine selected station
            selected_station_id = None
            selected_material = None
            selected_determinand = None
            selected_station_name = None

            # 1. Check if something was selected on the map
            if event_data and event_data.get("selection") and event_data["selection"].get("points"):
                point = event_data["selection"]["points"][0]
                point_idx = point["point_index"]
                row = anomaly_df.iloc[point_idx]
                selected_station_id = row['station_id']
                selected_material = row['sample_material_type']
                selected_determinand = row['determinand_label']
                selected_station_name = row['station_name']

            # 2. Provide a selectbox fallback/override
            st.divider()
            st.subheader("Station Historical Analysis")
            
            # Unique stations for the dropdown
            unique_stations = anomaly_df[['station_id', 'station_name', 'sample_material_type', 'determinand_label']].drop_duplicates()
            station_options = [
                f"{r['station_name']} | {r['determinand_label']} ({r['station_id']})" 
                for _, r in unique_stations.iterrows()
            ]
            
            # Sync selectbox with map selection if map was clicked
            default_index = 0
            if selected_station_id:
                target_str = f"{selected_station_name} | {selected_determinand} ({selected_station_id})"
                if target_str in station_options:
                    default_index = station_options.index(target_str) + 1

            selection_str = st.selectbox(
                "Or search/select a station manually:", 
                ["Select a station..."] + station_options,
                index=default_index
            )

            # If selectbox is used, update the selection variables
            if selection_str != "Select a station...":
                # Parse: "Name | Determinand (ID)"
                parts = selection_str.split(" | ")
                selected_station_name = parts[0]
                det_and_id = parts[1].rsplit(" (", 1)
                selected_determinand = det_and_id[0]
                selected_station_id = det_and_id[1].rstrip(")")
                
                # Find the material for this selection
                match = unique_stations[
                    (unique_stations['station_id'] == selected_station_id) & 
                    (unique_stations['determinand_label'] == selected_determinand)
                ]
                if not match.empty:
                    selected_material = match.iloc[0]['sample_material_type']

            # 3. Render History if a station is selected
            if selected_station_id:
                with st.spinner(f"Fetching history for {selected_station_name}..."):
                    history_df = get_station_history(selected_station_id, selected_material, selected_determinand)
                    
                if not history_df.empty:
                    st.success(f"Viewing history for: **{selected_station_name}** ({selected_determinand})")
                    
                    # Info columns
                    info_col1, info_col2, info_col3 = st.columns(3)
                    info_col1.metric("Station ID", selected_station_id)
                    info_col2.metric("Material", selected_material)
                    info_col3.metric("Total Samples", len(history_df))

                    # Plot History
                    unit_label = history_df['unit'].iloc[0] if not history_df.empty and pd.notna(history_df['unit'].iloc[0]) else "Result"
                    
                    fig_hist = px.line(
                        history_df, x='window_start', y='avg_result',
                        title=f"Historical Trends for {selected_station_name}",
                        labels={'avg_result': f'Value ({unit_label})', 'window_start': 'Date'},
                        markers=True
                    )
                    
                    # Highlight Anomalies
                    anomalies_only = history_df[history_df['is_anomaly']]
                    if not anomalies_only.empty:
                        fig_hist.add_scatter(
                            x=anomalies_only['window_start'],
                            y=anomalies_only['avg_result'],
                            mode='markers',
                            marker=dict(color='red', size=12, symbol='x'),
                            name='Detected Anomaly',
                            hovertext=[f"Z-Score: {z:.2f}" for z in anomalies_only['z_score']]
                        )
                    
                    st.plotly_chart(fig_hist, width='stretch')
                    
                    with st.expander("View Raw Station Data"):
                        st.dataframe(history_df.sort_values("window_start", ascending=False))
                else:
                    st.info("No historical data found for this specific station.")
            else:
                st.info("Select a station from the map or dropdown to view its history.")

            st.divider()
            st.subheader("Current Anomalies List")
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
            st.plotly_chart(fig_gqa, width="stretch")

            st.divider()
            st.subheader("Regional GQA Trends Over Time")

            # Create a copy for the trend chart to ensure temporal sorting
            trend_df = gqa_df.copy().sort_values("window_start")
            grade_map = {"A": 6, "B": 5, "C": 4, "D": 3, "E": 2, "F": 1}
            trend_df["gqa_score"] = trend_df["gqa_grade"].map(grade_map)

            fig_trend = px.line(
                trend_df,
                x="window_start",
                y="gqa_score",
                color="region",
                markers=True,
                title="GQA Grade Trends by Region",
                labels={"gqa_score": "GQA Grade", "window_start": "Date"},
                hover_data=["gqa_grade", "do_value", "bod_value", "ammonia_value"],
            )

            # Customize Y-axis to show A-F instead of 1-6
            fig_trend.update_layout(
                yaxis=dict(
                    tickmode="array",
                    tickvals=[1, 2, 3, 4, 5, 6],
                    ticktext=["F", "E", "D", "C", "B", "A"],
                )
            )

            st.plotly_chart(fig_trend, width="stretch")

            st.subheader("Regional GQA Details")
            st.dataframe(gqa_df)

# --- Tab 4: Model Performance Comparison ---
with tab4:
    st.header("Overall Model Performance Comparison")
    st.markdown("""
        Compare the predictive accuracy of all AI models against each other and a **Persistence Baseline** 
        (which simply predicts that tomorrow's value will be the same as today's).
    """)
    
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        pred_regions_df = get_predictions_unique_regions()
        if pred_regions_df.empty:
            selected_regions_perf = []
        else:
            region_options = [f"{r} ({int(c)})" for r, c in zip(pred_regions_df['region'], pred_regions_df['total_predictions'])]
            selected_region_strs = st.multiselect(
                "Filter by Region(s)",
                region_options,
                default=region_options,
                key="perf_regions"
            )
            selected_regions_perf = [s.rsplit(' (', 1)[0] for s in selected_region_strs]
            
    with col_f2:
        if not selected_regions_perf:
            selected_material_perf = "All"
        else:
            pred_materials_df = get_predictions_materials(selected_regions_perf)
            material_options = [f"{m} ({int(c)})" for m, c in zip(pred_materials_df['sample_material_type'], pred_materials_df['total_predictions'])]
            selected_material_str = st.selectbox(
                "Filter by Material Type",
                ["All"] + material_options,
                key="perf_material"
            )
            selected_material_perf = selected_material_str.rsplit(' (', 1)[0] if selected_material_str != "All" else "All"
            
    with col_f3:
        if not selected_regions_perf:
            selected_determinand_perf = "All"
        else:
            pred_determinands_df = get_predictions_determinands(selected_regions_perf, selected_material_perf)
            det_options = [f"{d} ({int(c)})" for d, c in zip(pred_determinands_df['determinand_label'], pred_determinands_df['total_predictions'])]
            selected_determinand_str = st.selectbox(
                "Filter by Determinand",
                ["All"] + det_options,
                key="perf_determinand"
            )
            selected_determinand_perf = selected_determinand_str.rsplit(' (', 1)[0] if selected_determinand_str != "All" else "All"

    if not selected_regions_perf:
        st.warning("Please select at least one region.")
    else:
        perf_df = get_overall_model_performance(selected_regions_perf, selected_material_perf, selected_determinand_perf)
        
        if perf_df.empty:
            st.info("Insufficient data to calculate performance metrics for the selected filters.")
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

# --- Tab 5: GQA Overall Statistics ---
with tab5:
    st.header("Overall GQA Statistics")
    st.markdown("""
        Detailed overview and statistical breakdown of the Regional General Quality Assessment (GQA) river health grades.
    """)
    
    gqa_all_meta = db.get_gqa_metadata()
    if gqa_all_meta['min_date'] is None:
        st.info("No GQA data available yet.")
    else:
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            all_gqa_dates = pd.date_range(start=gqa_all_meta['min_date'], end=gqa_all_meta['max_date']).date.tolist()
            if len(all_gqa_dates) > 1:
                gqa_date_range = st.select_slider(
                    "Select Date Range (GQA Stats)",
                    options=all_gqa_dates,
                    value=(min(all_gqa_dates), max(all_gqa_dates)),
                    key="gqa_stats_date"
                )
                start_date_gqa_s, end_date_gqa_s = gqa_date_range
            else:
                start_date_gqa_s = end_date_gqa_s = all_gqa_dates[0]
                st.info(f"Showing data for {start_date_gqa_s}")
                
        with col_f2:
            gqa_mats = gqa_all_meta['materials']['sample_material_type'].tolist()
            selected_mats_gqa = st.multiselect(
                "Filter by Material Type(s)",
                gqa_mats,
                default=gqa_mats,
                key="gqa_stats_mats"
            )
            
        gqa_regions_df = db.get_unique_regions()
        gqa_regions = gqa_regions_df['region'].tolist() if not gqa_regions_df.empty else []
        selected_regions_gqa = st.multiselect(
            "Filter by Region(s)",
            gqa_regions,
            default=gqa_regions,
            key="gqa_stats_regions"
        )
        
        all_gqa_df = get_gqa(start_date_gqa_s, end_date_gqa_s, "All")
        
        if all_gqa_df.empty:
            st.warning("No GQA data found for the selected date range.")
        else:
            filtered_gqa = all_gqa_df[
                (all_gqa_df['region'].isin(selected_regions_gqa)) & 
                (all_gqa_df['sample_material_type'].isin(selected_mats_gqa))
            ]
            
            if filtered_gqa.empty:
                st.warning("No GQA data matches the selected Region/Material filters.")
            else:
                st.subheader("Key Performance Indicators")
                kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
                kpi_col1.metric("Total GQA Assessments", len(filtered_gqa))
                kpi_col2.metric("Median Dissolved Oxygen", f"{filtered_gqa['do_value'].median():.2f}%")
                kpi_col3.metric("Median BOD", f"{filtered_gqa['bod_value'].median():.2f} mg/L")
                kpi_col4.metric("Median Ammonia", f"{filtered_gqa['ammonia_value'].median():.3f} mg/L")
                
                st.divider()
                
                col_chart1, col_chart2 = st.columns([1, 2])
                
                with col_chart1:
                    st.subheader("GQA Grade Distribution")
                    grade_counts = filtered_gqa['gqa_grade'].value_counts().reset_index()
                    grade_counts.columns = ['GQA Grade', 'Count']
                    grade_counts = grade_counts.sort_values('GQA Grade')
                    
                    color_map = {'A': '#2ecc71', 'B': '#27ae60', 'C': '#f1c40f', 'D': '#e67e22', 'E': '#e74c3c', 'F': '#95a5a6'}
                    fig_gqa_pie = px.pie(
                        grade_counts, 
                        names='GQA Grade', 
                        values='Count',
                        color='GQA Grade',
                        color_discrete_map=color_map,
                        category_orders={"GQA Grade": ["A", "B", "C", "D", "E", "F"]},
                        hole=0.4,
                        title="Proportion of River Health Grades"
                    )
                    st.plotly_chart(fig_gqa_pie, width='stretch')
                    
                with col_chart2:
                    st.subheader("Water Quality Parameter Spread by Grade")
                    param_choice = st.radio(
                        "Select parameter to analyze:",
                        ["Dissolved Oxygen (% Saturation)", "BOD (mg/L)", "Ammonia (mg/L)"],
                        horizontal=True
                    )
                    
                    use_log_scale = st.checkbox("Use Logarithmic Scale (Y-axis)", value=False)
                    
                    if param_choice == "Dissolved Oxygen (% Saturation)":
                        y_col = 'do_value'
                        title_str = "Dissolved Oxygen levels across GQA Grades"
                    elif param_choice == "BOD (mg/L)":
                        y_col = 'bod_value'
                        title_str = "BOD levels across GQA Grades"
                    else:
                        y_col = 'ammonia_value'
                        title_str = "Ammoniacal Nitrogen levels across GQA Grades"
                        
                    fig_box = px.box(
                        filtered_gqa,
                        x='gqa_grade',
                        y=y_col,
                        color='gqa_grade',
                        color_discrete_map=color_map,
                        category_orders={"gqa_grade": ["A", "B", "C", "D", "E", "F"]},
                        title=title_str,
                        points="outliers",
                        log_y=use_log_scale
                    )
                    st.plotly_chart(fig_box, width='stretch')
                    
                st.divider()
                
                col_chart3, col_chart4 = st.columns(2)
                
                with col_chart3:
                    st.subheader("GQA Grade Breakdown by Region")
                    region_grade = filtered_gqa.groupby(['region', 'gqa_grade']).size().reset_index(name='count')
                    fig_reg_grade = px.bar(
                        region_grade,
                        x='region',
                        y='count',
                        color='gqa_grade',
                        color_discrete_map=color_map,
                        category_orders={"gqa_grade": ["A", "B", "C", "D", "E", "F"]},
                        title="Assessments Count by Region & Grade",
                        barmode="stack"
                    )
                    st.plotly_chart(fig_reg_grade, width='stretch')
                    
                with col_chart4:
                    st.subheader("Average Metrics by Grade")
                    avg_metrics = filtered_gqa.groupby('gqa_grade').agg(
                        avg_do=('do_value', 'mean'),
                        avg_bod=('bod_value', 'mean'),
                        avg_ammonia=('ammonia_value', 'mean'),
                        assessments_count=('gqa_grade', 'count')
                    ).reset_index()
                    
                    st.dataframe(
                        avg_metrics.style.format({
                            'avg_do': '{:.2f}%',
                            'avg_bod': '{:.2f} mg/L',
                            'avg_ammonia': '{:.3f} mg/L',
                            'assessments_count': '{:,.0f}'
                        }),
                        width='stretch'
                    )
                    
                with st.expander("View Filtered GQA Raw Data"):
                    st.dataframe(filtered_gqa.sort_values('window_start', ascending=False), width='stretch')

# --- Tab 6: AI Predictable Groups Statistics ---
with tab6:
    st.header("AI Predictable Groups Statistics")
    st.markdown("""
        Analysis of regional daily aggregates that qualify for AI forecasting (groups with 10+ daily samples).
    """)
    
    overall_df = db.get_predictable_groups_overall_stats()
    
    if overall_df.empty:
        st.warning("No data found in region_daily_averages.")
    else:
        total_rec = overall_df['total_records'].iloc[0]
        pred_rec = overall_df['predictable_records'].iloc[0]
        elig_rate = overall_df['eligibility_rate'].iloc[0]
        
        st.subheader("Eligibility Metrics")
        el_col1, el_col2, el_col3 = st.columns(3)
        el_col1.metric("Total Daily Groups", f"{total_rec:,.0f}")
        el_col2.metric("AI Eligible Groups (Samples >= 10)", f"{pred_rec:,.0f}")
        el_col3.metric("Overall AI Eligibility Rate", f"{elig_rate:.2f}%")
        
        st.divider()
        
        st.subheader("Sample Volume Distribution & Eligibility Analysis")
        dist_df = db.get_predictable_groups_sample_distribution()
        
        if not dist_df.empty:
            dist_df['Eligibility Zone'] = dist_df['sample_bucket'].apply(
                lambda x: 'AI Eligible (Samples >= 10)' if x in ['10-19', '20-49', '50+'] else 'Insufficient (Samples < 10)'
            )
            
            dist_col1, dist_col2 = st.columns([1, 1])
            
            with dist_col1:
                st.subheader("Eligibility Proportion")
                elig_summary = dist_df.groupby('Eligibility Zone')['group_count'].sum().reset_index()
                fig_elig_pie = px.pie(
                    elig_summary,
                    names='Eligibility Zone',
                    values='group_count',
                    color='Eligibility Zone',
                    color_discrete_map={
                        'AI Eligible (Samples >= 10)': '#2ecc71',
                        'Insufficient (Samples < 10)': '#e74c3c'
                    },
                    hole=0.4,
                    title="Overall Eligibility Ratio"
                )
                st.plotly_chart(fig_elig_pie, width='stretch')
                
            with dist_col2:
                st.subheader("Data Suitability Insights")
                st.markdown("""
                    To ensure robust and accurate machine learning predictions, the forecasting pipeline filters out 
                    daily regional aggregations with fewer than **10 samples**.
                    
                    - **AI Eligible Data**: daily aggregates where multiple observations were recorded, providing high confidence.
                    - **Insufficient Data**: single observations or sparse samples that do not capture the daily variation and are excluded to prevent overfitting.
                    
                    Below you can explore which **Regions**, **Material Types**, and **Measurement Determinands** contribute the most to the AI-ready data pool.
                """)
            
        st.divider()
        
        col_reg, col_mat = st.columns(2)
        
        with col_reg:
            st.subheader("AI Eligibility by Region")
            reg_stats = db.get_predictable_groups_regional_stats()
            if not reg_stats.empty:
                fig_reg_elig = px.bar(
                    reg_stats,
                    x='region',
                    y=['predictable_records', 'total_records'],
                    barmode='overlay',
                    labels={'value': 'Record Count', 'variable': 'Category'},
                    title="Predictable vs. Total Records by Region"
                )
                newnames = {'predictable_records': 'AI Predictable', 'total_records': 'Total Groups'}
                fig_reg_elig.for_each_trace(lambda t: t.update(name = newnames[t.name]))
                st.plotly_chart(fig_reg_elig, width='stretch')
                
                with st.expander("View Regional Detail"):
                    st.dataframe(reg_stats, width='stretch')
                    
        with col_mat:
            st.subheader("AI Eligibility by Material Type")
            mat_stats = db.get_predictable_groups_material_stats()
            if not mat_stats.empty:
                fig_mat_elig = px.bar(
                    mat_stats,
                    y='sample_material_type',
                    x='predictable_records',
                    color='eligibility_rate',
                    color_continuous_scale='Viridis',
                    labels={'predictable_records': 'AI Predictable Count', 'sample_material_type': 'Material Type', 'eligibility_rate': 'Eligibility %'},
                    title="AI Predictable Volumes by Material Type",
                    orientation='h'
                )
                st.plotly_chart(fig_mat_elig, width='stretch')
                
                with st.expander("View Material Detail"):
                    st.dataframe(mat_stats, width='stretch')
                    
        st.divider()
        
        st.subheader("Top 15 Measurement Determinands: AI Suitability")
        det_stats = db.get_predictable_groups_determinand_stats()
        if not det_stats.empty:
            fig_det_elig = px.bar(
                det_stats,
                x='eligibility_rate',
                y='determinand_label',
                color='predictable_records',
                color_continuous_scale='Cividis',
                labels={'eligibility_rate': 'Eligibility %', 'determinand_label': 'Determinand Label', 'predictable_records': 'Eligible Count'},
                title="AI Eligibility Percentage for Top Determinands",
                orientation='h'
            )
            fig_det_elig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig_det_elig, width='stretch')
            
            with st.expander("View Determinand Detail"):
                st.dataframe(det_stats, width='stretch')

st.markdown("---")
st.caption("Data is cached for 60 seconds.")
