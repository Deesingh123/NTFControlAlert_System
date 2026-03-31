import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date, time
from streamlit_autorefresh import st_autorefresh

# ── Page Config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Quality Analytics Dashboard",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Uncomment for real‑time updates (refresh every 5 seconds)
# st_autorefresh(interval=5000, key="factory_refresh")

# ── Light Theme Colors ──────────────────────────────────────────────────────
BG_LIGHT = "#FFFFFF"
CARD_BG = "#F8FAFC"
TEXT_DARK = "#0D3679"
ACCENT = "#2563EB"
SUCCESS = "#22c55e"
WARNING = "#F59E0B"
DANGER = "#E65CD3"
BORDER = "#E2E8F0"

st.markdown(f"""
<style>
    .stApp {{ background-color: {BG_LIGHT}; color: {TEXT_DARK}; }}
    .block-container {{ padding-top: 1rem; }}
    div[data-testid="stMetric"] {{
        background: {CARD_BG};
        border: 1px solid {BORDER};
        border-radius: 12px;
        padding: 12px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.02);
    }}
    div[data-testid="stMetric"] label {{ color: {ACCENT} !important; }}
    div[data-testid="stMetric"] div {{ color: {TEXT_DARK} !important; font-weight: 600; }}
    .stTabs [data-baseweb="tab-list"] {{ gap: 8px; }}
    .stTabs [data-baseweb="tab"] {{
        background: {CARD_BG};
        border: 1px solid {BORDER};
        border-radius: 8px 8px 0 0;
        padding: 8px 20px;
        color: {TEXT_DARK};
    }}
    .stTabs [aria-selected="true"] {{ background: {ACCENT}10; border-bottom: 2px solid {ACCENT}; }}
    .stMultiSelect, .stSelectbox {{ background: {CARD_BG}; }}
    h1, h2, h3, h4 {{ color: {ACCENT}; }}
    hr {{ border-color: {BORDER}; }}
    .stDataFrame {{ background: {CARD_BG}; }}
    .stMarkdown, .stCaption {{ color: {TEXT_DARK}; }}
    .scrollable {{
        max-height: 500px;
        overflow-y: auto;
        border: 1px solid {BORDER};
        border-radius: 8px;
        padding: 1rem;
        background: {CARD_BG};
    }}
    .scrollable-h {{
        overflow-x: auto;
        white-space: nowrap;
    }}
</style>
""", unsafe_allow_html=True)

# ── Load & Prepare Data ─────────────────────────────────────────────────────
@st.cache_data(ttl=5)
def load_data():
    conn = sqlite3.connect(r"C:\Users\devds\Downloads\rslt_data.db")
    df = pd.read_sql_query("SELECT * FROM log_data", conn)
    conn.close()

    # Rename columns to match the dashboard's expectations
    # Assuming the table has these columns in order: 
    # id, test_name, model, line, process, status, failed_testcode, track_id, time, timestamp
    # Adjust if your column names differ
    df.columns = [
        'id', 'test_name', 'model', 'line', 'process', 'status',
        'failed_testcode', 'track_id', 'extra', 'time', 'timestamp'
    ]

    # Drop the unused 'extra' column (if present)
    if 'extra' in df.columns:
        df = df.drop(columns=['extra'])

    # Convert status: 'P' -> 'PASSED', 'F' -> 'FAILED'
    df['status'] = df['status'].astype(str).str.upper()
    df['status'] = df['status'].replace({'P': 'PASSED', 'F': 'FAILED'})

    # Parse timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])
    df['date'] = df['timestamp'].dt.date
    df['hour'] = df['timestamp'].dt.hour
    df['time_only'] = df['timestamp'].dt.time

    # Clean model column
    df['model'] = df['model'].replace(['No Data Found', 'NULL', ''], pd.NA)
    df['model'] = df['model'].fillna('Unknown')

    # Clean failed_testcode
    df['failed_testcode'] = df['failed_testcode'].fillna('')
    df['failed_testcode'] = df['failed_testcode'].replace(['NULL', 'None', ''], 'MISSING_CODE')

    # Per‑test status: simply Passed / Failed (no NTF at test level)
    df['test_status'] = 'Passed'
    df.loc[df['status'] == 'FAILED', 'test_status'] = 'Failed'

    # Create a station column (using line as station) so the dashboard filter works
    df['station'] = df['line']

    return df

df = load_data()
total_rows = len(df)
total_unique_tracks = df['track_id'].nunique()

# ── Sidebar Controls ────────────────────────────────────────────────────────
with st.sidebar:
    st.header("🔧 Control Panel")

    search_term = st.text_input("🔍 Global Search (any field)", "").strip().lower()

    # Date range
    if not df.empty:
        min_date = df['timestamp'].min().date()
        max_date = df['timestamp'].max().date()
    else:
        min_date = max_date = date.today()
    date_range = st.date_input(
        "📅 Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    if len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = min_date, max_date

    # ==================== NEW TIME RANGE (Drop-down style) ====================
    st.subheader("⏰ Time Window")

    # Default values from your data
    if not df.empty:
        default_start = df['timestamp'].min().time()
        default_end   = df['timestamp'].max().time()
    else:
        default_start = time(0, 0)
        default_end   = time(23, 59)

    col1, col2 = st.columns(2)

    with col1:
        start_time = st.time_input(
            "Start Time",
            value=default_start,
            step=300,           # 5-minute steps (you can change to 60 for 1-hour)
            key="start_time"
        )

    with col2:
        end_time = st.time_input(
            "End Time",
            value=default_end,
            step=300,
            key="end_time"
        )
    # Max time between failures (minutes)
    st.markdown("**⏱️ Max time between failures (minutes)**")
    max_failure_span = st.slider(
        "For tracks with ≥2 failures, classify as NTF if span ≤ value, else Real Fail",
        min_value=0, max_value=60, value=60, step=1,
        help="If a track has multiple failures and the time between first and last failure is ≤ this value, it is marked NTF; otherwise Real Fail."
    )


    
    # Model, Line, Process, Station multi-selects
    all_models = sorted(df['model'].unique())
    all_lines = sorted(df['line'].unique())
    all_processes = sorted(df['process'].unique())
    all_stations = sorted(df['station'].unique())

    # Session state initialisation
    if 'models' not in st.session_state:
        st.session_state.models = all_models
        st.session_state.lines = all_lines
        st.session_state.processes = all_processes
        st.session_state.stations = all_stations

    def toggle_all(option):
        if option == 'models':
            st.session_state.models = all_models if len(st.session_state.models) != len(all_models) else []
        elif option == 'lines':
            st.session_state.lines = all_lines if len(st.session_state.lines) != len(all_lines) else []
        elif option == 'processes':
            st.session_state.processes = all_processes if len(st.session_state.processes) != len(all_processes) else []
        elif option == 'stations':
            st.session_state.stations = all_stations if len(st.session_state.stations) != len(all_stations) else []

    col1, col2 = st.columns([3,1])
    with col1:
        st.markdown("**Model**")
    with col2:
        if st.button("↺ All", key="toggle_model"):
            toggle_all('models')
            st.rerun()
    sel_model = st.multiselect("", all_models, default=st.session_state.models, key='models', label_visibility="collapsed")

    col1, col2 = st.columns([3,1])
    with col1:
        st.markdown("**Line**")
    with col2:
        if st.button("↺ All", key="toggle_line"):
            toggle_all('lines')
            st.rerun()
    sel_line = st.multiselect("", all_lines, default=st.session_state.lines, key='lines', label_visibility="collapsed")

    col1, col2 = st.columns([3,1])
    with col1:
        st.markdown("**Process**")
    with col2:
        if st.button("↺ All", key="toggle_process"):
            toggle_all('processes')
            st.rerun()
    sel_process = st.multiselect("", all_processes, default=st.session_state.processes, key='processes', label_visibility="collapsed")

    col1, col2 = st.columns([3,1])
    with col1:
        st.markdown("**Station**")
    with col2:
        if st.button("↺ All", key="toggle_station"):
            toggle_all('stations')
            st.rerun()
    sel_station = st.multiselect("", all_stations, default=st.session_state.stations, key='stations', label_visibility="collapsed")

    # Track Status Filter (includes track-level categories)
    track_status_options = ['All', 'Passed', 'Failed', 'NTF', 'Real Fail']
    sel_track_status = st.selectbox("📊 Track Status Filter", track_status_options, index=0)

    st.markdown("---")
    st.caption(f"📁 Total records: **{total_rows:,}**")
    st.caption(f"🗓️ From {min_date} to {max_date}")

    if st.button("🔄 Reset All Filters", use_container_width=True):
        for key in ['models', 'lines', 'processes', 'stations']:
            st.session_state[key] = globals()[f'all_{key}']
        st.rerun()

# ── Apply base filters (time, model, line, process, station) ────────────────
filtered = df[
    (df['date'] >= start_date) & (df['date'] <= end_date) &
    (df['time_only'] >= start_time) & (df['time_only'] <= end_time) &
    (df['model'].isin(sel_model)) &
    (df['line'].isin(sel_line)) &
    (df['process'].isin(sel_process)) &
    (df['station'].isin(sel_station))
]

# Global text search
if search_term:
    str_cols = filtered.select_dtypes(include=['object', 'string']).columns
    mask = pd.Series(False, index=filtered.index)
    for col in str_cols:
        mask |= filtered[col].astype(str).str.lower().str.contains(search_term, na=False)
    filtered = filtered[mask]

# ── Compute track‑level classification ──────────────────────────────────────
def compute_track_info(df, max_span):
    failures = df[df['status'] == 'FAILED']
    failure_counts = failures.groupby('track_id').size().reset_index(name='failure_count')
    multi_fail = failures.groupby('track_id')['timestamp'].agg(['min', 'max']).reset_index()
    multi_fail.columns = ['track_id', 'first_failure', 'last_failure']
    multi_fail['failure_span_min'] = (multi_fail['last_failure'] - multi_fail['first_failure']).dt.total_seconds() / 60.0

    all_tracks = df['track_id'].unique()
    track_info = pd.DataFrame({'track_id': all_tracks})
    track_info = track_info.merge(failure_counts, on='track_id', how='left')
    track_info['failure_count'] = track_info['failure_count'].fillna(0).astype(int)
    track_info = track_info.merge(multi_fail[['track_id', 'failure_span_min']], on='track_id', how='left')

    def classify(row):
        if row['failure_count'] == 0:
            return 'Passed'
        elif row['failure_count'] == 1:
            return 'Failed'
        else:
            span = row['failure_span_min']
            if span <= max_span:
                return 'NTF'
            else:
                return 'Real Fail'

    track_info['overall_status'] = track_info.apply(classify, axis=1)
    return track_info

track_info_full = compute_track_info(filtered, max_failure_span)

# ── Apply track status filter ───────────────────────────────────────────────
if sel_track_status != 'All':
    valid_tracks = track_info_full[track_info_full['overall_status'] == sel_track_status]['track_id']
    filtered = filtered[filtered['track_id'].isin(valid_tracks)]
    track_info_full = track_info_full[track_info_full['overall_status'] == sel_track_status]

# Re‑compute track info after filter (since filtered may have changed)
track_info_final = compute_track_info(filtered, max_failure_span)

# Compute KPIs
total_tracks_filtered = len(track_info_final)
passed_tracks = (track_info_final['overall_status'] == 'Passed').sum()
failed_tracks = (track_info_final['overall_status'] == 'Failed').sum()
ntf_tracks = (track_info_final['overall_status'] == 'NTF').sum()
real_fail_tracks = (track_info_final['overall_status'] == 'Real Fail').sum()

pass_rate = (passed_tracks / total_tracks_filtered * 100) if total_tracks_filtered else 0
failed_rate = (failed_tracks / total_tracks_filtered * 100) if total_tracks_filtered else 0
ntf_rate = (ntf_tracks / total_tracks_filtered * 100) if total_tracks_filtered else 0
real_rate = (real_fail_tracks / total_tracks_filtered * 100) if total_tracks_filtered else 0

# ── Header ──────────────────────────────────────────────────────────────────
st.markdown("""
<div class="dashboard-header">
    <h1 class="main-title">QUALITY ANALYTICS</h1>
    <div class="subtitle">Failure Root Cause • NTF vs Real Defect</div>
</div>
""", unsafe_allow_html=True)

# ── KPIs (track‑level) – 5 columns ─────────────────────────────────────────
kpi_cols = st.columns(5)
kpi_cols[0].metric(
    "📋 Total Unique Tracks",
    f"{total_unique_tracks:,}",
    delta="All data",
    delta_color="normal",
    help="Total distinct track_ids in the entire database (not affected by any filter or slider)"
)
kpi_cols[1].metric("✅ Passed (no fails)", f"{passed_tracks:,}", delta=f"{pass_rate:.1f}%" if total_tracks_filtered else "0%", delta_color="normal")
kpi_cols[2].metric("❌ Failed (single)", f"{failed_tracks:,}", delta=f"{failed_rate:.1f}%" if total_tracks_filtered else "0%", delta_color="inverse")
kpi_cols[3].metric("⚠️ NTF (multi, ≤span)", f"{ntf_tracks:,}", delta=f"{ntf_rate:.1f}%" if total_tracks_filtered else "0%", delta_color="off")
kpi_cols[4].metric("🔥 Real Fail (multi, >span)", f"{real_fail_tracks:,}", delta=f"{real_rate:.1f}%" if total_tracks_filtered else "0%", delta_color="inverse")

# ── Tabs ────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📊 Overview", "🔍 Failure Analysis", "📈 Trends", "🏭 Station View", "📋 Raw Data", "💡 Insights"])

with tab1:
    row1 = st.columns([1.5, 1])
    with row1[0]:
        st.subheader("🏷️ Track Failure Type")
        if total_tracks_filtered > 0:
            track_counts = track_info_final['overall_status'].value_counts().reset_index()
            track_counts.columns = ['status', 'count']
            fig_donut = px.pie(
                track_counts, values='count', names='status', hole=0.6,
                color='status',
                color_discrete_map={'Passed': SUCCESS, 'Failed': DANGER, 'NTF': WARNING, 'Real Fail': '#D9534F'},
                title="Track‑level Classification"
            )
            fig_donut.update_traces(textinfo='percent+label', textposition='inside',
                                     marker=dict(line=dict(color=BORDER, width=2)))
            fig_donut.update_layout(showlegend=False, height=250, margin=dict(t=30,b=0,l=0,r=0))
            st.plotly_chart(fig_donut, use_container_width=True)
        else:
            st.info("No tracks in current selection")

    with row1[1]:
        st.subheader("🏭 Process Contribution (tests)")
        if not filtered.empty:
            proc_counts = filtered.groupby(['process', 'test_status']).size().reset_index(name='count')
            fig_proc = px.bar(
                proc_counts, x='process', y='count', color='test_status',
                color_discrete_map={'Passed': SUCCESS, 'Failed': DANGER},
                barmode='stack', height=300
            )
            fig_proc.update_layout(xaxis_tickangle=-45, margin=dict(t=20,b=50), showlegend=False)
            st.plotly_chart(fig_proc, use_container_width=True)
        else:
            st.info("No data")

    st.subheader("📊 Model & Line Quality (tests)")
    col_mod, col_line = st.columns(2)

    with col_mod:
        if not filtered.empty:
            mod_counts = filtered.groupby(['model', 'test_status']).size().reset_index(name='count')
            fig_mod = px.bar(
                mod_counts, x='model', y='count', color='test_status',
                color_discrete_map={'Passed': SUCCESS, 'Failed': DANGER},
                barmode='group', height=350, title="Tests by Model"
            )
            fig_mod.update_layout(xaxis_tickangle=-45, margin=dict(t=30,b=80), showlegend=False)
            st.plotly_chart(fig_mod, use_container_width=True)
        else:
            st.info("No model data")

    with col_line:
        if not filtered.empty:
            line_counts = filtered.groupby(['line', 'test_status']).size().reset_index(name='count')
            fig_line = px.bar(
                line_counts, x='line', y='count', color='test_status',
                color_discrete_map={'Passed': SUCCESS, 'Failed': DANGER},
                barmode='group', height=350, title="Tests by Line"
            )
            fig_line.update_layout(xaxis_tickangle=-45, margin=dict(t=30,b=80), showlegend=False)
            st.plotly_chart(fig_line, use_container_width=True)
        else:
            st.info("No line data")

    st.subheader("🔥 Model × Line Failure Rate (Real Fail %) – track level")
    if not track_info_final.empty:
        track_model_line = filtered.groupby('track_id')[['model', 'line']].first().reset_index()
        track_class = track_info_final[['track_id', 'overall_status']].merge(track_model_line, on='track_id', how='left')
        total_tracks_ml = track_class.groupby(['model', 'line']).size().reset_index(name='total_tracks')
        real_fail_tracks_ml = track_class[track_class['overall_status'] == 'Real Fail'].groupby(['model', 'line']).size().reset_index(name='real_fail_tracks')
        merged_ml = total_tracks_ml.merge(real_fail_tracks_ml, on=['model','line'], how='left').fillna(0)
        merged_ml['fail_rate'] = (merged_ml['real_fail_tracks'] / merged_ml['total_tracks'] * 100).round(1)

        heat_data = merged_ml.pivot(index='model', columns='line', values='fail_rate').fillna(0)
        fig_heat = px.imshow(
            heat_data,
            text_auto=True,
            color_continuous_scale='Reds',
            aspect="auto",
            title="Real Fail % by Model and Line (track level)",
            labels=dict(x="Line", y="Model", color="Fail %")
        )
        fig_heat.update_layout(height=400, margin=dict(t=40,b=40))
        st.plotly_chart(fig_heat, use_container_width=True)
    else:
        st.info("No data for Model × Line heatmap")


    
    st.subheader("🔥 Model × Line Failure Rate (NTF %) – track level")
    if not track_info_final.empty:
        track_model_line = filtered.groupby('track_id')[['model', 'line']].first().reset_index()
        track_class = track_info_final[['track_id', 'overall_status']].merge(track_model_line, on='track_id', how='left')
        total_tracks_ml = track_class.groupby(['model', 'line']).size().reset_index(name='total_tracks')
        real_fail_tracks_ml = track_class[track_class['overall_status'] == 'NTF'].groupby(['model', 'line']).size().reset_index(name='ntf_tracks')
        merged_ml = total_tracks_ml.merge(real_fail_tracks_ml, on=['model','line'], how='left').fillna(0)
        merged_ml['ntf_rate'] = (merged_ml['ntf_tracks'] / merged_ml['total_tracks'] * 100).round(1)

        heat_data = merged_ml.pivot(index='model', columns='line', values='ntf_rate').fillna(0)
        fig_heat = px.imshow(
            heat_data,
            text_auto=True,
            color_continuous_scale='Reds',
            aspect="auto",
            title="NTF % by Model and Line (track level)",
            labels=dict(x="Line", y="Model", color="NTF %")
        )
        fig_heat.update_layout(height=400, margin=dict(t=40,b=40))
        st.plotly_chart(fig_heat, use_container_width=True)
    else:
        st.info("No data for Model × Line heatmap")




with tab2:
    st.subheader("📉 Top Failure Codes (Pareto)")
    if len(filtered[filtered['status'] == 'FAILED']) > 0:
        fail_codes = filtered[filtered['status'] == 'FAILED']['failed_testcode'].value_counts().reset_index()
        fail_codes.columns = ['code', 'count']
        fail_codes = fail_codes.head(15)
        fail_codes['cum_percent'] = fail_codes['count'].cumsum() / fail_codes['count'].sum() * 100

        fig_pareto = go.Figure()
        fig_pareto.add_trace(go.Bar(
            x=fail_codes['code'], y=fail_codes['count'],
            name='Failure Count', marker_color=DANGER
        ))
        fig_pareto.add_trace(go.Scatter(
            x=fail_codes['code'], y=fail_codes['cum_percent'],
            name='Cumulative %', yaxis='y2', line=dict(color=ACCENT, width=1)
        ))
        fig_pareto.update_layout(
            xaxis_tickangle=-45, xaxis_title="", yaxis_title="Count",
            yaxis2=dict(title='Cumulative %', overlaying='y', side='right', range=[0, 110]),
            height=450, margin=dict(b=120), hovermode='x unified',
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5)
        )
        fig_pareto.update_xaxes(tickfont=dict(size=10))
        st.plotly_chart(fig_pareto, use_container_width=True)
    else:
        st.info("No failures to show Pareto")

    st.subheader("🔬 Failure Details")
    if len(filtered[filtered['status'] == 'FAILED']) > 0:
        top_fails = fail_codes.head(10)[['code', 'count']].copy()
        example_ids = []
        for code in top_fails['code']:
            ex = filtered[(filtered['failed_testcode'] == code) & (filtered['status'] == 'FAILED')]['track_id'].iloc[0] if not filtered[filtered['failed_testcode'] == code].empty else '-'
            example_ids.append(ex)
        top_fails['example_track'] = example_ids
        st.dataframe(top_fails, use_container_width=True, hide_index=True)
    else:
        st.info("No failure records")

    st.subheader("🌳 Failure Treemap (Process → Station → Failure Code)")
    if len(filtered[filtered['status'] == 'FAILED']) > 0:
        treemap_df = filtered[filtered['status'] == 'FAILED'].groupby(
            ['process', 'station', 'failed_testcode']
        ).size().reset_index(name='count')
        fig_tree = px.treemap(
            treemap_df,
            path=['process', 'station', 'failed_testcode'],
            values='count',
            color='count',
            color_continuous_scale='Reds',
            title="Failure Hierarchy"
        )
        fig_tree.update_layout(height=500)
        st.plotly_chart(fig_tree, use_container_width=True)
    else:
        st.info("No failures to display treemap")

with tab3:
    st.subheader("⏱️ Hourly Test Volume & Failures")
    if not filtered.empty:
        hourly = filtered.groupby([filtered['timestamp'].dt.hour, 'test_status']).size().reset_index(name='count')
        hourly.columns = ['hour', 'status', 'count']
        all_hours = pd.DataFrame({'hour': range(24)})
        hourly = all_hours.merge(hourly, on='hour', how='left').fillna(0)

        fig_hour = px.line(
            hourly, x='hour', y='count', color='status',
            color_discrete_map={'Passed': SUCCESS, 'Failed': DANGER},
            markers=True, height=400
        )
        fig_hour.update_layout(xaxis=dict(dtick=4), margin=dict(t=20))
        st.plotly_chart(fig_hour, use_container_width=True)
    else:
        st.info("No time-series data")

    if filtered['date'].nunique() > 1:
        st.subheader("📅 Daily Failure Trend (tests)")
        daily = filtered.groupby(['date', 'test_status']).size().reset_index(name='count')
        fig_daily = px.line(
            daily, x='date', y='count', color='test_status',
            color_discrete_map={'Passed': SUCCESS, 'Failed': DANGER},
            markers=True, height=400
        )
        fig_daily.update_layout(margin=dict(t=20))
        st.plotly_chart(fig_daily, use_container_width=True)

    # New: NTF vs Real Fail over time (track-level)
    st.subheader("📈 NTF vs Real Fail Tracks Over Time")
    if not track_info_final.empty:
        # Get track dates (first test timestamp for each track)
        track_dates = filtered.groupby('track_id')['timestamp'].min().reset_index()
        track_dates.columns = ['track_id', 'first_test']
        track_dates['date'] = track_dates['first_test'].dt.date
        track_status_with_date = track_info_final.merge(track_dates, on='track_id')
        track_status_with_date = track_status_with_date[track_status_with_date['overall_status'].isin(['NTF', 'Real Fail'])]
        daily_ntf_rf = track_status_with_date.groupby(['date', 'overall_status']).size().reset_index(name='count')
        if not daily_ntf_rf.empty:
            fig_trend = px.line(
                daily_ntf_rf, x='date', y='count', color='overall_status',
                color_discrete_map={'NTF': WARNING, 'Real Fail': '#D9534F'},
                markers=True, height=400, title="NTF vs Real Fail Tracks per Day"
            )
            fig_trend.update_layout(xaxis_title="Date", yaxis_title="Number of Tracks")
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info("No NTF or Real Fail tracks in selected period")

with tab4:
    st.subheader("🏭 Station Performance (tests)")
    if not filtered.empty:
        station_stats = filtered.groupby(['station', 'test_status']).size().reset_index(name='count')
        station_pivot = station_stats.pivot(index='station', columns='test_status', values='count').fillna(0)
        for col in ['Passed', 'Failed']:
            if col not in station_pivot.columns:
                station_pivot[col] = 0
        station_pivot['Total'] = station_pivot[['Passed', 'Failed']].sum(axis=1)
        station_pivot = station_pivot.sort_values('Total', ascending=False)

        top_stations = station_pivot.head(15).reset_index()
        melted = top_stations.melt(id_vars='station', value_vars=['Passed', 'Failed'],
                                   var_name='status', value_name='count')
        fig_station = px.bar(
            melted,
            x='station', y='count', color='status',
            color_discrete_map={'Passed': SUCCESS, 'Failed': DANGER},
            barmode='stack', height=400,
            title="Top 15 Stations by Test Volume"
        )
        fig_station.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_station, use_container_width=True)

        station_fail_rate = filtered.groupby('station').apply(
            lambda x: pd.Series({
                'total_tests': len(x),
                'failed_tests': (x['test_status'] == 'Failed').sum(),
                'passed_tests': (x['test_status'] == 'Passed').sum()
            })
        ).reset_index()
        station_fail_rate['fail_rate'] = (station_fail_rate['failed_tests'] / station_fail_rate['total_tests'] * 100).round(1)

        st.dataframe(
            station_fail_rate.sort_values('fail_rate', ascending=False),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No station data")

    # New: Station-level NTF and Real Fail counts
    st.subheader("🏭 Station NTF & Real Fail Counts (track-level)")
    if not track_info_final.empty:
        track_station = filtered.groupby('track_id')[['station']].first().reset_index()
        track_status_station = track_info_final.merge(track_station, on='track_id')
        station_ntf_rf = track_status_station.groupby(['station', 'overall_status']).size().reset_index(name='count')
        station_ntf_rf = station_ntf_rf[station_ntf_rf['overall_status'].isin(['NTF', 'Real Fail'])]
        if not station_ntf_rf.empty:
            fig_station_ntf_rf = px.bar(
                station_ntf_rf, x='station', y='count', color='overall_status',
                color_discrete_map={'NTF': WARNING, 'Real Fail': '#D9534F'},
                barmode='group', height=400,
                title="NTF and Real Fail Tracks per Station"
            )
            fig_station_ntf_rf.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_station_ntf_rf, use_container_width=True)
        else:
            st.info("No NTF or Real Fail tracks in selected data")
    else:
        st.info("No track data available")

with tab5:
    st.subheader("📄 Raw Data Preview")
    cols_to_show = ['timestamp', 'model', 'line', 'process', 'station', 'status', 'test_status', 'failed_testcode', 'track_id']
    display_df = filtered[cols_to_show].copy()
    display_df['timestamp'] = display_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    st.dataframe(display_df, use_container_width=True, height=500)

    csv = display_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        "⬇️ Download Filtered Data as CSV",
        csv,
        "factory_quality_data.csv",
        "text/csv",
        use_container_width=True
    )

with tab6:
    st.subheader("💡 Additional Insights")

    # 1. Failure Span Distribution
    st.subheader("⏱️ Failure Span Distribution (Multi-Failure Tracks)")
    multi_fail_tracks = track_info_final[track_info_final['failure_count'] >= 2]
    if not multi_fail_tracks.empty:
        fig_span = px.histogram(
            multi_fail_tracks, x='failure_span_min', nbins=30,
            color_discrete_sequence=[ACCENT],
            labels={'failure_span_min': 'Failure Span (minutes)'},
            title="Distribution of Time Between First and Last Failure"
        )
        fig_span.add_vline(x=max_failure_span, line_dash="dash", line_color=DANGER,
                           annotation_text=f"NTF threshold = {max_failure_span} min")
        fig_span.update_layout(height=400)
        st.plotly_chart(fig_span, use_container_width=True)
    else:
        st.info("No multi-failure tracks in selected data")

    # 2. Top Failure Codes by Model
    st.subheader("🔧 Top Failure Codes by Model")
    if len(filtered[filtered['status'] == 'FAILED']) > 0:
        top_fail_by_model = filtered[filtered['status'] == 'FAILED'].groupby(['model', 'failed_testcode']).size().reset_index(name='count')
        top_fail_by_model = top_fail_by_model.sort_values(['model', 'count'], ascending=[True, False])
        top_fail_by_model = top_fail_by_model.groupby('model').head(3).reset_index(drop=True)
        fig_model_codes = px.bar(
            top_fail_by_model, x='model', y='count', color='failed_testcode',
            title="Top 3 Failure Codes per Model",
            labels={'count': 'Number of Failures', 'failed_testcode': 'Failure Code'}
        )
        fig_model_codes.update_layout(xaxis_tickangle=-45, height=500)
        st.plotly_chart(fig_model_codes, use_container_width=True)
    else:
        st.info("No failures to display")

    

# ── Footer ──────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(f"🔄 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Dashboard shows filtered subset. Use sidebar to explore all data.")