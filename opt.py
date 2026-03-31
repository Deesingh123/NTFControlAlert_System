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

#st_autorefresh(interval=5000, key="factory_refresh")

# ── Light Theme Colors ──────────────────────────────────────────────────────
BG_LIGHT = "#FFFFFF"
CARD_BG = "#F8FAFC"
TEXT_DARK = "#0D3679"
ACCENT = "#2563EB"          # blue
SUCCESS = "#22c55e"         # emerald
WARNING = "#F59E0B"         # amber
DANGER = "#E65CD3"          # Pink
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
    /* Scrollable containers */
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
    conn = sqlite3.connect(r"C:\Users\devds\OneDrive\Desktop\factory_data.db")
    df = pd.read_sql_query("SELECT * FROM log_data", conn)
    conn.close()
    
    # Convert status: 'P' -> 'PASSED', 'F' -> 'FAILED'
    # Also handle any unexpected values
    df['status'] = df['status'].astype(str).str.upper()
    df['status'] = df['status'].replace({'P': 'PASSED', 'F': 'FAILED'})
    
    # Parse timestamp (assuming column name is exactly 'timestamp')
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
    
    # NTF patterns
    ntf_patterns = [
        'BARCODE', 'BAD_OR_FAILED', 'NO DATA', 'NOT FOUND',
        'UNCOVERED', 'TIMEOUT', 'NULL', 'MISSING', 'FAILED_BARCODE',
        'COMMUNICATION', 'HARDWARE_NOT_DETECTED', 'CALIBRATION_REQUIRED'
    ]
    pattern = '|'.join(ntf_patterns)
    
    df['status_detailed'] = 'Passed'
    failed_mask = df['status'] == 'FAILED'
    df.loc[failed_mask, 'status_detailed'] = 'Real Fail'
    
    ntf_mask = failed_mask & (
        df['failed_testcode'].str.contains(pattern, case=False, na=False) |
        (df['failed_testcode'] == 'MISSING_CODE')
    )
    df.loc[ntf_mask, 'status_detailed'] = 'NTF'
    
    return df

df = load_data()
total_rows = len(df)
total_unique_tracks = df['track_id'].nunique()   # fixed, unfiltered



# ── Sidebar Controls ────────────────────────────────────────────────────────
with st.sidebar:
    st.header("🔧 Control Panel")
    
    # Global text search
    search_term = st.text_input("🔍 Global Search (any field)", "").strip().lower()
    
    # Date range - ensure Python date objects
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
    
    # Time range slider - ensure time objects
    if not df.empty:
        time_min = df['timestamp'].min().time()
        time_max = df['timestamp'].max().time()
    else:
        time_min = time(0, 0)
        time_max = time(23, 59)
    time_range = st.slider(
        "Select time window",
        value=(datetime.combine(date.today(), time_min), datetime.combine(date.today(), time_max)),
        format="HH:mm"
    )
    start_time = time_range[0].time()
    end_time = time_range[1].time()
    
    # Max time between failures (minutes) slider
    st.markdown("**⏱️ Max time between failures (minutes)**")
    max_failure_span = st.slider(
        "Only tracks with >1 failure and span ≤ value",
        min_value=0, max_value=30, value=30, step=1,
        help="For tracks with at least two failures, keep only those where the time between first and last failure is ≤ this many minutes."
    )
    # ... rest of sidebar unchanged
    
    # Model, Line, Process, Station multi-selects
    all_models = sorted(df['model'].unique())
    all_lines = sorted(df['line'].unique())
    all_processes = sorted(df['process'].unique())
    all_stations = sorted(df['station'].dropna().unique())
    
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
    
    status_options = ['All', 'Passed', 'Failed', 'NTF', 'Real Fail']
    sel_status = st.selectbox("📊 Status Filter", status_options, index=0)
    
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

# Global text search across all string columns
if search_term:
    str_cols = filtered.select_dtypes(include=['object', 'string']).columns
    mask = pd.Series(False, index=filtered.index)
    for col in str_cols:
        mask |= filtered[col].astype(str).str.lower().str.contains(search_term, na=False)
    filtered = filtered[mask]

# ── Compute per‑track stats for span filtering (before test‑level status filter) ──
# We need failure times for span calculation, but we do this before status filter
# because span should be based on all failures in the base‑filtered set.
track_stats_base = filtered.groupby('track_id').agg(
    has_real_fail=('status_detailed', lambda x: (x == 'Real Fail').any()),
    has_ntf=('status_detailed', lambda x: (x == 'NTF').any()),
    failure_count=('status_detailed', lambda x: ((x == 'Real Fail') | (x == 'NTF')).sum())
).reset_index()

# Get failure time spans for tracks with at least 2 failures
failure_times = filtered[filtered['status_detailed'].isin(['NTF', 'Real Fail'])].groupby('track_id')['timestamp'].agg(['min', 'max']).reset_index()
failure_times.columns = ['track_id', 'first_failure', 'last_failure']
failure_times['failure_span_min'] = (failure_times['last_failure'] - failure_times['first_failure']).dt.total_seconds() / 60.0

track_stats_base = track_stats_base.merge(failure_times[['track_id', 'failure_span_min']], on='track_id', how='left')
track_stats_base['failure_span_min'] = track_stats_base['failure_span_min'].fillna(0)

# Apply span filter: keep tracks with failure_count <=1 OR span ≤ max_failure_span
span_mask = (track_stats_base['failure_count'] <= 1) | (track_stats_base['failure_span_min'] <= max_failure_span)
valid_tracks = track_stats_base[span_mask]['track_id'].unique()
filtered = filtered[filtered['track_id'].isin(valid_tracks)]

# ── Apply test‑level status filter ──────────────────────────────────────────
if sel_status != 'All':
    if sel_status == 'Failed':
        filtered = filtered[filtered['status'].str.upper() == 'FAILED']
    else:
        filtered = filtered[filtered['status_detailed'] == sel_status]

# ── Recompute track‑level stats from the FINAL filtered set for KPIs ────────
final_track_stats = filtered.groupby('track_id').agg(
    has_real_fail=('status_detailed', lambda x: (x == 'Real Fail').any()),
    has_ntf=('status_detailed', lambda x: (x == 'NTF').any())
).reset_index()

def final_overall_status(row):
    if row['has_real_fail']:
        return 'Real Fail'
    elif row['has_ntf']:
        return 'NTF'
    else:
        return 'Passed'
final_track_stats['overall_status'] = final_track_stats.apply(final_overall_status, axis=1)

total_tracks_filtered = len(final_track_stats)
passed_tracks = (final_track_stats['overall_status'] == 'Passed').sum()
ntf_tracks = (final_track_stats['overall_status'] == 'NTF').sum()
real_fail_tracks = (final_track_stats['overall_status'] == 'Real Fail').sum()

pass_rate = (passed_tracks / total_tracks_filtered * 100) if total_tracks_filtered else 0
ntf_rate = (ntf_tracks / total_tracks_filtered * 100) if total_tracks_filtered else 0
real_rate = (real_fail_tracks / total_tracks_filtered * 100) if total_tracks_filtered else 0

# ── Header ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .dashboard-header {
        text-align: center;
        padding: 1.0rem 1rem 1.0rem;
        background: linear-gradient(to bottom, rgba(59, 130, 246, 0.4), transparent);
        border-bottom: 1px solid rgba(59, 130, 246, 0.18);
        margin-bottom: 1.5rem;
    }
    .main-title {
        font-size: 2.9rem;
        font-weight: 900;
        letter-spacing: -1.2px;
        background: linear-gradient(90deg, #1e3a8a, #3b82f6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        white-space: nowrap; 
    }
    .subtitle {
        font-size: clamp(0.8rem, 2.2vw, 1.2rem);
        color: #94a3b8;
        font-weight: 400;
        letter-spacing: 0.4px;
        text-transform: lowercase;
        opacity: 0.9;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="dashboard-header">
    <h1 class="main-title">QUALITY ANALYTICS</h1>
    <div class="subtitle">Failure Root Cause • NTF vs Real Defect</div>
</div>
""", unsafe_allow_html=True)

# ── KPIs ────────────────────────────────────────────────────────────────────
kpi_cols = st.columns(4)
kpi_cols[0].metric(
    "📋 Total Unique Tracks",
    f"{total_unique_tracks:,}",
    delta="All data",
    delta_color="normal",
    help="Total distinct track_ids in the entire database (not affected by any filter or slider)"
)
kpi_cols[1].metric("✅ Passed", f"{passed_tracks:,}", delta=f"{pass_rate:.1f}%" if total_tracks_filtered else "0%", delta_color="normal")
kpi_cols[2].metric("⚠️ NTF", f"{ntf_tracks:,}", delta=f"{ntf_rate:.1f}%" if total_tracks_filtered else "0%", delta_color="off")
kpi_cols[3].metric("❌ Real Fails", f"{real_fail_tracks:,}", delta=f"{real_rate:.1f}%" if total_tracks_filtered else "0%", delta_color="inverse")

# ── Tabs (all charts use `filtered`, which now respects all filters) ────────
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Overview", "🔍 Failure Analysis", "📈 Trends", "🏭 Station View", "📋 Raw Data"])

with tab1:
    # Row 1: Failure Type + Process Contribution
    row1 = st.columns([1.5, 1])
    with row1[0]:
        st.subheader("🧩 Failure Type (per test)")
        if len(filtered[filtered['status'] == 'Failed']) > 0:
            fail_counts = filtered[filtered['status'] == 'Failed']['status_detailed'].value_counts().reset_index()
            fail_counts.columns = ['status', 'count']
            fig_donut = px.pie(
                fail_counts, values='count', names='status', hole=0.6,
                color='status', color_discrete_map={'NTF': WARNING, 'Real Fail': DANGER},
                title="NTF vs Real Fail"
            )
            fig_donut.update_traces(textinfo='percent+label', textposition='inside',
                                     marker=dict(line=dict(color=BORDER, width=2)))
            fig_donut.update_layout(showlegend=False, height=250, margin=dict(t=30,b=0,l=0,r=0))
            st.plotly_chart(fig_donut, use_container_width=True)
        else:
            st.info("No failures in current selection")
    
    with row1[1]:
        st.subheader("🏭 Process Contribution")
        if not filtered.empty:
            proc_counts = filtered.groupby(['process', 'status_detailed']).size().reset_index(name='count')
            fig_proc = px.bar(
                proc_counts, x='process', y='count', color='status_detailed',
                color_discrete_map={'Passed': SUCCESS, 'NTF': WARNING, 'Real Fail': DANGER},
                barmode='stack', height=300
            )
            fig_proc.update_layout(xaxis_tickangle=-45, margin=dict(t=20,b=50), showlegend=False)
            st.plotly_chart(fig_proc, use_container_width=True)
        else:
            st.info("No data")
    
    # Row 2: Model and Line breakdowns
    st.subheader("📊 Model & Line Quality")
    col_mod, col_line = st.columns(2)
    
    with col_mod:
        if not filtered.empty:
            mod_counts = filtered.groupby(['model', 'status_detailed']).size().reset_index(name='count')
            fig_mod = px.bar(
                mod_counts, x='model', y='count', color='status_detailed',
                color_discrete_map={'Passed': SUCCESS, 'NTF': WARNING, 'Real Fail': DANGER},
                barmode='group', height=350, title="Tests by Model"
            )
            fig_mod.update_layout(xaxis_tickangle=-45, margin=dict(t=30,b=80), showlegend=False)
            st.plotly_chart(fig_mod, use_container_width=True)
        else:
            st.info("No model data")
    
    with col_line:
        if not filtered.empty:
            line_counts = filtered.groupby(['line', 'status_detailed']).size().reset_index(name='count')
            fig_line = px.bar(
                line_counts, x='line', y='count', color='status_detailed',
                color_discrete_map={'Passed': SUCCESS, 'NTF': WARNING, 'Real Fail': DANGER},
                barmode='group', height=350, title="Tests by Line"
            )
            fig_line.update_layout(xaxis_tickangle=-45, margin=dict(t=30,b=80), showlegend=False)
            st.plotly_chart(fig_line, use_container_width=True)
        else:
            st.info("No line data")
    
    # Row 3: Model × Line Heatmap (failure rate)
    st.subheader("🔥 Model × Line Failure Rate (Real Fail %)")
    if not filtered.empty:
        total_counts = filtered.groupby(['model', 'line']).size().reset_index(name='total')
        fail_counts = filtered[filtered['status_detailed'] == 'Real Fail'].groupby(['model', 'line']).size().reset_index(name='failures')
        merged = total_counts.merge(fail_counts, on=['model','line'], how='left').fillna(0)
        merged['fail_rate'] = (merged['failures'] / merged['total'] * 100).round(1)
        
        heat_data = merged.pivot(index='model', columns='line', values='fail_rate').fillna(0)
        fig_heat = px.imshow(
            heat_data,
            text_auto=True,
            color_continuous_scale='Reds',
            aspect="auto",
            title="Real Fail % by Model and Line",
            labels=dict(x="Line", y="Model", color="Fail %")
        )
        fig_heat.update_layout(height=400, margin=dict(t=40,b=40))
        st.plotly_chart(fig_heat, use_container_width=True)
    else:
        st.info("No data for Model × Line heatmap")

with tab2:
    st.subheader("📉 Top Failure Codes (Pareto)")
    if len(filtered[filtered['status'] == 'Failed']) > 0:
        fail_codes = filtered[filtered['status'] == 'Failed']['failed_testcode'].value_counts().reset_index()
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
    if len(filtered[filtered['status'] == 'Failed']) > 0:
        top_fails = fail_codes.head(10)[['code', 'count']].copy()
        example_ids = []
        for code in top_fails['code']:
            ex = filtered[(filtered['failed_testcode'] == code) & (filtered['status'] == 'Failed')]['track_id'].iloc[0] if not filtered[filtered['failed_testcode'] == code].empty else '-'
            example_ids.append(ex)
        top_fails['example_track'] = example_ids
        st.dataframe(top_fails, use_container_width=True, hide_index=True)
    else:
        st.info("No failure records")
    
    # Failure Treemap
    st.subheader("🌳 Failure Treemap (Process → Station → Failure Code)")
    if len(filtered[filtered['status'] == 'Failed']) > 0:
        treemap_df = filtered[filtered['status'] == 'Failed'].groupby(
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
        hourly = filtered.groupby([filtered['timestamp'].dt.hour, 'status_detailed']).size().reset_index(name='count')
        hourly.columns = ['hour', 'status', 'count']
        all_hours = pd.DataFrame({'hour': range(24)})
        hourly = all_hours.merge(hourly, on='hour', how='left').fillna(0)
        
        fig_hour = px.line(
            hourly, x='hour', y='count', color='status',
            color_discrete_map={'Passed': SUCCESS, 'NTF': WARNING, 'Real Fail': DANGER},
            markers=True, height=400
        )
        fig_hour.update_layout(xaxis=dict(dtick=4), margin=dict(t=20))
        st.plotly_chart(fig_hour, use_container_width=True)
    else:
        st.info("No time-series data")
    
    if filtered['date'].nunique() > 1:
        st.subheader("📅 Daily Failure Trend")
        daily = filtered.groupby(['date', 'status_detailed']).size().reset_index(name='count')
        fig_daily = px.line(
            daily, x='date', y='count', color='status_detailed',
            color_discrete_map={'Passed': SUCCESS, 'NTF': WARNING, 'Real Fail': DANGER},
            markers=True, height=400
        )
        fig_daily.update_layout(margin=dict(t=20))
        st.plotly_chart(fig_daily, use_container_width=True)

with tab4:
    st.subheader("🏭 Station Performance")
    if not filtered.empty:
        station_stats = filtered.groupby(['station', 'status_detailed']).size().reset_index(name='count')
        station_pivot = station_stats.pivot(index='station', columns='status_detailed', values='count').fillna(0)
        for col in ['Passed', 'NTF', 'Real Fail']:
            if col not in station_pivot.columns:
                station_pivot[col] = 0
        station_pivot['Total'] = station_pivot[['Passed', 'NTF', 'Real Fail']].sum(axis=1)
        station_pivot = station_pivot.sort_values('Total', ascending=False)
        
        top_stations = station_pivot.head(15).reset_index()
        melted = top_stations.melt(id_vars='station', value_vars=['Passed', 'NTF', 'Real Fail'], 
                                   var_name='status', value_name='count')
        fig_station = px.bar(
            melted,
            x='station', y='count', color='status',
            color_discrete_map={'Passed': SUCCESS, 'NTF': WARNING, 'Real Fail': DANGER},
            barmode='stack', height=400,
            title="Top 15 Stations by Test Volume"
        )
        fig_station.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_station, use_container_width=True)
        
        station_fail_rate = filtered.groupby('station').apply(
            lambda x: pd.Series({
                'total': len(x),
                'real_fails': (x['status_detailed'] == 'Real Fail').sum(),
                'ntf': (x['status_detailed'] == 'NTF').sum(),
                'passed': (x['status_detailed'] == 'Passed').sum()
            })
        ).reset_index()
        station_fail_rate['real_fail_rate'] = (station_fail_rate['real_fails'] / station_fail_rate['total'] * 100).round(1)
        station_fail_rate['ntf_rate'] = (station_fail_rate['ntf'] / station_fail_rate['total'] * 100).round(1)
        
        st.dataframe(
            station_fail_rate.sort_values('real_fail_rate', ascending=False),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No station data")

with tab5:
    st.subheader("📄 Raw Data Preview")
    cols_to_show = ['timestamp', 'model', 'line', 'process', 'station', 'status', 'status_detailed', 'failed_testcode', 'track_id']
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

# ── Footer ──────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(f"🔄 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Dashboard shows filtered subset. Use sidebar to explore all data.")