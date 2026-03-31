import sqlite3
import os
from datetime import datetime, date, time, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ── CONFIG ───────────────────────────────────────────────────────────────────
DB_PATH = r"C:\Users\devds\Downloads\Qr_Converter-main\output\rslt_data.db"
#DB_PATH = r"rslt_data.db"

# ── PAGE SETUP ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Factory Quality Dashboard",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st_autorefresh(interval=10_000, key="auto_refresh")

# ── THEME (unchanged) ────────────────────────────────────────────────────────
# ... (your existing CSS, keep as is) ...

# ── CHART PALETTE (unchanged) ────────────────────────────────────────────────
CHART_BG   = "rgba(0,0,0,0)"
PAPER_BG   = "rgba(0,0,0,0)"
GRID_COLOR = "#f1f0ec"
FONT_COLOR = "#374151"
C_PASS     = "#16a34a"
C_FAIL     = "#dc2626"
C_BLUE     = "#1d4ed8"
C_AMBER    = "#d97706"

def chart_layout(fig, height=340, margin=None, legend=True):
    m = margin or dict(t=30, b=50, l=10, r=10)
    fig.update_layout(
        plot_bgcolor=CHART_BG,
        paper_bgcolor=PAPER_BG,
        font=dict(family="JetBrains Mono, monospace", color=FONT_COLOR, size=11),
        height=height,
        margin=m,
        showlegend=legend,
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="left", x=0,
            font=dict(size=10),
            bgcolor="rgba(255,255,255,0.7)",
            bordercolor="#e2e8f0",
            borderwidth=1,
        ),
        xaxis=dict(gridcolor=GRID_COLOR, linecolor="#e2e8f0",
                   zerolinecolor=GRID_COLOR, tickfont=dict(color=FONT_COLOR)),
        yaxis=dict(gridcolor=GRID_COLOR, linecolor="#e2e8f0",
                   zerolinecolor=GRID_COLOR, tickfont=dict(color=FONT_COLOR)),
    )
    return fig

# ── DATA LOADING (unchanged) ─────────────────────────────────────────────────
@st.cache_data(ttl=8)
def load_data(db_path: str) -> pd.DataFrame:
    if not os.path.exists(db_path):
        return pd.DataFrame()
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query("SELECT * FROM log_data", conn)
    except Exception:
        conn.close()
        return pd.DataFrame()
    conn.close()
    if df.empty:
        return df

    df["status"]    = df["status"].astype(str).str.strip().str.upper()
    df["passed"]    = df["status"] == "P"
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["date"]      = df["timestamp"].dt.date
    df["hour"]      = df["timestamp"].dt.hour
    df["time_only"] = df["timestamp"].dt.time

    for col in ["model", "line", "station", "process", "failed_testcode"]:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace(["None", "NULL", "nan", ""], pd.NA)

    df["model"]           = df["model"].fillna("Unknown")
    df["failed_testcode"] = df["failed_testcode"].fillna("")
    df["status_display"]  = df["status"].map({"P": "Passed", "F": "Failed"}).fillna("Unknown")
    return df

# ── DB CHECK ─────────────────────────────────────────────────────────────────
if not os.path.exists(DB_PATH):
    st.error(f"⚠️ Database not found at **{DB_PATH}**\n\nUpdate `DB_PATH` at the top of this file.")
    st.stop()

df = load_data(DB_PATH)

if df.empty:
    st.warning("⏳ **log_data** table is empty — waiting for the parser to insert records.")
    st.info(f"Parser writes to: `{DB_PATH}` · Auto-refresh: 10 s")
    st.stop()

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Filters")
    st.markdown("---")

    # ── Global text search
    search_term = st.text_input("🔍 Search (track / station / model)", "").strip().lower()

    # ── Date range ────────────────────────────────────────────────────────────
    min_date = df["date"].min()
    max_date = df["date"].max()
    # Use a key so we can reset it programmatically
    date_range = st.date_input(
        "📅 Date range",
        value=(min_date, max_date),
        min_value=min_date, max_value=max_date,
        key="date_range_key"
    )
    start_date, end_date = (date_range if len(date_range) == 2 else (min_date, max_date))

    # ── Timestamp window (time-of-day) ────────────────────────────────────────
    st.markdown("""
    <div class="ts-filter-label" style="margin-top:12px;">
        ⏱ Timestamp window
    </div>
    """, unsafe_allow_html=True)

    # Pre-fill from data
    ts_default_start = df["timestamp"].min().replace(second=0, microsecond=0)
    ts_default_end   = df["timestamp"].max().replace(second=59, microsecond=999999)

    col_ts1, col_ts2 = st.columns(2)
    with col_ts1:
        st.caption("From time")
        ts_start_time = st.time_input(
            "From", value=ts_default_start.time(),
            label_visibility="collapsed", key="ts_start",
            step=60,
        )
    with col_ts2:
        st.caption("To time")
        ts_end_time = st.time_input(
            "To", value=ts_default_end.time(),
            label_visibility="collapsed", key="ts_end",
            step=60,
        )

    # Quick preset buttons (optional, keep as is)
    st.caption("Quick windows:")
    qp_cols = st.columns(3)
    with qp_cols[0]:
        if st.button("Day", key="qp_day", use_container_width=True):
            st.session_state["ts_start"] = time(0, 0)
            st.session_state["ts_end"]   = time(23, 59)
            st.rerun()
    with qp_cols[1]:
        if st.button("AM", key="qp_am", use_container_width=True):
            st.session_state["ts_start"] = time(6, 0)
            st.session_state["ts_end"]   = time(14, 0)
            st.rerun()
    with qp_cols[2]:
        if st.button("PM", key="qp_pm", use_container_width=True):
            st.session_state["ts_start"] = time(14, 0)
            st.session_state["ts_end"]   = time(22, 0)
            st.rerun()

    st.markdown("---")

    # ── Dimension filters ─────────────────────────────────────────────────────
    all_models    = sorted(df["model"].dropna().unique())
    all_lines     = sorted(df["line"].dropna().unique())
    all_processes = sorted(df["process"].dropna().unique())
    all_stations  = sorted(df["station"].dropna().unique())

    def multiselect_with_toggle(label, key, all_opts):
        c1, c2 = st.columns([4, 1])
        with c1:
            st.markdown(f"**{label}**")
        with c2:
            if st.button("↺", key=f"btn_{key}", use_container_width=True):
                st.session_state[key] = list(all_opts)
                st.rerun()
        default      = st.session_state.get(key, list(all_opts))
        valid_default = [v for v in default if v in all_opts]
        return st.multiselect("", all_opts, default=valid_default,
                              key=key, label_visibility="collapsed")

    sel_model   = multiselect_with_toggle("Model",   "f_model",   all_models)
    sel_line    = multiselect_with_toggle("Line",    "f_line",    all_lines)
    sel_process = multiselect_with_toggle("Process", "f_process", all_processes)
    sel_station = multiselect_with_toggle("Station", "f_station", all_stations)

    sel_status = st.selectbox("📊 Status", ["All", "Passed", "Failed"], index=0)

    st.markdown("---")
    db_size = os.path.getsize(DB_PATH) / 1024
    st.caption(f"📁 Total records: **{len(df):,}**")
    st.caption(f"🗓️ {min_date} → {max_date}")
    st.caption(f"💾 DB size: {db_size:.1f} KB")
    st.caption("🔄 Auto-refresh: 10 s")

    # ── RESET ALL FILTERS (including date and time) ──────────────────────────
    if st.button("↺ Reset ALL filters", use_container_width=True, key="reset_all"):
        # Reset multiselects
        for k in ["f_model", "f_line", "f_process", "f_station"]:
            st.session_state.pop(k, None)
        # Reset time window to full range
        st.session_state["ts_start"] = ts_default_start.time()
        st.session_state["ts_end"]   = ts_default_end.time()
        # Reset date range to full range
        st.session_state["date_range_key"] = (min_date, max_date)
        st.rerun()

# ── APPLY FILTERS (unchanged) ─────────────────────────────────────────────────
filt = df[
    (df["date"] >= start_date) & (df["date"] <= end_date) &
    (df["time_only"] >= ts_start_time) & (df["time_only"] <= ts_end_time) &
    (df["model"].isin(sel_model)) &
    (df["line"].isin(sel_line)) &
    (df["process"].isin(sel_process)) &
    (df["station"].isin(sel_station))
].copy()

if search_term:
    str_cols = ["track_id", "station", "model", "line", "process", "failed_testcode"]
    mask = pd.Series(False, index=filt.index)
    for col in str_cols:
        mask |= filt[col].astype(str).str.lower().str.contains(search_term, na=False)
    filt = filt[mask]

if sel_status == "Passed":
    filt = filt[filt["status"] == "P"]
elif sel_status == "Failed":
    filt = filt[filt["status"] == "F"]

# ── KPIs (with deltas restored) ──────────────────────────────────────────────
total           = len(filt)
passed          = int((filt["status"] == "P").sum())
failed          = int((filt["status"] == "F").sum())
pass_pct        = passed / total * 100 if total else 0
fail_pct        = failed / total * 100 if total else 0
unique_tracks   = filt["track_id"].nunique()
unique_stations = filt["station"].nunique()
unique_lines    = filt["line"].nunique()
has_code        = filt[(filt["status"] == "F") & (filt["failed_testcode"] != "")]["track_id"].nunique()
no_code         = filt[(filt["status"] == "F") & (filt["failed_testcode"] == "")]["track_id"].nunique()

# ── HEADER (unchanged) ───────────────────────────────────────────────────────
now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
ts_window_str = f"{ts_start_time.strftime('%H:%M')} → {ts_end_time.strftime('%H:%M')}"
date_window_str = f"{start_date} → {end_date}"

st.markdown(f"""
<div class="qad-header">
    <div>
        <div class="qad-title">QUALITY<span>.</span>ANALYTICS</div>
        <div class="qad-sub">manufacturing intelligence · real-time · dixon technologies</div>
    </div>
    <div class="qad-meta">
        <span class="live-dot"></span>LIVE &nbsp;|&nbsp; {now_str}<br>
        📅 {date_window_str}<br>
        ⏱ {ts_window_str}<br>
        {total:,} records · {os.path.basename(DB_PATH)}
    </div>
</div>
""", unsafe_allow_html=True)

# ── KPI ROW (with deltas) ────────────────────────────────────────────────────
k = st.columns(6)
k[0].metric("Records",       f"{total:,}",
            f"{total - len(df):+,}" if total != len(df) else "all data")
k[1].metric("Unique Tracks", f"{unique_tracks:,}")
k[2].metric("✅ Passed",      f"{passed:,}",   f"{pass_pct:.1f}%", delta_color="normal")
k[3].metric("❌ Failed",      f"{failed:,}",   f"{fail_pct:.1f}%", delta_color="inverse")
k[4].metric("Stations",      f"{unique_stations:,}")
k[5].metric("Lines",         f"{unique_lines:,}")

# ── TIME WINDOW INDICATOR (unchanged) ────────────────────────────────────────
if ts_start_time != ts_default_start.time() or ts_end_time != ts_default_end.time():
    st.info(
        f"⏱ **Active time filter:** {ts_start_time.strftime('%H:%M')} → "
        f"{ts_end_time.strftime('%H:%M')}  ·  "
        f"Date: {start_date} → {end_date}  ·  "
        f"Showing **{total:,}** of **{len(df):,}** records"
    )

st.markdown("")

# ── TABS (unchanged, keep all your charts and insights) ──────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "▦  OVERVIEW",
    "⚑  FAILURES",
    "⌛  TRENDS",
    "⊞  STATIONS",
    "≡  RAW DATA",
])




# ─────────────────────────────────────────────────────────────────────────────
with tab1:
    c1, c2 = st.columns([1, 1.6])

    with c1:
        st.markdown('<div class="section-label">Pass / Fail Split</div>', unsafe_allow_html=True)
        if total > 0:
            donut_df = pd.DataFrame({"status": ["Passed", "Failed"],
                                     "count":  [passed, failed]})
            fig = px.pie(donut_df, values="count", names="status", hole=0.62,
                         color="status",
                         color_discrete_map={"Passed": C_PASS, "Failed": C_FAIL})
            fig.update_traces(textinfo="percent+label", textposition="inside",
                              marker=dict(line=dict(color="#f5f4f0", width=3)))
            fig = chart_layout(fig, height=280,
                               margin=dict(t=10, b=10, l=0, r=0), legend=False)
            st.plotly_chart(fig, use_container_width=True, key="donut_split")
        else:
            st.info("No data in selected range")

    with c2:
        st.markdown('<div class="section-label">Tests by Process</div>', unsafe_allow_html=True)
        if not filt.empty:
            proc = filt.groupby(["process", "status_display"]).size().reset_index(name="count")
            fig = px.bar(proc, x="process", y="count", color="status_display",
                         color_discrete_map={"Passed": C_PASS, "Failed": C_FAIL},
                         barmode="stack")
            fig.update_layout(xaxis_tickangle=-30, xaxis_title="", yaxis_title="Tests")
            fig = chart_layout(fig, height=280)
            st.plotly_chart(fig, use_container_width=True, key="tests_by_process")
        else:
            st.info("No data")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<div class="section-label">Tests by Model</div>', unsafe_allow_html=True)
        if not filt.empty:
            mod = filt.groupby(["model", "status_display"]).size().reset_index(name="count")
            fig = px.bar(mod, x="model", y="count", color="status_display",
                         color_discrete_map={"Passed": C_PASS, "Failed": C_FAIL},
                         barmode="group")
            fig.update_layout(xaxis_tickangle=-35, xaxis_title="", yaxis_title="Tests")
            fig = chart_layout(fig, height=300)
            st.plotly_chart(fig, use_container_width=True, key="tests_by_model")
        else:
            st.info("No data")

    with c2:
        st.markdown('<div class="section-label">Tests by Line</div>', unsafe_allow_html=True)
        if not filt.empty:
            line_df = filt.groupby(["line", "status_display"]).size().reset_index(name="count")
            fig = px.bar(line_df, x="line", y="count", color="status_display",
                         color_discrete_map={"Passed": C_PASS, "Failed": C_FAIL},
                         barmode="group")
            fig.update_layout(xaxis_tickangle=-35, xaxis_title="", yaxis_title="Tests")
            fig = chart_layout(fig, height=300)
            st.plotly_chart(fig, use_container_width=True, key="tests_by_line")
        else:
            st.info("No data")

    st.markdown('<div class="section-label">Fail Rate % — Model × Line</div>', unsafe_allow_html=True)
    if not filt.empty and filt["model"].nunique() > 0:
        tot_ml  = filt.groupby(["model", "line"]).size().reset_index(name="total")
        fail_ml = filt[filt["status"] == "F"].groupby(["model", "line"]).size().reset_index(name="fails")
        ml = tot_ml.merge(fail_ml, on=["model", "line"], how="left").fillna(0)
        ml["fail_pct"] = (ml["fails"] / ml["total"] * 100).round(1)
        heat = ml.pivot(index="model", columns="line", values="fail_pct").fillna(0)
        fig = px.imshow(heat, text_auto=True, color_continuous_scale="Blues",
                        aspect="auto", labels=dict(x="Line", y="Model", color="Fail %"))
        fig.update_coloraxes(colorbar=dict(tickfont=dict(color=FONT_COLOR)))
        fig = chart_layout(fig, height=350,
                           margin=dict(t=20, b=40, l=10, r=10), legend=False)
        fig.update_xaxes(side="bottom")
        st.plotly_chart(fig, use_container_width=True, key="heatmap_model_line")
    else:
        st.info("Not enough data to display Model × Line matrix.")

    # =================== NEW INSIGHT 1: Model × Process Heatmap ===================
    st.markdown('<div class="section-label">Fail Rate % — Model × Process</div>', unsafe_allow_html=True)
    if not filt.empty and filt["model"].nunique() > 0 and filt["process"].nunique() > 0:
        tot_mp = filt.groupby(["model", "process"]).size().reset_index(name="total")
        fail_mp = filt[filt["status"] == "F"].groupby(["model", "process"]).size().reset_index(name="fails")
        mp = tot_mp.merge(fail_mp, on=["model", "process"], how="left").fillna(0)
        mp["fail_pct"] = (mp["fails"] / mp["total"] * 100).round(1)

        heat_mp = mp.pivot(index="model", columns="process", values="fail_pct").fillna(0)

        fig_mp = px.imshow(
            heat_mp,
            text_auto=True,
            color_continuous_scale="Blues",
            aspect="auto",
            labels=dict(x="Process", y="Model", color="Fail %")
        )
        fig_mp.update_coloraxes(colorbar=dict(tickfont=dict(color=FONT_COLOR)))
        fig_mp = chart_layout(
            fig_mp,
            height=350,
            margin=dict(t=20, b=40, l=10, r=10),
            legend=False
        )
        fig_mp.update_xaxes(side="bottom")
        st.plotly_chart(fig_mp, use_container_width=True, key="heatmap_model_process")
    else:
        st.info("Not enough data to display Model × Process matrix (missing model or process information).")

    # =================== NEW INSIGHT 2: all Model–Process Combinations ===================
    st.markdown('<div class="section-label">all Model–Process Combinations (by count & fail rate)</div>', unsafe_allow_html=True)
    if not filt.empty and "model" in filt.columns and "process" in filt.columns:
        all_combos = (
            filt.groupby(["model", "process"])
            .agg(
                total=("status", "count"),
                fails=("status", lambda x: (x == "F").sum())
            )
            .reset_index()
        )
        all_combos["fail_rate"] = (all_combos["fails"] / all_combos["total"] * 100).round(1)
        all_combos = all_combos[all_combos["total"] >= 1]  # only consider pairs with at least 1 test
        all_combos = all_combos.sort_values("fail_rate", ascending=False).head(15)

        if not all_combos.empty:
            # Show both rate and count for passed and failed  for all model with different processes
            all_combos["passed"] = all_combos["total"] - all_combos["fails"]
            st.dataframe(
                all_combos.rename(
                    columns={
                        "model": "Model",
                        "process": "Process",
                        "total": "Total Tests",
                        "passed": "Passed",
                        "fails": "Failures",
                        "fail_rate": "Fail Rate (%)"
                    }
                ),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No model–process combinations with at least 1 test found.")
    else:
        st.info("Model or process data missing in the current selection.")


    
    # ── Unique failed track_ids per model × process ──
    unique_fail_counts = (
     filt[filt["status"] == "F"]
     .groupby(["model", "process"])["track_id"]
     .nunique()
     .unstack(fill_value=0)
    )

    fig = go.Figure(data=go.Heatmap(
     z=unique_fail_counts.values,
     x=unique_fail_counts.columns,
     y=unique_fail_counts.index,
     text=unique_fail_counts.values,
     texttemplate="%{text:.0f}",
     textfont={"size": 15, "color": "black"},
     colorscale="Reds",
     showscale=True,
     hovertemplate="Model: %{y}<br>Process: %{x}<br>Unique failed tracks: %{z}<extra></extra>",
    ))

    fig.update_layout(
     title="Unique Failed Tracks per Model & Process",
     xaxis_title="Process",
     yaxis_title="Model",
     xaxis_side="top",               # labels on top usually more readable
     height=500 + 18 * len(unique_fail_counts.index),
     width=700 + 8 * len(unique_fail_counts.columns),
     coloraxis_colorbar=dict(
        title="Unique fails",
        thickness=20,
        len=0.7,
     ),
    margin=dict(l=10, r=80, t=70, b=10),
    )

    st.plotly_chart(fig, use_container_width=True)   


# ─────────────────────────────────────────────────────────────────────────────
with tab2:
    fails       = filt[filt["status"] == "F"].copy()
    coded_fails = fails[fails["failed_testcode"] != ""]

    s1, s2, s3 = st.columns(3)
    #s1.metric("Total Failed Records", f"{len(fails):,}")
    #s2.metric("With Fail Code",       f"{has_code:,}")
    #s3.metric("Blank Fail Code",      f"{no_code:,}")

    if no_code > 0:
        st.info(
            f"ℹ️  **{no_code:,} failed units** have no testcode stored — "
            "TH4 parts[10] was blank at status=F. Investigate those track IDs in the raw files."
        )

    st.markdown('<div class="section-label">Pareto — Top Failure Codes</div>', unsafe_allow_html=True)
    if not coded_fails.empty:
        fc = coded_fails["failed_testcode"].value_counts().reset_index()
        fc.columns = ["code", "count"]
        fc = fc.head(20)
        fc["cum_pct"] = (fc["count"].cumsum() / fc["count"].sum() * 100).round(1)

        fig = go.Figure()
        fig.add_trace(go.Bar(x=fc["code"], y=fc["count"], name="Count",
                             marker_color=C_FAIL,
                             marker_line=dict(color="#f5f4f0", width=1)))
        fig.add_trace(go.Scatter(x=fc["code"], y=fc["cum_pct"],
                                 name="Cumulative %", yaxis="y2",
                                 line=dict(color=C_BLUE, width=2),
                                 mode="lines+markers", marker=dict(size=5)))
        fig.update_layout(
            xaxis_tickangle=-40, xaxis_title="", yaxis_title="Count",
            yaxis2=dict(title="Cumulative %", overlaying="y", side="right",
                        range=[0, 110], tickfont=dict(color=FONT_COLOR),
                        gridcolor="rgba(0,0,0,0)"),
            hovermode="x unified",
        )
        fig = chart_layout(fig, height=420, margin=dict(t=20, b=110, l=10, r=60))
        st.plotly_chart(fig, use_container_width=True, key="pareto_failure_codes")

        detail = fc.copy()
        detail["example_track"] = [
            fails[fails["failed_testcode"] == c]["track_id"].iloc[0]
            if not fails[fails["failed_testcode"] == c].empty else "—"
            for c in detail["code"]
        ]
        detail["% of failures"] = (detail["count"] / len(coded_fails) * 100).round(1)
        st.dataframe(
            detail[["code", "count", "% of failures", "cum_pct", "example_track"]].rename(
                columns={"code": "Fail Code", "count": "Count",
                         "cum_pct": "Cum %", "example_track": "Example Track"}),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("No failures with a testcode in current selection.")

    st.markdown('<div class="section-label">Failures by Process</div>', unsafe_allow_html=True)
    if not fails.empty:
        proc_fail = fails["process"].value_counts().reset_index()
        proc_fail.columns = ["process", "count"]
        fig = px.pie(proc_fail, values="count", names="process", hole=0.5,
                     color_discrete_sequence=["#1d4ed8","#0ea5e9","#6366f1",
                                              "#ec4899","#f59e0b","#10b981"])
        fig.update_traces(textinfo="percent+label", textposition="inside",
                          marker=dict(line=dict(color="#f5f4f0", width=2)))
        fig = chart_layout(fig, height=320, margin=dict(t=10, b=10, l=0, r=0))
        st.plotly_chart(fig, use_container_width=True, key="failures_by_process_pie")
    else:
        st.info("No failures in current selection.")

    st.markdown('<div class="section-label">Failure Treemap — Process → Station → Code</div>', unsafe_allow_html=True)
    if not fails.empty:
        tree_col   = "failed_testcode" if not coded_fails.empty else "status_display"
        tree_label = tree_col.replace("_", " ").title()
        tree_df    = fails.groupby(["process", "station", tree_col]).size().reset_index(name="count")
        tree_df.columns = ["process", "station", tree_label, "count"]
        fig = px.treemap(tree_df, path=["process", "station", tree_label],
                         values="count", color="count",
                         color_continuous_scale="Blues")
        fig.update_layout(height=460, margin=dict(t=10, b=10, l=0, r=0),
                          paper_bgcolor=PAPER_BG, plot_bgcolor=CHART_BG,
                          font=dict(color=FONT_COLOR))
        st.plotly_chart(fig, use_container_width=True, key="failure_treemap")
    else:
        st.info("No failure data to display treemap.")

# ─────────────────────────────────────────────────────────────────────────────
with tab3:
    st.markdown('<div class="section-label">Hourly Test Volume</div>', unsafe_allow_html=True)
    if not filt.empty:
        hourly = filt.groupby(["hour", "status_display"]).size().reset_index(name="count")
        fig = px.line(hourly, x="hour", y="count", color="status_display",
                      color_discrete_map={"Passed": C_PASS, "Failed": C_FAIL},
                      markers=True)
        fig.update_layout(xaxis=dict(dtick=2, title="Hour of day"), yaxis_title="Tests")
        fig = chart_layout(fig, height=360)
        st.plotly_chart(fig, use_container_width=True, key="hourly_volume")
    else:
        st.info("No data")

    if filt["date"].nunique() > 1:
        st.markdown('<div class="section-label">Daily Trend</div>', unsafe_allow_html=True)
        daily = filt.groupby(["date", "status_display"]).size().reset_index(name="count")
        fig = px.area(daily, x="date", y="count", color="status_display",
                      color_discrete_map={"Passed": C_PASS, "Failed": C_FAIL},
                      markers=True)
        fig.update_layout(xaxis_title="", yaxis_title="Tests")
        fig = chart_layout(fig, height=360)
        st.plotly_chart(fig, use_container_width=True, key="daily_trend")
    else:
        st.markdown('<div class="section-label">Minute-by-Minute (within time window)</div>', unsafe_allow_html=True)
        if not filt.empty:
            filt_copy = filt.copy()
            filt_copy["minute"] = filt_copy["timestamp"].dt.floor("1min")
            bymin = filt_copy.groupby(["minute", "status_display"]).size().reset_index(name="count")
            fig = px.bar(bymin, x="minute", y="count", color="status_display",
                         color_discrete_map={"Passed": C_PASS, "Failed": C_FAIL},
                         barmode="stack")
            fig.update_layout(xaxis_title="Time", yaxis_title="Tests")
            fig = chart_layout(fig, height=360)
            st.plotly_chart(fig, use_container_width=True, key="minute_by_minute")
        else:
            st.info("No data")

    st.markdown('<div class="section-label">Fail Rate % per Hour</div>', unsafe_allow_html=True)
    if not filt.empty:
        hr_total = filt.groupby("hour").size().reset_index(name="total")
        hr_fail  = filt[filt["status"] == "F"].groupby("hour").size().reset_index(name="fails")
        hr = hr_total.merge(hr_fail, on="hour", how="left").fillna(0)
        hr["fail_rate"] = (hr["fails"] / hr["total"] * 100).round(2)
        fig = px.bar(hr, x="hour", y="fail_rate",
                     color="fail_rate", color_continuous_scale="Reds",
                     labels={"fail_rate": "Fail %", "hour": "Hour"})
        fig.update_layout(xaxis=dict(dtick=2))
        fig = chart_layout(fig, height=300, legend=False)
        st.plotly_chart(fig, use_container_width=True, key="fail_rate_hour")
    else:
        st.info("No data")

# ─────────────────────────────────────────────────────────────────────────────
with tab4:
    st.markdown('<div class="section-label">Station Performance — Top 15</div>', unsafe_allow_html=True)
    if not filt.empty:
        st_stats = filt.groupby("station").agg(
            total=("status", "count"),
            passed=("passed", "sum"),
        ).reset_index()
        st_stats["failed"]    = st_stats["total"] - st_stats["passed"]
        st_stats["fail_rate"] = (st_stats["failed"] / st_stats["total"] * 100).round(1)
        st_stats = st_stats.sort_values("total", ascending=False)

        top15  = st_stats.head(15)
        melted = top15.melt(id_vars="station", value_vars=["passed", "failed"],
                            var_name="status", value_name="count")
        melted["status"] = melted["status"].str.capitalize()
        fig = px.bar(melted, x="station", y="count", color="status",
                     color_discrete_map={"Passed": C_PASS, "Failed": C_FAIL},
                     barmode="stack")
        fig.update_layout(xaxis_tickangle=-40, xaxis_title="", yaxis_title="Tests")
        fig = chart_layout(fig, height=380)
        st.plotly_chart(fig, use_container_width=True, key="station_performance_top15")

        st.markdown('<div class="section-label">Fail Rate vs Volume</div>', unsafe_allow_html=True)
        fig2 = px.scatter(st_stats, x="total", y="fail_rate",
                          size="total", hover_name="station",
                          color="fail_rate", color_continuous_scale="Reds",
                          labels={"total": "Total Tests", "fail_rate": "Fail %"})
        fig2 = chart_layout(fig2, height=340, legend=False)
        st.plotly_chart(fig2, use_container_width=True, key="fail_rate_vs_volume")

        st.markdown('<div class="section-label">All Stations Table</div>', unsafe_allow_html=True)
        st.dataframe(
            st_stats.rename(columns={"station": "Station", "total": "Total",
                                     "passed": "Passed", "failed": "Failed",
                                     "fail_rate": "Fail %"})
                    .sort_values("Fail %", ascending=False),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("No data for station analysis.")

# ─────────────────────────────────────────────────────────────────────────────
with tab5:
    st.markdown('<div class="section-label">Filtered Records</div>', unsafe_allow_html=True)

    # Active filter summary
    st.markdown(
        f"**Active filters:** "
        f"Date `{start_date} → {end_date}` · "
        f"Time `{ts_start_time.strftime('%H:%M')} → {ts_end_time.strftime('%H:%M')}` · "
        f"`{total:,}` records shown",
        unsafe_allow_html=False,
    )

    display_cols = ["timestamp", "track_id", "model", "line", "station",
                    "process", "status_display", "failed_testcode"]
    display_df = filt[[c for c in display_cols if c in filt.columns]].copy()
    display_df["timestamp"] = display_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    display_df = display_df.sort_values("timestamp", ascending=False)

    st.dataframe(display_df, use_container_width=True, height=480, hide_index=True)

    csv = display_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download Filtered Data as CSV",
        csv, "factory_quality_filtered.csv", "text/csv",
        use_container_width=True,
    )

# ── FOOTER (unchanged) ───────────────────────────────────────────────────────
st.markdown("---")
st.caption(
    f"⚙️ Quality Analytics Dashboard  ·  DB: {DB_PATH}  ·  "
    f"Refreshed: {now_str}  ·  "
    f"Filtered {total:,} / {len(df):,} records  ·  "
    f"Time window: {ts_start_time.strftime('%H:%M')} → {ts_end_time.strftime('%H:%M')}"
)