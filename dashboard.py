#!/usr/bin/env python3
"""
Streamlit Dashboard — AP Prohibited Properties Explorer
=========================================================
Reads data/all_villages.csv and provides interactive filtering,
pattern matching, and exploration.

Usage:
    streamlit run dashboard.py
    streamlit run dashboard.py -- --data ./data/all_villages.csv
"""

import sys
import os
import argparse

import pandas as pd
import streamlit as st


# ─── Config ──────────────────────────────────────────────────────────────────

DEFAULT_DATA_PATH = "./data/all_villages.csv"


@st.cache_data
def load_data(path):
    """Load CSV once and cache it."""
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    # Clean up column names (strip whitespace)
    df.columns = [c.strip() for c in df.columns]
    return df


def main():
    st.set_page_config(
        page_title="AP Prohibited Properties Explorer",
        page_icon="🏠",
        layout="wide",
    )

    st.title("🏠 AP Prohibited Properties Explorer")

    # Determine data path
    data_path = DEFAULT_DATA_PATH
    if len(sys.argv) > 1:
        for i, arg in enumerate(sys.argv):
            if arg == "--data" and i + 1 < len(sys.argv):
                data_path = sys.argv[i + 1]

    if not os.path.exists(data_path):
        st.error(f"Data file not found: `{data_path}`")
        st.info("Run `python consolidate_csv.py` first to create the combined CSV.")
        return

    # Load data
    with st.spinner("Loading data..."):
        df = load_data(data_path)

    # ─── Sidebar: Overview ───────────────────────────────────────────────
    st.sidebar.header("📊 Dataset Overview")
    st.sidebar.metric("Total Rows", f"{len(df):,}")
    st.sidebar.metric("Total Columns", len(df.columns))

    if "_district" in df.columns:
        st.sidebar.metric("Districts", df["_district"].nunique())
    if "_mandal" in df.columns:
        st.sidebar.metric("Mandals", df[["_district", "_mandal"]].drop_duplicates().shape[0])
    if "_village" in df.columns:
        st.sidebar.metric("Villages", df[["_district", "_mandal", "_village"]].drop_duplicates().shape[0])

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Columns in data:**")
    st.sidebar.code("\n".join(df.columns.tolist()), language=None)

    # ─── Tab Layout ──────────────────────────────────────────────────────
    tab_filter, tab_search, tab_stats, tab_raw = st.tabs([
        "🔍 Filter & Explore", "🔎 Pattern Search", "📈 Statistics", "📋 Raw Data"
    ])

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 1: Filter & Explore
    # ═══════════════════════════════════════════════════════════════════════
    with tab_filter:
        st.subheader("Filter by Location")

        col1, col2, col3 = st.columns(3)

        # District filter
        with col1:
            districts = ["All"] + sorted(df["_district"].dropna().unique().tolist()) if "_district" in df.columns else ["All"]
            selected_district = st.selectbox("District", districts, key="filter_district")

        # Mandal filter (depends on district)
        filtered = df.copy()
        if selected_district != "All" and "_district" in df.columns:
            filtered = filtered[filtered["_district"] == selected_district]

        with col2:
            mandals = ["All"] + sorted(filtered["_mandal"].dropna().unique().tolist()) if "_mandal" in filtered.columns else ["All"]
            selected_mandal = st.selectbox("Mandal", mandals, key="filter_mandal")

        if selected_mandal != "All" and "_mandal" in filtered.columns:
            filtered = filtered[filtered["_mandal"] == selected_mandal]

        with col3:
            villages = ["All"] + sorted(filtered["_village"].dropna().unique().tolist()) if "_village" in filtered.columns else ["All"]
            selected_village = st.selectbox("Village", villages, key="filter_village")

        if selected_village != "All" and "_village" in filtered.columns:
            filtered = filtered[filtered["_village"] == selected_village]

        # ─── Column-level filters ────────────────────────────────────────
        st.subheader("Filter by Column Values")

        # Let user pick which columns to filter on
        filterable_cols = [c for c in df.columns if not c.startswith("_")]
        selected_filter_cols = st.multiselect(
            "Select columns to filter on",
            filterable_cols,
            key="filter_cols",
        )

        for col in selected_filter_cols:
            unique_vals = filtered[col].dropna().unique()
            if len(unique_vals) <= 100:
                chosen = st.multiselect(
                    f"Filter `{col}`",
                    sorted([str(v) for v in unique_vals]),
                    key=f"filter_val_{col}",
                )
                if chosen:
                    filtered = filtered[filtered[col].astype(str).isin(chosen)]
            else:
                val_input = st.text_input(
                    f"Filter `{col}` (type to search, supports partial match)",
                    key=f"filter_text_{col}",
                )
                if val_input:
                    filtered = filtered[
                        filtered[col].astype(str).str.contains(val_input, case=False, na=False)
                    ]

        # Show results
        st.markdown(f"**Showing {len(filtered):,} rows** (of {len(df):,} total)")
        st.dataframe(filtered, use_container_width=True, height=500)

        # Download filtered
        if len(filtered) > 0:
            csv_download = filtered.to_csv(index=False, encoding="utf-8-sig")
            st.download_button(
                "⬇️ Download filtered data as CSV",
                csv_download,
                file_name="filtered_properties.csv",
                mime="text/csv",
            )

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 2: Pattern Search (regex / text search across all columns)
    # ═══════════════════════════════════════════════════════════════════════
    with tab_search:
        st.subheader("Search Across All Data")

        search_col1, search_col2 = st.columns([3, 1])
        with search_col1:
            search_term = st.text_input(
                "Search term (searches all columns, supports regex)",
                placeholder="e.g. KRISHNA or TANK or 2019 or survey pattern like ^10[0-9]/",
                key="global_search",
            )
        with search_col2:
            case_sensitive = st.checkbox("Case sensitive", value=False, key="case_sens")
            use_regex = st.checkbox("Use regex", value=True, key="use_regex")

        # Column-specific search
        st.markdown("**Or search in a specific column:**")
        col_search_col, col_search_term = st.columns([1, 2])
        with col_search_col:
            search_in_col = st.selectbox(
                "Column",
                ["(All columns)"] + list(df.columns),
                key="search_col",
            )
        with col_search_term:
            col_search_val = st.text_input(
                "Search value",
                placeholder="Type your search...",
                key="col_search_val",
            )

        # Execute search
        search_results = df.copy()
        active_search = search_term or col_search_val

        if search_term:
            mask = pd.Series([False] * len(df))
            for col in df.columns:
                try:
                    col_mask = df[col].astype(str).str.contains(
                        search_term,
                        case=case_sensitive,
                        na=False,
                        regex=use_regex,
                    )
                    mask = mask | col_mask
                except Exception:
                    pass
            search_results = df[mask]

        if col_search_val:
            if search_in_col == "(All columns)":
                mask = pd.Series([False] * len(search_results))
                for col in search_results.columns:
                    try:
                        col_mask = search_results[col].astype(str).str.contains(
                            col_search_val, case=case_sensitive, na=False, regex=use_regex
                        )
                        mask = mask | col_mask
                    except Exception:
                        pass
                search_results = search_results[mask]
            else:
                search_results = search_results[
                    search_results[search_in_col].astype(str).str.contains(
                        col_search_val, case=case_sensitive, na=False, regex=use_regex
                    )
                ]

        if active_search:
            st.markdown(f"**Found {len(search_results):,} matching rows**")
            st.dataframe(search_results, use_container_width=True, height=500)

            if len(search_results) > 0:
                csv_download = search_results.to_csv(index=False, encoding="utf-8-sig")
                st.download_button(
                    "⬇️ Download search results as CSV",
                    csv_download,
                    file_name="search_results.csv",
                    mime="text/csv",
                )
        else:
            st.info("Enter a search term above to find matching records.")

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 3: Statistics
    # ═══════════════════════════════════════════════════════════════════════
    with tab_stats:
        st.subheader("Data Statistics")

        # District-level summary
        if "_district" in df.columns:
            st.markdown("### Properties by District")
            dist_counts = df.groupby("_district").size().reset_index(name="count")
            dist_counts = dist_counts.sort_values("count", ascending=False)
            st.bar_chart(dist_counts.set_index("_district")["count"])
            st.dataframe(dist_counts, use_container_width=True)

        # Mandal-level for selected district
        if "_district" in df.columns and "_mandal" in df.columns:
            st.markdown("### Properties by Mandal (select a district)")
            dist_for_stats = st.selectbox(
                "District",
                sorted(df["_district"].dropna().unique().tolist()),
                key="stats_district",
            )
            mandal_counts = (
                df[df["_district"] == dist_for_stats]
                .groupby("_mandal").size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
            )
            st.bar_chart(mandal_counts.set_index("_mandal")["count"])
            st.dataframe(mandal_counts, use_container_width=True)

        # Value counts for any column
        st.markdown("### Value Counts for Any Column")
        count_col = st.selectbox("Select column", df.columns.tolist(), key="value_count_col")
        top_n = st.slider("Top N values", 10, 100, 25, key="top_n")
        vc = df[count_col].value_counts().head(top_n).reset_index()
        vc.columns = [count_col, "count"]
        st.dataframe(vc, use_container_width=True)

        # Notification date distribution if present
        if "Notification Date" in df.columns:
            st.markdown("### Notifications Over Time")
            try:
                df_dates = df.copy()
                df_dates["_notif_date"] = pd.to_datetime(
                    df_dates["Notification Date"], format="mixed", dayfirst=True, errors="coerce"
                )
                date_counts = df_dates["_notif_date"].dt.year.value_counts().sort_index()
                st.bar_chart(date_counts)
            except Exception as e:
                st.warning(f"Could not parse dates: {e}")

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 4: Raw Data
    # ═══════════════════════════════════════════════════════════════════════
    with tab_raw:
        st.subheader("Raw Data Browser")
        st.markdown(f"Showing all **{len(df):,}** rows. Use the table's built-in sort and search.")

        # Column selector
        visible_cols = st.multiselect(
            "Select columns to display",
            df.columns.tolist(),
            default=df.columns.tolist(),
            key="raw_cols",
        )

        if visible_cols:
            st.dataframe(df[visible_cols], use_container_width=True, height=600)
        else:
            st.dataframe(df, use_container_width=True, height=600)

        # Full download
        csv_full = df.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            "⬇️ Download full dataset as CSV",
            csv_full,
            file_name="all_villages_full.csv",
            mime="text/csv",
        )


if __name__ == "__main__":
    main()