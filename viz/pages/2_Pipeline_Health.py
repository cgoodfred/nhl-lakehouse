"""Pipeline health dashboard backed by Iceberg metadata and existing tables."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from lib import catalog, fmt_season, load_table_arrow, metric_card

st.set_page_config(page_title="Pipeline Health", layout="wide", page_icon="📊")

ACCENT = "#ffce00"
DEFAULT_STALE_AFTER_HOURS = 48
SNAPSHOT_HISTORY_DAYS = 30
TABLE_FRESHNESS_HOURS = {
    "silver.tracking_attempts": 36,
    "silver.tracking_frames": 36,
    "gold.goal_tracking_status": 36,
}

TABLES = [
    "silver.games",
    "silver.teams",
    "silver.players",
    "silver.plays",
    "silver.game_rosters",
    "silver.tracking_attempts",
    "silver.tracking_frames",
    "gold.player_shots",
    "gold.goal_tracking_status",
]

ATTEMPT_FAILURE_STATUSES = {
    "http_other",
    "fetch_error",
    "invalid_payload",
    "timeout",
    "error",
}
ATTEMPT_NOT_TRACKED_STATUSES = {"http_404", "not_tracked", "no_url"}
FRESHNESS_LABELS = {
    "healthy": "[ok] HEALTHY",
    "stale": "[warn] STALE",
    "empty": "[empty] EMPTY",
    "missing": "[missing] MISSING",
    "unknown": "[unknown] UNKNOWN",
}


SectionData = dict[str, Any]


def _section(data: Any = None, error: str | None = None) -> SectionData:
    return {"data": data, "error": error}


def _section_error(exc: Exception) -> SectionData:
    return _section(error=f"{exc.__class__.__name__}: {exc}")


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _fmt_int(value: Any) -> str:
    if value is None or pd.isna(value):
        return "unknown"
    return f"{int(value):,}"


def _fmt_bytes(value: Any) -> str:
    if value is None or pd.isna(value):
        return "unknown"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size) < 1024 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return "unknown"


def _fmt_time(value: datetime | None) -> str:
    if value is None:
        return "unknown"
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


def _summary_int(summary: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        raw = summary.get(key)
        if raw is None:
            continue
        try:
            return int(raw)
        except (TypeError, ValueError):
            continue
    return None


def _snapshot_timestamp(snapshot) -> datetime | None:
    ts_ms = getattr(snapshot, "timestamp_ms", None)
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC)


def _freshness_status(table_name: str, row_count: int | None, last_write: datetime | None) -> str:
    if last_write is None:
        return "missing"
    if row_count == 0:
        return "empty"
    stale_after = TABLE_FRESHNESS_HOURS.get(table_name, DEFAULT_STALE_AFTER_HOURS)
    age_hours = (_now_utc() - last_write).total_seconds() / 3600
    if age_hours > stale_after:
        return "stale"
    return "healthy"


def _latest_snapshot(table):
    current = table.current_snapshot()
    if current is not None:
        return current
    snapshots = list(table.snapshots())
    if not snapshots:
        return None
    return max(snapshots, key=lambda snap: getattr(snap, "timestamp_ms", 0) or 0)


@st.cache_data(ttl=60, show_spinner="Loading Iceberg table metadata...")
def _table_health() -> SectionData:
    rows = []
    try:
        cat = catalog()
    except Exception as exc:
        return _section_error(exc)

    for name in TABLES:
        tier, table = name.split(".", 1)
        try:
            iceberg_table = cat.load_table(name)
            snapshots = list(iceberg_table.snapshots())
            latest = _latest_snapshot(iceberg_table)
            summary = dict(getattr(latest, "summary", {}) or {}) if latest else {}
            last_write = _snapshot_timestamp(latest) if latest else None
            row_count = _summary_int(summary, "total-records")
            added_records = _summary_int(summary, "added-records")
            data_files = _summary_int(summary, "total-data-files", "added-data-files")
            file_size = _summary_int(
                summary,
                "total-file-size",
                "total-files-size",
                "added-files-size",
                "added-file-size",
            )
            operation = summary.get("operation") or getattr(latest, "operation", None)
            status = _freshness_status(name, row_count, last_write)
            error = None
        except Exception as exc:
            snapshots = []
            last_write = None
            row_count = None
            added_records = None
            data_files = None
            file_size = None
            operation = None
            status = "missing"
            error = f"{exc.__class__.__name__}: {exc}"
        rows.append(
            {
                "table_name": name,
                "tier": tier,
                "table": table,
                "row_count": row_count,
                "last_write": last_write,
                "last_write_display": _fmt_time(last_write),
                "operation": operation or "unknown",
                "snapshot_count": len(snapshots),
                "added_records": added_records,
                "data_files": data_files,
                "file_size_bytes": file_size,
                "freshness_status": status,
                "error": error,
            }
        )
    return _section(pd.DataFrame(rows))


@st.cache_data(ttl=60, show_spinner="Loading seasons...")
def _seasons() -> SectionData:
    try:
        arrow = load_table_arrow("silver.games")
        if "season" not in arrow.column_names or arrow.num_rows == 0:
            return _section([])
        seasons = sorted(
            {int(value) for value in arrow.column("season").to_pylist() if value is not None}
        )
        return _section(seasons)
    except Exception as exc:
        return _section_error(exc)


@st.cache_data(ttl=60, show_spinner="Loading tracking attempts...")
def _tracking_attempts() -> SectionData:
    try:
        arrow = load_table_arrow("silver.tracking_attempts")
        if arrow.num_rows == 0:
            return _section(pd.DataFrame(columns=["status", "attempts"]))
        df = arrow.select(["status"]).to_pandas()
        counts = (
            df["status"].fillna("unknown").value_counts().rename_axis("status").reset_index(
                name="attempts"
            )
        )
        return _section(counts)
    except Exception as exc:
        return _section_error(exc)


@st.cache_data(ttl=60, show_spinner="Loading tracking coverage...")
def _tracking_coverage() -> SectionData:
    try:
        arrow = load_table_arrow("gold.goal_tracking_status")
        if arrow.num_rows == 0:
            return _section(pd.DataFrame(columns=["tracking_status", "goals"]))
        df = arrow.select(["tracking_status"]).to_pandas()
        counts = (
            df["tracking_status"]
            .fillna("unknown")
            .value_counts()
            .rename_axis("tracking_status")
            .reset_index(name="goals")
        )
        return _section(counts)
    except Exception as exc:
        return _section_error(exc)


@st.cache_data(ttl=60, show_spinner="Loading snapshot history...")
def _snapshot_history() -> SectionData:
    rows = []
    cutoff = _now_utc() - pd.Timedelta(days=SNAPSHOT_HISTORY_DAYS)
    try:
        cat = catalog()
    except Exception as exc:
        return _section_error(exc)

    for name in TABLES:
        try:
            table = cat.load_table(name)
            for snapshot in table.snapshots():
                summary = dict(getattr(snapshot, "summary", {}) or {})
                ts = _snapshot_timestamp(snapshot)
                if ts is None or ts < cutoff:
                    continue
                rows.append(
                    {
                        "timestamp": ts,
                        "date": ts.date(),
                        "table_name": name,
                        "operation": summary.get("operation", "unknown"),
                        "added_records": _summary_int(summary, "added-records"),
                        "total_records": _summary_int(summary, "total-records"),
                        "snapshot_id": getattr(snapshot, "snapshot_id", None),
                    }
                )
        except Exception:
            continue
    return _section(pd.DataFrame(rows))


def _metric_value(health: pd.DataFrame, table_name: str) -> str:
    row = health[health["table_name"] == table_name]
    if row.empty:
        return "unknown"
    return _fmt_int(row.iloc[0]["row_count"])


def _render_cards(health: pd.DataFrame, seasons: SectionData, coverage: SectionData) -> None:
    seasons_data = seasons["data"]
    coverage_data = coverage["data"]

    if seasons["error"]:
        season_value = "unknown"
    elif seasons_data:
        season_value = f"{fmt_season(seasons_data[0])} - {fmt_season(seasons_data[-1])}"
    else:
        season_value = "0"

    available_pct = "unknown"
    if coverage["error"] is None and coverage_data is not None and not coverage_data.empty:
        total = int(coverage_data["goals"].sum())
        available = int(
            coverage_data.loc[
                coverage_data["tracking_status"] == "available", "goals"
            ].sum()
        )
        available_pct = f"{available / total:.1%}" if total else "0.0%"

    cards = [
        ("Seasons", season_value),
        ("Games", _metric_value(health, "silver.games")),
        ("Plays", _metric_value(health, "silver.plays")),
        ("Players", _metric_value(health, "silver.players")),
        ("Goals", _metric_value(health, "gold.player_shots")),
        ("Tracking Frames", _metric_value(health, "silver.tracking_frames")),
        ("Tracking Coverage", available_pct),
    ]
    cols = st.columns(len(cards))
    for col, (label, value) in zip(cols, cards, strict=False):
        col.markdown(metric_card(label, value, ACCENT), unsafe_allow_html=True)


def _render_freshness_grid(health: pd.DataFrame) -> None:
    st.markdown("### Table Freshness")
    display = health.copy()
    display["status"] = display["freshness_status"].map(
        lambda s: FRESHNESS_LABELS.get(s, s.upper())
    )
    display = display[
        [
            "status",
            "table_name",
            "row_count",
            "file_size_bytes",
            "last_write_display",
            "operation",
            "snapshot_count",
            "added_records",
            "data_files",
        ]
    ].rename(
        columns={
            "table_name": "Table",
            "row_count": "Rows",
            "file_size_bytes": "Size",
            "last_write_display": "Last write",
            "operation": "Operation",
            "snapshot_count": "Snapshots",
            "added_records": "Latest added rows",
            "data_files": "Data files",
            "status": "Status",
        }
    )
    display["Rows"] = display["Rows"].map(_fmt_int)
    display["Size"] = display["Size"].map(_fmt_bytes)
    display["Latest added rows"] = display["Latest added rows"].map(_fmt_int)
    display["Data files"] = display["Data files"].map(_fmt_int)
    st.dataframe(display, use_container_width=True, hide_index=True)

    missing = health[health["error"].notna()]
    if not missing.empty:
        with st.expander("Missing or unreadable tables"):
            for row in missing.itertuples():
                st.warning(f"{row.table_name}: {row.error}")


def _render_tracking(attempts: SectionData, coverage: SectionData) -> None:
    st.markdown("### Tracking Health")
    left, right = st.columns(2)

    with left:
        st.markdown("**Fetch Reliability**")
        attempts_data = attempts["data"]
        if attempts["error"]:
            st.warning(f"Could not load silver.tracking_attempts: `{attempts['error']}`")
        elif attempts_data.empty:
            st.info("No tracking attempts recorded yet.")
        else:
            df = attempts_data.copy()
            total_expected = int(
                df.loc[
                    ~df["status"].isin(ATTEMPT_NOT_TRACKED_STATUSES),
                    "attempts",
                ].sum()
            )
            successes = int(df.loc[df["status"] == "success", "attempts"].sum())
            failures = int(df.loc[df["status"].isin(ATTEMPT_FAILURE_STATUSES), "attempts"].sum())
            rate = successes / total_expected if total_expected else 0
            st.metric("Expected-fetch success rate", f"{rate:.1%}")
            st.caption(
                f"{successes:,} successes, {failures:,} failures. "
                "404/not-tracked statuses are excluded from this reliability rate."
            )
            fig = px.bar(df, x="status", y="attempts", color="status")
            fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown("**Goal Coverage**")
        coverage_data = coverage["data"]
        if coverage["error"]:
            st.warning(f"Could not load gold.goal_tracking_status: `{coverage['error']}`")
        elif coverage_data.empty:
            st.info("No tracking coverage records available yet.")
        else:
            df = coverage_data.copy()
            total = int(df["goals"].sum())
            available = int(df.loc[df["tracking_status"] == "available", "goals"].sum())
            st.metric("Goals with stored frames", f"{available / total:.1%}" if total else "0.0%")
            st.caption(f"{available:,} of {total:,} tracked goal records are available.")
            fig = px.bar(df, x="tracking_status", y="goals", color="tracking_status")
            fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)


def _render_activity(history: SectionData) -> None:
    st.markdown("### Writes Per Day")
    if history["error"]:
        st.warning(f"Could not load snapshot history: `{history['error']}`")
        return
    df = history["data"]
    if df.empty:
        st.info(f"No snapshots found in the last {SNAPSHOT_HISTORY_DAYS} days.")
        return

    daily = (
        df.groupby(["date", "table_name"], as_index=False)
        .size()
        .rename(columns={"size": "snapshots"})
    )
    fig = px.bar(daily, x="date", y="snapshots", color="table_name")
    fig.update_layout(
        xaxis_title=None,
        yaxis_title="Snapshots",
        margin=dict(l=10, r=10, t=10, b=10),
        legend_title_text="Table",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Recent Snapshot Feed")
    feed = df.sort_values("timestamp", ascending=False).head(20).copy()
    feed["timestamp"] = feed["timestamp"].map(_fmt_time)
    st.dataframe(
        feed[
            [
                "timestamp",
                "table_name",
                "operation",
                "added_records",
                "total_records",
                "snapshot_id",
            ]
        ].rename(
            columns={
                "timestamp": "Timestamp",
                "table_name": "Table",
                "operation": "Operation",
                "added_records": "Added rows",
                "total_records": "Total rows",
                "snapshot_id": "Snapshot ID",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )


def main() -> None:
    st.markdown(
        "<h1 style='margin-bottom:0'>Pipeline Health</h1>"
        "<p style='color:#9aa5b1; margin-top:4px;'>"
        "Freshness, volume, and tracking coverage from existing Iceberg metadata "
        "and lakehouse tables."
        "</p>",
        unsafe_allow_html=True,
    )

    if st.button("Refresh", type="secondary"):
        st.cache_data.clear()
        st.rerun()

    health_result = _table_health()
    if health_result["error"]:
        st.error(f"Could not load table health: `{health_result['error']}`")
        return
    health = health_result["data"]
    seasons = _seasons()
    attempts = _tracking_attempts()
    coverage = _tracking_coverage()

    _render_cards(health, seasons, coverage)
    st.markdown("<hr style='margin: 20px 0; border-color: #243240;'>", unsafe_allow_html=True)
    _render_freshness_grid(health)
    st.markdown("<hr style='margin: 20px 0; border-color: #243240;'>", unsafe_allow_html=True)
    _render_tracking(attempts, coverage)
    st.markdown("<hr style='margin: 20px 0; border-color: #243240;'>", unsafe_allow_html=True)
    _render_activity(_snapshot_history())


main()
