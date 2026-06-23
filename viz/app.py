"""NHL goal map — Streamlit app reading gold.player_shots from Iceberg.

Filter cascade: season -> team -> player. Renders each goal as a marker
on a top-down NHL rink (boards drawn with 28ft rounded corners,
faceoff dots/circles, blue lines, goal lines, and creases). Three
breakdown pie charts at the bottom: shot type, period, strength state.

Connects to the Lakekeeper REST catalog and SeaweedFS S3-compatible
storage via PyIceberg. In-cluster mode reads service DNS directly;
local-dev mode (no KUBERNETES_SERVICE_HOST env var) hijacks DNS so
Lakekeeper's catalog overrides resolve to the laptop's port-forwards.
"""

import math
import os
import socket

_IN_CLUSTER = bool(os.environ.get("KUBERNETES_SERVICE_HOST"))
if not _IN_CLUSTER:
    _real_getaddrinfo = socket.getaddrinfo

    def _patched_getaddrinfo(host, *args, **kwargs):
        if host in (
            "lakekeeper.lakehouse.svc.cluster.local",
            "seaweedfs-s3.lakehouse.svc.cluster.local",
        ):
            return _real_getaddrinfo("127.0.0.1", *args, **kwargs)
        return _real_getaddrinfo(host, *args, **kwargs)

    socket.getaddrinfo = _patched_getaddrinfo

import duckdb  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402
from pyiceberg.catalog.rest import RestCatalog  # noqa: E402

# NHL rink dimensions in feet, official coord system: x in [-100, 100],
# y in [-42.5, 42.5]. Corner radius is 28ft.
RINK_X = 100.0
RINK_Y = 42.5
CORNER_R = 28.0
GOAL_LINE_X = 89.0
BLUE_LINE_X = 25.0

# Plotly defaults consistent with the dark theme.
ICE_BG = "#0e1d2b"
LINE_RED = "#d62728"
LINE_BLUE = "#1f77b4"
MARKER_COLOR = "#ffce00"


@st.cache_resource
def _catalog() -> RestCatalog:
    return RestCatalog(
        "nhl",
        **{
            "uri": os.environ["LAKEKEEPER_URI"],
            "warehouse": os.environ.get("LAKEKEEPER_WAREHOUSE", "nhl"),
            "credential": (
                f"{os.environ['LAKEKEEPER_CLIENT_ID']}:{os.environ['LAKEKEEPER_CLIENT_SECRET']}"
            ),
            "scope": os.environ.get("LAKEKEEPER_SCOPE", "lakekeeper"),
            "oauth2-server-uri": os.environ["LAKEKEEPER_OAUTH2_SERVER_URI"],
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
            "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
            "s3.path-style-access": "true",
        },
    )


@st.cache_data(ttl=300)
def _shots_arrow():
    catalog = _catalog()
    try:
        return catalog.load_table("gold.player_shots").scan().to_arrow()
    except Exception as exc:
        st.error(
            "Could not load gold.player_shots. Has the gold Spark job run yet?\n\n"
            f"`{exc}`"
        )
        st.stop()


def _shots_connection():
    con = duckdb.connect()
    con.register("shots", _shots_arrow())
    return con


def _rink_boundary_points(n_per_corner: int = 24):
    """Polygon approximation of the NHL rink boundary with rounded corners."""
    pts = []
    corners = [
        (RINK_X - CORNER_R, -(RINK_Y - CORNER_R), -math.pi / 2),  # bottom-right
        (RINK_X - CORNER_R,   RINK_Y - CORNER_R,   0.0),           # top-right
        (-(RINK_X - CORNER_R), RINK_Y - CORNER_R,  math.pi / 2),   # top-left
        (-(RINK_X - CORNER_R), -(RINK_Y - CORNER_R), math.pi),     # bottom-left
    ]
    for cx, cy, start_angle in corners:
        for i in range(n_per_corner + 1):
            theta = start_angle + (math.pi / 2) * (i / n_per_corner)
            pts.append((cx + CORNER_R * math.cos(theta), cy + CORNER_R * math.sin(theta)))
    pts.append(pts[0])
    return pts


def _rink_shapes() -> list:
    """Static interior rink markings (lines, dots, circles, creases)."""
    shapes = []

    # Center red line
    shapes.append(dict(type="line", x0=0, x1=0, y0=-RINK_Y, y1=RINK_Y,
                       line=dict(color=LINE_RED, width=2)))
    # Blue lines
    for x in (-BLUE_LINE_X, BLUE_LINE_X):
        shapes.append(dict(type="line", x0=x, x1=x, y0=-RINK_Y, y1=RINK_Y,
                           line=dict(color=LINE_BLUE, width=2)))
    # Goal lines (thinner red, just inside the boards)
    for x in (-GOAL_LINE_X, GOAL_LINE_X):
        shapes.append(dict(type="line", x0=x, x1=x, y0=-RINK_Y + 4, y1=RINK_Y - 4,
                           line=dict(color=LINE_RED, width=1)))
    # Center faceoff circle + dot
    shapes.append(dict(type="circle", x0=-15, x1=15, y0=-15, y1=15,
                       line=dict(color=LINE_BLUE, width=1.5)))
    shapes.append(dict(type="circle", x0=-0.7, x1=0.7, y0=-0.7, y1=0.7,
                       line=dict(color=LINE_BLUE), fillcolor=LINE_BLUE))
    # End faceoff circles + dots
    for end_sign in (-1, 1):
        for y in (-22, 22):
            cx = end_sign * 69
            cy = y
            shapes.append(dict(type="circle", x0=cx - 15, x1=cx + 15,
                               y0=cy - 15, y1=cy + 15,
                               line=dict(color=LINE_RED, width=1.5)))
            shapes.append(dict(type="circle", x0=cx - 1, x1=cx + 1,
                               y0=cy - 1, y1=cy + 1,
                               line=dict(color=LINE_RED), fillcolor=LINE_RED))
        # Goal crease (rough approximation as half-rect)
        gx = end_sign * GOAL_LINE_X
        shapes.append(dict(type="rect", x0=gx - end_sign * 4.5, x1=gx, y0=-4, y1=4,
                           line=dict(color=LINE_RED, width=1),
                           fillcolor="rgba(31, 119, 180, 0.18)"))
    # Neutral-zone faceoff dots
    for x in (-20, 20):
        for y in (-22, 22):
            shapes.append(dict(type="circle", x0=x - 1, x1=x + 1,
                               y0=y - 1, y1=y + 1,
                               line=dict(color=LINE_RED), fillcolor=LINE_RED))
    return shapes


def _rink_figure() -> go.Figure:
    pts = _rink_boundary_points()
    boundary_x = [p[0] for p in pts]
    boundary_y = [p[1] for p in pts]

    fig = go.Figure()
    # Boards — drawn as a closed polygon trace so we can render the 28ft rounded
    # corners that real NHL rinks have. Plotly shapes don't have a rounded-rect
    # primitive, so we approximate by sampling along each corner arc.
    fig.add_trace(go.Scatter(
        x=boundary_x, y=boundary_y, mode="lines",
        line=dict(color="white", width=2.5),
        fill="toself", fillcolor=ICE_BG,
        hoverinfo="skip", showlegend=False,
    ))
    fig.update_layout(
        xaxis=dict(range=[-RINK_X - 4, RINK_X + 4], showgrid=False,
                   zeroline=False, visible=False),
        yaxis=dict(range=[-RINK_Y - 4, RINK_Y + 4], scaleanchor="x",
                   scaleratio=1, showgrid=False, zeroline=False, visible=False),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=10, b=10),
        height=520,
        shapes=_rink_shapes(),
        showlegend=False,
    )
    return fig


def _pie(values: list, labels: list, title: str) -> go.Figure:
    fig = go.Figure(go.Pie(
        values=values, labels=labels,
        hole=0.45,
        textposition="inside",
        textinfo="label+percent",
        marker=dict(line=dict(color="#0e1217", width=2)),
        sort=True,
    ))
    fig.update_layout(
        title=dict(text=title, x=0.5, xanchor="center", font=dict(size=14)),
        showlegend=False,
        margin=dict(l=10, r=10, t=40, b=10),
        height=300,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e8eef2"),
    )
    return fig


def _fmt_season(season: int) -> str:
    s = str(season)
    return f"{s[:4]}-{s[4:]}"


def main():
    st.set_page_config(page_title="NHL Goal Map", layout="wide", page_icon="🏒")

    # Header
    st.markdown(
        "<h1 style='margin-bottom:0'>NHL Goal Map</h1>"
        "<p style='color:#9aa5b1; margin-top:4px;'>"
        "Every goal scored by the selected player this season, plotted at the "
        "rink coordinates the shot was taken from. Data flows bronze (NHL API) → "
        "silver (typed Iceberg facts) → gold (denormalized for this view)."
        "</p>",
        unsafe_allow_html=True,
    )

    con = _shots_connection()

    seasons = [
        r[0] for r in
        con.execute("SELECT DISTINCT season FROM shots ORDER BY season DESC").fetchall()
    ]
    if not seasons:
        st.warning("No data in gold.player_shots yet.")
        return

    col_season, col_team, col_player = st.columns(3)
    season = col_season.selectbox("Season", seasons, format_func=_fmt_season)

    teams = [r[0] for r in con.execute(
        "SELECT DISTINCT team_abbrev FROM shots WHERE season = ? ORDER BY team_abbrev",
        [season],
    ).fetchall()]
    team = col_team.selectbox(
        "Team", teams,
        index=teams.index("LAK") if "LAK" in teams else 0,
    )

    players_rows = con.execute(
        """SELECT player_id, player_name, COUNT(*) AS goals
           FROM shots WHERE season = ? AND team_abbrev = ?
           GROUP BY player_id, player_name ORDER BY goals DESC, player_name""",
        [season, team],
    ).fetchall()
    if not players_rows:
        st.warning(f"No goals for {team} in {_fmt_season(season)}.")
        return
    player_labels = [f"{name} ({goals})" for _id, name, goals in players_rows]
    player_label = col_player.selectbox("Player (goal count)", player_labels)
    player_id = players_rows[player_labels.index(player_label)][0]
    player_name = players_rows[player_labels.index(player_label)][1]

    shots = con.execute(
        """SELECT x_coord, y_coord, shot_type, period_number, period_type,
                  time_in_period, strength_state, is_empty_net,
                  game_date, home_score, away_score
           FROM shots WHERE season = ? AND team_abbrev = ? AND player_id = ?
           ORDER BY game_date, period_number, time_in_period""",
        [season, team, player_id],
    ).df()

    # Summary metric row
    st.markdown("<hr style='margin: 8px 0 16px 0; border-color: #243240;'>",
                unsafe_allow_html=True)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Player", player_name)
    m2.metric("Team", team)
    m3.metric("Season", _fmt_season(season))
    m4.metric("Goals", len(shots))

    # Rink + table
    left, right = st.columns([2, 1])
    with left:
        fig = _rink_figure()
        fig.add_trace(go.Scatter(
            x=shots["x_coord"], y=shots["y_coord"],
            mode="markers",
            marker=dict(
                size=15, color=MARKER_COLOR,
                line=dict(color="#0e1217", width=1.5),
                symbol="circle",
            ),
            text=[
                (
                    f"{r.game_date} - P{r.period_number} {r.time_in_period}<br>"
                    f"{r.shot_type or 'unknown'} - {r.strength_state}"
                    f"{' (empty net)' if r.is_empty_net else ''}"
                )
                for r in shots.itertuples()
            ],
            hovertemplate="%{text}<extra></extra>",
            showlegend=False,
        ))
        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown("**Goals this season**")
        table = shots[["game_date", "period_number", "time_in_period",
                       "shot_type", "strength_state"]].rename(columns={
            "game_date": "Date",
            "period_number": "P",
            "time_in_period": "Time",
            "shot_type": "Shot",
            "strength_state": "Strength",
        })
        st.dataframe(table, use_container_width=True, hide_index=True, height=470)

    # Breakdowns
    st.markdown("<hr style='margin: 24px 0 8px 0; border-color: #243240;'>",
                unsafe_allow_html=True)
    st.markdown("### Breakdowns")
    p1, p2, p3 = st.columns(3)

    # Shot type
    shot_counts = (shots["shot_type"].fillna("unknown")
                   .value_counts().sort_values(ascending=False))
    with p1:
        st.plotly_chart(
            _pie(shot_counts.values.tolist(), shot_counts.index.tolist(), "Shot type"),
            use_container_width=True,
        )

    # Period (combine number + type so OT/SO show separately)
    def _period_label(r):
        if r.period_type and r.period_type != "REG":
            return r.period_type
        return f"P{r.period_number}"
    period_series = shots.apply(_period_label, axis=1)
    period_counts = period_series.value_counts()
    # Stable ordering: P1, P2, P3, OT, SO, anything else after.
    order = ["P1", "P2", "P3", "OT", "SO"]
    period_counts = period_counts.reindex(
        [p for p in order if p in period_counts.index]
        + [p for p in period_counts.index if p not in order]
    )
    with p2:
        st.plotly_chart(
            _pie(period_counts.values.tolist(), period_counts.index.tolist(), "Period"),
            use_container_width=True,
        )

    # Strength state — break out empty-net goals as their own slice
    def _strength_label(r):
        if r.is_empty_net:
            return "EN"
        return r.strength_state or "unknown"
    strength_series = shots.apply(_strength_label, axis=1)
    strength_counts = strength_series.value_counts()
    with p3:
        st.plotly_chart(
            _pie(strength_counts.values.tolist(),
                 strength_counts.index.tolist(),
                 "Strength state"),
            use_container_width=True,
        )


if __name__ == "__main__":
    main()
