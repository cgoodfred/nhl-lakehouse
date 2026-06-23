"""NHL goal map — Streamlit app reading gold.player_shots from Iceberg.

Filter cascade: season → team → player. Renders each goal as a marker
on an NHL rink (Plotly shapes for the rink, scatter trace for shots).

Connects to the Lakekeeper REST catalog and SeaweedFS S3-compatible
storage via PyIceberg. In-cluster mode reads service DNS directly;
local-dev mode (no KUBERNETES_SERVICE_HOST env var) hijacks DNS so
Lakekeeper's catalog overrides resolve to the laptop's port-forwards.

Required env vars (Deployment provides them; local dev exports manually):
    LAKEKEEPER_URI                http://lakekeeper.lakehouse.svc.cluster.local:8181/catalog
    LAKEKEEPER_CLIENT_ID          lakekeeper-spark
    LAKEKEEPER_CLIENT_SECRET      <secret>
    LAKEKEEPER_OAUTH2_SERVER_URI  https://keycloak.cluster.cgood.dev/realms/Lakehouse/...
    LAKEKEEPER_SCOPE              lakekeeper
    LAKEKEEPER_WAREHOUSE          nhl
    S3_ENDPOINT                   http://seaweedfs-s3.lakehouse.svc.cluster.local:8333
    S3_ACCESS_KEY                 <secret>
    S3_SECRET_KEY                 <secret>
"""

import os
import socket

# DNS hijack for local development only — in-cluster, the service DNS
# already resolves correctly. Must run before requests/urllib3 are imported.
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

# NHL rink half-dimensions in feet (the official NHL coordinate system).
RINK_X = 100.0   # length / 2
RINK_Y = 42.5    # width / 2
GOAL_LINE_X = 89.0
BLUE_LINE_X = 25.0
CORNER_RADIUS = 28.0


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
    """Load gold.player_shots as an Arrow table. Arrow tables are
    serializable so st.cache_data can persist them across reruns; the
    DuckDB connection that wraps them is created per-rerun below."""
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
    """Fresh DuckDB connection with the cached Arrow table registered.
    Cheap (in-memory zero-copy register); a new connection per script
    rerun is fine because the heavy lift (the catalog scan) is cached."""
    con = duckdb.connect()
    con.register("shots", _shots_arrow())
    return con


def _rink_figure() -> go.Figure:
    """Plotly figure showing an NHL rink (top-down), no shot data yet."""
    fig = go.Figure()
    fig.update_layout(
        xaxis=dict(range=[-RINK_X - 2, RINK_X + 2], showgrid=False, zeroline=False, visible=False),
        yaxis=dict(
            range=[-RINK_Y - 2, RINK_Y + 2],
            scaleanchor="x",
            scaleratio=1,
            showgrid=False,
            zeroline=False,
            visible=False,
        ),
        plot_bgcolor="#f3f8ff",
        margin=dict(l=10, r=10, t=10, b=10),
        height=520,
        shapes=_rink_shapes(),
        showlegend=False,
    )
    return fig


def _rink_shapes() -> list:
    """Static Plotly shapes drawing the rink markings."""
    shapes = []
    # Boards — rounded rect approximated as a single rect for simplicity.
    shapes.append(
        dict(type="rect", x0=-RINK_X, x1=RINK_X, y0=-RINK_Y, y1=RINK_Y,
             line=dict(color="black", width=2), fillcolor="rgba(0,0,0,0)")
    )
    # Center red line
    shapes.append(
        dict(type="line", x0=0, x1=0, y0=-RINK_Y, y1=RINK_Y,
             line=dict(color="red", width=2))
    )
    # Blue lines
    for x in (-BLUE_LINE_X, BLUE_LINE_X):
        shapes.append(
            dict(type="line", x0=x, x1=x, y0=-RINK_Y, y1=RINK_Y,
                 line=dict(color="blue", width=2))
        )
    # Goal lines
    for x in (-GOAL_LINE_X, GOAL_LINE_X):
        shapes.append(
            dict(type="line", x0=x, x1=x, y0=-RINK_Y, y1=RINK_Y,
                 line=dict(color="red", width=1))
        )
    # Center faceoff circle (15ft radius)
    shapes.append(
        dict(type="circle", x0=-15, x1=15, y0=-15, y1=15,
             line=dict(color="blue", width=1.5))
    )
    # Center faceoff dot
    shapes.append(
        dict(type="circle", x0=-0.5, x1=0.5, y0=-0.5, y1=0.5,
             line=dict(color="blue"), fillcolor="blue")
    )
    # End faceoff dots + circles (15ft radius), and goal creases.
    for end_sign in (-1, 1):
        for y in (-22, 22):
            cx = end_sign * 69
            cy = y
            shapes.append(
                dict(type="circle", x0=cx - 15, x1=cx + 15, y0=cy - 15, y1=cy + 15,
                     line=dict(color="red", width=1.5))
            )
            shapes.append(
                dict(type="circle", x0=cx - 1, x1=cx + 1, y0=cy - 1, y1=cy + 1,
                     line=dict(color="red"), fillcolor="red")
            )
        # Goal crease - semicircle on the ice-side of the goal line.
        # Simple half-rectangle approximation (6ft by 4.5ft).
        gx = end_sign * GOAL_LINE_X
        shapes.append(
            dict(type="rect", x0=gx - end_sign * 4.5, x1=gx, y0=-4, y1=4,
                 line=dict(color="red", width=1), fillcolor="rgba(0,150,255,0.15)")
        )
    # Neutral-zone faceoff dots
    for x in (-20, 20):
        for y in (-22, 22):
            shapes.append(
                dict(type="circle", x0=x - 0.5, x1=x + 0.5, y0=y - 0.5, y1=y + 0.5,
                     line=dict(color="red"), fillcolor="red")
            )
    return shapes


def main():
    st.set_page_config(page_title="NHL Goal Map", layout="wide", page_icon="🏒")
    st.title("NHL Goal Map")
    st.caption(
        "Every goal scored by the selected player this season, plotted at the "
        "rink coordinates where the shot was taken from."
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
    team = col_team.selectbox("Team", teams, index=teams.index("LAK") if "LAK" in teams else 0)

    players_rows = con.execute(
        """SELECT player_id, player_name, COUNT(*) AS goals
           FROM shots WHERE season = ? AND team_abbrev = ?
           GROUP BY player_id, player_name ORDER BY goals DESC, player_name""",
        [season, team],
    ).fetchall()
    if not players_rows:
        st.warning(f"No goals for {team} in {_fmt_season(season)}.")
        return
    player_label = col_player.selectbox(
        "Player",
        [f"{name} ({goals})" for _id, name, goals in players_rows],
    )
    player_id = players_rows[[f"{n} ({g})" for _i, n, g in players_rows].index(player_label)][0]

    shots = con.execute(
        """SELECT x_coord, y_coord, shot_type, period_number, time_in_period,
                  game_date, home_score, away_score
           FROM shots WHERE season = ? AND team_abbrev = ? AND player_id = ?
           ORDER BY game_date, period_number, time_in_period""",
        [season, team, player_id],
    ).df()

    left, right = st.columns([2, 1])

    with left:
        fig = _rink_figure()
        fig.add_trace(go.Scatter(
            x=shots["x_coord"], y=shots["y_coord"],
            mode="markers",
            marker=dict(
                size=14, color="#d62728", line=dict(color="black", width=1.2),
                symbol="circle",
            ),
            text=[
                (
                    f"{r.game_date} - P{r.period_number} {r.time_in_period} - "
                    f"{r.shot_type or 'unknown'}"
                )
                for r in shots.itertuples()
            ],
            hovertemplate="%{text}<extra></extra>",
        ))
        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.metric("Goals", len(shots))
        st.dataframe(
            shots[["game_date", "period_number", "time_in_period", "shot_type"]].rename(
                columns={
                    "game_date": "Date",
                    "period_number": "P",
                    "time_in_period": "Time",
                    "shot_type": "Shot",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )


def _fmt_season(season: int) -> str:
    s = str(season)
    return f"{s[:4]}-{s[4:]}"


if __name__ == "__main__":
    main()
