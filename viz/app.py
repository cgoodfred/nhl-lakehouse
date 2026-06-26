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
import requests  # noqa: E402
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
MARKER_COLOR = "#ffce00"  # fallback when team color is unknown

# NHL team color map — primary + secondary hex from each team's brand guide.
# Used to color goal markers, accent metric cards, and swatch team rows in
# the tracking-panel legend. Unknown teams fall back to MARKER_COLOR.
NHL_TEAM_COLORS = {
    "ANA": {"primary": "#F47A38", "secondary": "#B9975B"},
    "ARI": {"primary": "#8C2633", "secondary": "#E2D6B5"},  # historic
    "BOS": {"primary": "#FFB81C", "secondary": "#000000"},
    "BUF": {"primary": "#003087", "secondary": "#FFB81C"},
    "CGY": {"primary": "#C8102E", "secondary": "#F1BE48"},
    "CAR": {"primary": "#CC0000", "secondary": "#000000"},
    "CHI": {"primary": "#CF0A2C", "secondary": "#000000"},
    "COL": {"primary": "#6F263D", "secondary": "#236192"},
    "CBJ": {"primary": "#002654", "secondary": "#CE1126"},
    "DAL": {"primary": "#006847", "secondary": "#8F8F8C"},
    "DET": {"primary": "#CE1126", "secondary": "#FFFFFF"},
    "EDM": {"primary": "#041E42", "secondary": "#FF4C00"},
    "FLA": {"primary": "#041E42", "secondary": "#C8102E"},
    "LAK": {"primary": "#111111", "secondary": "#A2AAAD"},
    "MIN": {"primary": "#154734", "secondary": "#A6192E"},
    "MTL": {"primary": "#AF1E2D", "secondary": "#192168"},
    "NSH": {"primary": "#FFB81C", "secondary": "#041E42"},
    "NJD": {"primary": "#CE1126", "secondary": "#000000"},
    "NYI": {"primary": "#00539B", "secondary": "#F47D30"},
    "NYR": {"primary": "#0038A8", "secondary": "#CE1126"},
    "OTT": {"primary": "#C52032", "secondary": "#C2912C"},
    "PHI": {"primary": "#F74902", "secondary": "#000000"},
    "PIT": {"primary": "#000000", "secondary": "#CFC493"},
    "SJS": {"primary": "#006D75", "secondary": "#000000"},
    "SEA": {"primary": "#001628", "secondary": "#99D9D9"},
    "STL": {"primary": "#002F87", "secondary": "#FCB514"},
    "TBL": {"primary": "#002868", "secondary": "#FFFFFF"},
    "TOR": {"primary": "#00205B", "secondary": "#FFFFFF"},
    "UTA": {"primary": "#71AFE5", "secondary": "#090909"},
    "VAN": {"primary": "#001F5B", "secondary": "#00843D"},
    "VGK": {"primary": "#B4975A", "secondary": "#333F42"},
    "WSH": {"primary": "#C8102E", "secondary": "#041E42"},
    "WPG": {"primary": "#041E42", "secondary": "#004C97"},
}


def _team_palette(team: str | None) -> dict[str, str]:
    """Return {primary, secondary} for a team, or sensible defaults."""
    if team and team in NHL_TEAM_COLORS:
        return NHL_TEAM_COLORS[team]
    return {"primary": MARKER_COLOR, "secondary": "#7a7a7a"}


def _text_on(hex_color: str) -> str:
    """Black or white for legibility against a colored background. Perceived
    luminance via the standard ITU-R BT.601 weights."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    return "#000000" if luminance > 0.55 else "#FFFFFF"

# NHL player+puck tracking from wsr.nhle.com uses inches with origin at one
# corner of the rink. Rink is 200ft x 85ft = 2400in x 1020in, so the center
# (which matches the PBP coord origin) sits at (1200, 510) inches.
PPT_CENTER_X_IN = 1200
PPT_CENTER_Y_IN = 510
PPT_INCHES_PER_FT = 12

# Browser-like headers — wsr.nhle.com is Cloudflare-protected and rejects
# requests without a recognized Referer + User-Agent.
PPT_HEADERS = {
    "Referer": "https://www.nhl.com/",
    "Origin": "https://www.nhl.com",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,*/*",
}


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


@st.cache_data(ttl=3600)
def _player_meta() -> dict[int, dict]:
    """Map playerId -> {name, position} for headers and legend rendering."""
    catalog = _catalog()
    try:
        arrow = catalog.load_table("silver.players").scan().to_arrow()
    except Exception:
        return {}
    df = arrow.to_pandas()
    return {
        int(pid): {
            "name": f"{first} {last}".strip(),
            "position": pos or "",
        }
        for pid, first, last, pos in zip(
            df.player_id, df.first_name, df.last_name, df.position_code, strict=False,
        )
    }


def _player_id_to_name() -> dict[int, str]:
    """Backwards-compat wrapper used by the tracking-frame helper."""
    return {pid: m["name"] for pid, m in _player_meta().items()}


# NHL CDN headshot URLs come pre-baked in gold.player_shots.player_headshot
# (populated from bronze rosterSpots[].headshot via silver.players). No
# per-render HEAD probe needed — just consume the column.


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
    # Goal lines. Because |x|=89 falls inside the corner-arc zone (|x| > X-R),
    # the rink boundary at that x is below RINK_Y. Compute the exact board
    # intersection so the line ends at the boards rather than overshooting
    # past them into space.
    goal_line_y = (RINK_Y - CORNER_R) + math.sqrt(
        CORNER_R**2 - (GOAL_LINE_X - (RINK_X - CORNER_R)) ** 2
    )
    for x in (-GOAL_LINE_X, GOAL_LINE_X):
        shapes.append(dict(type="line", x0=x, x1=x, y0=-goal_line_y, y1=goal_line_y,
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


def _metric_card(label: str, value: str, accent: str) -> str:
    """Render an HTML metric card with a left-border accent in team color."""
    return (
        f"<div style='background:#1a2129; border-left:4px solid {accent}; "
        f"padding:14px 16px; border-radius:6px; height:100%;'>"
        f"  <div style='color:#9aa5b1; font-size:0.75em; "
        f"text-transform:uppercase; letter-spacing:0.05em;'>{label}</div>"
        f"  <div style='color:#e8eef2; font-size:1.4em; font-weight:600; "
        f"margin-top:4px;'>{value}</div>"
        f"</div>"
    )


def _player_card(
    name: str, team: str, position: str, headshot_url: str | None,
) -> str:
    """Player-identity card with headshot (or initials fallback) + name + meta.

    headshot_url is the URL stored in gold.player_shots (originating from
    bronze rosterSpots[].headshot). Pass None to force the initials fallback
    — the <img> tag has no onerror handler since Streamlit strips inline JS,
    so we can't gracefully recover at render time."""
    palette = _team_palette(team)
    accent = palette["primary"]
    text_on_accent = _text_on(accent)
    initials = "".join(p[0] for p in name.split()[:2]).upper() or "?"

    if headshot_url:
        avatar_html = (
            f"<img src='{headshot_url}' alt='{name}' style='width:56px; "
            f"height:56px; border-radius:50%; object-fit:cover; "
            f"border:2px solid {accent};' />"
        )
    else:
        avatar_html = (
            f"<div style='width:56px; height:56px; border-radius:50%; "
            f"background:{accent}; color:{text_on_accent}; display:flex; "
            f"align-items:center; justify-content:center; font-size:1.3em; "
            f"font-weight:700; border:2px solid {accent};'>{initials}</div>"
        )

    position_meta = f" · {position}" if position else ""
    return (
        f"<div style='background:#1a2129; border-left:4px solid {accent}; "
        f"padding:10px 16px; border-radius:6px; height:100%; display:flex; "
        f"align-items:center; gap:14px;'>"
        f"  {avatar_html}"
        f"  <div>"
        f"    <div style='color:#9aa5b1; font-size:0.75em; "
        f"text-transform:uppercase; letter-spacing:0.05em;'>Player</div>"
        f"    <div style='color:#e8eef2; font-size:1.25em; "
        f"font-weight:600;'>{name}</div>"
        f"    <div style='color:#9aa5b1; font-size:0.85em;'>{team}{position_meta}</div>"
        f"  </div>"
        f"</div>"
    )


def _to_ft(x_in: float, y_in: float) -> tuple[float, float]:
    """Convert NHL tracking inches -> PBP feet (center-origin)."""
    return (
        (x_in - PPT_CENTER_X_IN) / PPT_INCHES_PER_FT,
        (y_in - PPT_CENTER_Y_IN) / PPT_INCHES_PER_FT,
    )


@st.cache_data(ttl=3600, show_spinner="Fetching tracking frames...")
def _fetch_tracking(url: str):
    resp = requests.get(url, headers=PPT_HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _frame_team_data(frame: dict, teams: list[str], id_to_name: dict):
    """Bin a single frame's onIce entries into per-team trace data + the puck."""
    by_team = {t: {"x": [], "y": [], "text": [], "hover": []} for t in teams}
    puck_x: list[float] = []
    puck_y: list[float] = []
    for key, entry in frame["onIce"].items():
        x_ft, y_ft = _to_ft(entry["x"], entry["y"])
        if str(key) == "1":
            puck_x.append(x_ft)
            puck_y.append(y_ft)
            continue
        team = entry.get("teamAbbrev") or "?"
        if team not in by_team:
            continue
        sweater = str(entry.get("sweaterNumber", ""))
        name = id_to_name.get(int(entry.get("playerId") or 0), "")
        by_team[team]["x"].append(x_ft)
        by_team[team]["y"].append(y_ft)
        by_team[team]["text"].append(sweater)
        by_team[team]["hover"].append(
            f"{name} (#{sweater}, {team})" if name else f"#{sweater} ({team})"
        )
    return by_team, puck_x, puck_y


def _tracking_animation(frames: list, id_to_name: dict) -> go.Figure:
    """Animated rink view of the whole goal sequence.

    Plotly's native frames + updatemenus drive play/pause + the slider.
    The rink itself is one static trace (added by _rink_figure); the three
    dynamic traces (home team, away team, puck) are updated per frame via
    indexed targeting so the rink never re-renders."""
    fig = _rink_figure()

    teams_seen = sorted({
        e.get("teamAbbrev") for f in frames
        for k, e in f["onIce"].items() if str(k) != "1"
    })
    if len(teams_seen) < 2:
        teams_seen = [*teams_seen, "?", "?"][:2]
    teams = teams_seen[:2]
    # Real team colors. If both teams' primaries are too similar (rare but
    # possible: e.g. CGY vs DET both red), fall back to the secondary for
    # the second team to keep them visually distinct on the rink.
    primary_0 = _team_palette(teams[0])["primary"]
    primary_1 = _team_palette(teams[1])["primary"]
    if primary_0.lower() == primary_1.lower():
        primary_1 = _team_palette(teams[1])["secondary"]
    palette = {teams[0]: primary_0, teams[1]: primary_1}

    # Initial rendered state = LAST frame (the goal moment). The Plotly
    # slider's `active` is also set to len(frames)-1 below, and the panel
    # caption says it defaults to the goal moment — initializing from
    # frames[0] would leave the rendered chart at the start of the replay
    # while the controls advertised the end. Pressing ▶ from the last
    # frame loops back to 0 automatically since fromcurrent + the end is
    # treated as a wrap by Plotly.
    init_team, init_puck_x, init_puck_y = _frame_team_data(frames[-1], teams, id_to_name)
    for team in teams:
        d = init_team[team]
        fig.add_trace(go.Scatter(
            x=d["x"], y=d["y"], mode="markers+text",
            marker=dict(size=22, color=palette[team],
                        line=dict(color="white", width=1.5)),
            text=d["text"], textposition="middle center",
            textfont=dict(color="white", size=11),
            name=team,
            hovertext=d["hover"], hovertemplate="%{hovertext}<extra></extra>",
        ))
    fig.add_trace(go.Scatter(
        x=init_puck_x, y=init_puck_y, mode="markers",
        marker=dict(size=12, color="black", symbol="circle",
                    line=dict(color="white", width=1.5)),
        name="puck", hovertemplate="puck<extra></extra>",
    ))

    # Indices of the 3 dynamic traces (rink is at index 0)
    dynamic_idx = [len(fig.data) - 3, len(fig.data) - 2, len(fig.data) - 1]

    plotly_frames = []
    for i, frame in enumerate(frames):
        frame_team, frame_puck_x, frame_puck_y = _frame_team_data(frame, teams, id_to_name)
        traces = []
        for team in teams:
            d = frame_team[team]
            traces.append(go.Scatter(
                x=d["x"], y=d["y"], text=d["text"], hovertext=d["hover"],
            ))
        traces.append(go.Scatter(x=frame_puck_x, y=frame_puck_y))
        plotly_frames.append(go.Frame(name=str(i), data=traces, traces=dynamic_idx))
    fig.frames = plotly_frames

    fig.update_layout(
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    bgcolor="rgba(0,0,0,0)"),
        updatemenus=[{
            "type": "buttons",
            "direction": "left",
            "x": 0.0, "xanchor": "left",
            "y": -0.08, "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "showactive": False,
            "buttons": [
                {
                    "label": "▶ Play",
                    "method": "animate",
                    "args": [None, {
                        "frame": {"duration": 70, "redraw": True},
                        "fromcurrent": True,
                        "transition": {"duration": 0},
                    }],
                },
                {
                    "label": "⏸ Pause",
                    "method": "animate",
                    "args": [[None], {
                        "frame": {"duration": 0, "redraw": False},
                        "mode": "immediate",
                        "transition": {"duration": 0},
                    }],
                },
            ],
        }],
        sliders=[{
            "active": len(frames) - 1,
            "x": 0.1, "xanchor": "left",
            "y": -0.05, "yanchor": "top",
            "len": 0.88,
            "currentvalue": {"prefix": "frame ", "visible": True,
                             "xanchor": "right"},
            "steps": [
                {
                    "args": [[str(i)], {
                        "frame": {"duration": 0, "redraw": True},
                        "mode": "immediate",
                        "transition": {"duration": 0},
                    }],
                    "label": str(i),
                    "method": "animate",
                }
                for i in range(len(frames))
            ],
        }],
    )
    return fig


def _render_legend_html(rows: list[dict]) -> str:
    """HTML table for the tracking-panel legend with a team-color swatch on
    each row. st.dataframe doesn't support per-cell color styling cleanly so
    we just render the table as markdown HTML."""
    head = (
        "<table style='width:100%; border-collapse:collapse; font-size:0.9em;'>"
        "<thead><tr>"
        "<th style='text-align:left; padding:6px 4px; color:#9aa5b1; "
        "border-bottom:1px solid #243240;'>#</th>"
        "<th style='text-align:left; padding:6px 4px; color:#9aa5b1; "
        "border-bottom:1px solid #243240;'>Player</th>"
        "<th style='text-align:left; padding:6px 4px; color:#9aa5b1; "
        "border-bottom:1px solid #243240;'>Team</th>"
        "</tr></thead><tbody>"
    )
    body = []
    for r in rows:
        team_color = _team_palette(r["Team"])["primary"]
        body.append(
            "<tr>"
            f"<td style='padding:5px 4px; color:#e8eef2; font-weight:600;'>{r['#']}</td>"
            f"<td style='padding:5px 4px; color:#e8eef2;'>{r['Player']}</td>"
            f"<td style='padding:5px 4px; color:#e8eef2;'>"
            f"<span style='display:inline-block; width:10px; height:10px; "
            f"background:{team_color}; border-radius:50%; margin-right:6px; "
            f"vertical-align:middle;'></span>{r['Team']}</td>"
            "</tr>"
        )
    return head + "".join(body) + "</tbody></table>"


def _legend_rows(frames: list, id_to_name: dict) -> list[dict]:
    """Unique players seen across the sequence, with sweater + team + name."""
    seen: dict[int, tuple[str, int | str]] = {}
    for frame in frames:
        for key, entry in frame["onIce"].items():
            if str(key) == "1":
                continue
            pid = entry.get("playerId")
            if pid and pid not in seen:
                seen[pid] = (
                    entry.get("teamAbbrev") or "?",
                    entry.get("sweaterNumber", ""),
                )
    rows = [
        {
            "#": sweater,
            "Player": id_to_name.get(int(pid), f"player {pid}"),
            "Team": team,
        }
        for pid, (team, sweater) in seen.items()
    ]
    # Stable sort: team then sweater
    rows.sort(key=lambda r: (r["Team"], int(r["#"]) if str(r["#"]).isdigit() else 999))
    return rows


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

    # Game-type filter maps the UI choice to a SQL predicate on shots.game_type.
    # NHL gameType encoding: 1 preseason, 2 regular season, 3 playoffs.
    GAME_TYPE_OPTIONS = {
        "Regular season": "game_type = 2",
        "Preseason":      "game_type = 1",
        "Playoffs":       "game_type = 3",
        "All NHL":        "game_type IN (1, 2, 3)",
    }

    col_season, col_type, col_team, col_player = st.columns(4)
    season = col_season.selectbox("Season", seasons, format_func=_fmt_season)
    game_type_label = col_type.selectbox(
        "Game type", list(GAME_TYPE_OPTIONS.keys()), index=0,
    )
    type_pred = GAME_TYPE_OPTIONS[game_type_label]

    teams = [r[0] for r in con.execute(
        f"SELECT DISTINCT team_abbrev FROM shots "
        f"WHERE season = ? AND {type_pred} ORDER BY team_abbrev",
        [season],
    ).fetchall()]
    if not teams:
        st.warning(f"No goals for {_fmt_season(season)} {game_type_label}.")
        return
    team = col_team.selectbox(
        "Team", teams,
        index=teams.index("LAK") if "LAK" in teams else 0,
    )

    players_rows = con.execute(
        f"""SELECT player_id, player_name, COUNT(*) AS goals
            FROM shots
            WHERE season = ? AND team_abbrev = ? AND {type_pred}
            GROUP BY player_id, player_name ORDER BY goals DESC, player_name""",
        [season, team],
    ).fetchall()
    if not players_rows:
        st.warning(
            f"No goals for {team} in {_fmt_season(season)} {game_type_label}."
        )
        return
    player_labels = [f"{name} ({goals})" for _id, name, goals in players_rows]
    player_label = col_player.selectbox("Player (goal count)", player_labels)
    player_id = players_rows[player_labels.index(player_label)][0]
    player_name = players_rows[player_labels.index(player_label)][1]

    shots = con.execute(
        f"""SELECT x_coord, y_coord, shot_type, period_number, period_type,
                   time_in_period, strength_state, is_empty_net,
                   game_date, home_score, away_score, ppt_replay_url,
                   player_headshot
            FROM shots
            WHERE season = ? AND team_abbrev = ? AND player_id = ? AND {type_pred}
            ORDER BY game_date, period_number, time_in_period""",
        [season, team, player_id],
    ).df()

    # Summary metric row — custom HTML cards accented in the selected team's
    # primary color so the page picks up a team identity when a team is picked.
    palette = _team_palette(team)
    accent = palette["primary"]
    meta = _player_meta().get(int(player_id), {})
    position = meta.get("position", "")
    # Headshot URL is in every row of `shots` for the selected player — they
    # only differ if the player was traded mid-season; just take row 0.
    headshot_url = (
        shots["player_headshot"].iloc[0] if len(shots) and "player_headshot" in shots
        else None
    )

    st.markdown("<hr style='margin: 8px 0 16px 0; border-color: #243240;'>",
                unsafe_allow_html=True)
    m1, m2, m3, m4 = st.columns(4)
    m1.markdown(
        _player_card(player_name, team, position, headshot_url),
        unsafe_allow_html=True,
    )
    m2.markdown(_metric_card("Team", team, accent), unsafe_allow_html=True)
    m3.markdown(_metric_card("Season", _fmt_season(season), accent),
                unsafe_allow_html=True)
    m4.markdown(_metric_card("Goals", str(len(shots)), accent),
                unsafe_allow_html=True)

    # Rink + table
    left, right = st.columns([2, 1])
    with left:
        fig = _rink_figure()
        fig.add_trace(go.Scatter(
            x=shots["x_coord"], y=shots["y_coord"],
            mode="markers",
            marker=dict(
                size=15, color=accent,
                line=dict(color=palette["secondary"], width=1.5),
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
        st.caption("Click a row to open the tracking view below.")
        table = shots[["game_date", "period_number", "time_in_period",
                       "shot_type", "strength_state"]].rename(columns={
            "game_date": "Date",
            "period_number": "P",
            "time_in_period": "Time",
            "shot_type": "Shot",
            "strength_state": "Strength",
        })
        table_event = st.dataframe(
            table,
            use_container_width=True,
            hide_index=True,
            height=470,
            on_select="rerun",
            selection_mode="single-row",
        )

    # Per-goal tracking panel — opens when a row in the table is selected.
    if table_event.selection.rows:
        idx = table_event.selection.rows[0]
        sel = shots.iloc[idx]
        st.markdown("<hr style='margin: 24px 0 8px 0; border-color: #243240;'>",
                    unsafe_allow_html=True)
        header = (
            f"### Tracking — {sel.game_date}, "
            f"P{sel.period_number} {sel.time_in_period} "
            f"({sel.shot_type or 'unknown'}, {sel.strength_state})"
        )
        st.markdown(header)
        if not sel.ppt_replay_url:
            st.info("No tracking data attached to this goal.")
        else:
            try:
                frames = _fetch_tracking(sel.ppt_replay_url)
            except Exception as exc:
                st.warning(
                    f"Could not fetch tracking from {sel.ppt_replay_url}\n\n"
                    f"`{exc.__class__.__name__}: {exc}`"
                )
            else:
                id_to_name = _player_id_to_name()
                track_col, legend_col = st.columns([3, 1])
                with track_col:
                    st.caption(
                        "▶ Play animates the goal sequence; slider scrubs "
                        "manually. Defaults to the goal moment (last frame)."
                    )
                    st.plotly_chart(
                        _tracking_animation(frames, id_to_name),
                        use_container_width=True,
                    )
                with legend_col:
                    st.markdown("**On-ice players**")
                    legend = _legend_rows(frames, id_to_name)
                    st.markdown(_render_legend_html(legend),
                                unsafe_allow_html=True)

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
