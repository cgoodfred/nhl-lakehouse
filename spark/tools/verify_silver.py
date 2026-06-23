#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyiceberg[s3fs,pyarrow]",
#     "duckdb",
#     "pandas",
# ]
# ///
"""Full verification of every silver table: row counts, key uniqueness,
null checks, value-set validation, and cross-table referential integrity.

Auto-resolves credentials via kubectl. Requires port-forwards:
    kubectl port-forward -n lakehouse svc/lakekeeper 8181:8181 &
    kubectl port-forward -n lakehouse svc/seaweedfs-s3 8333:8333 &

Just run it:
    ./spark/tools/verify_silver.py
"""

import base64
import json
import socket
import subprocess
import time

# DNS hijack — Lakekeeper's catalog overrides advertise in-cluster URLs that
# don't resolve from a laptop. Must run before requests/urllib3 are imported.
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
from pyiceberg.catalog.rest import RestCatalog  # noqa: E402

KEYCLOAK_TOKEN_URI = (
    "https://keycloak.cluster.cgood.dev/realms/Lakehouse/protocol/openid-connect/token"
)

TABLES = ["games", "plays", "players", "game_rosters", "teams"]


def _kube_secret(namespace: str, name: str, key: str) -> str:
    out = subprocess.check_output(
        ["kubectl", "get", "secret", "-n", namespace, name, "-o", f"jsonpath={{.data.{key}}}"]
    )
    return base64.b64decode(out).decode()


def _build_catalog() -> RestCatalog:
    lk_secret = _kube_secret("lakehouse", "lakekeeper-client-secret", "client-secret")
    s3_config = json.loads(
        _kube_secret("lakehouse", "seaweedfs-s3-config", "seaweedfs_s3_config")
    )
    creds = s3_config["identities"][0]["credentials"][0]
    return RestCatalog(
        "nhl",
        **{
            "uri": "http://localhost:8181/catalog",
            "warehouse": "nhl",
            "credential": f"lakekeeper-spark:{lk_secret}",
            "scope": "lakekeeper",
            "oauth2-server-uri": KEYCLOAK_TOKEN_URI,
            "s3.endpoint": "http://localhost:8333",
            "s3.access-key-id": creds["accessKey"],
            "s3.secret-access-key": creds["secretKey"],
            "s3.path-style-access": "true",
        },
    )


def _section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def _print_df(label: str, sql: str, con: duckdb.DuckDBPyConnection) -> None:
    print(f"\n-- {label}")
    print(con.execute(sql).df().to_string(index=False))


def _load_tables(con: duckdb.DuckDBPyConnection, catalog: RestCatalog) -> None:
    for t in TABLES:
        t0 = time.time()
        arrow = catalog.load_table(f"silver.{t}").scan().to_arrow()
        con.register(t, arrow)
        print(f"  loaded silver.{t}: {arrow.num_rows:>10,} rows in {time.time() - t0:.1f}s")


def main() -> None:
    print("loading tables (full scans — plays will take ~30s)...")
    catalog = _build_catalog()
    con = duckdb.connect()
    _load_tables(con, catalog)

    _section("silver.games")
    _print_df("counts", """
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT game_id) AS distinct_game_ids,
               COUNT(DISTINCT season) AS distinct_seasons,
               SUM(CASE WHEN game_id   IS NULL THEN 1 ELSE 0 END) AS null_game_id,
               SUM(CASE WHEN season    IS NULL THEN 1 ELSE 0 END) AS null_season,
               SUM(CASE WHEN game_date IS NULL THEN 1 ELSE 0 END) AS null_game_date
        FROM games
    """, con)
    _print_df("by season", """
        SELECT season, COUNT(*) AS games, MIN(game_date) AS first, MAX(game_date) AS last
        FROM games GROUP BY season ORDER BY season
    """, con)

    _section("silver.plays")
    _print_df("counts", """
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT game_id) AS distinct_game_ids,
               COUNT(DISTINCT season) AS distinct_seasons,
               SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END) AS null_event_id,
               SUM(CASE WHEN game_id  IS NULL THEN 1 ELSE 0 END) AS null_game_id,
               SUM(CASE WHEN season   IS NULL THEN 1 ELSE 0 END) AS null_season
        FROM plays
    """, con)
    _print_df("plays per game (min/avg/max)", """
        SELECT MIN(c) AS min_plays, ROUND(AVG(c), 1) AS avg_plays, MAX(c) AS max_plays
        FROM (SELECT game_id, COUNT(*) c FROM plays GROUP BY game_id)
    """, con)
    _print_df("strength_state distribution", """
        SELECT strength_state, COUNT(*) AS plays
        FROM plays GROUP BY strength_state ORDER BY plays DESC
    """, con)

    _section("silver.players")
    _print_df("counts", """
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT player_id) AS distinct_player_ids,
               SUM(CASE WHEN player_id  IS NULL THEN 1 ELSE 0 END) AS null_player_id,
               SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) AS null_first_name,
               SUM(CASE WHEN last_name  IS NULL THEN 1 ELSE 0 END) AS null_last_name
        FROM players
    """, con)
    _print_df("position_code distribution", """
        SELECT position_code, COUNT(*) AS players
        FROM players GROUP BY position_code ORDER BY players DESC
    """, con)
    _print_df("date span sanity (first_seen > last_seen should be 0)", """
        SELECT COUNT(*) AS players_with_inverted_dates
        FROM players WHERE first_seen_date > last_seen_date
    """, con)

    _section("silver.game_rosters")
    _print_df("counts", """
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT game_id) AS distinct_game_ids,
               COUNT(DISTINCT player_id) AS distinct_player_ids,
               SUM(CASE WHEN game_id   IS NULL THEN 1 ELSE 0 END) AS null_game_id,
               SUM(CASE WHEN player_id IS NULL THEN 1 ELSE 0 END) AS null_player_id,
               SUM(CASE WHEN season    IS NULL THEN 1 ELSE 0 END) AS null_season
        FROM game_rosters
    """, con)
    _print_df("spots per game (min/avg/max)", """
        SELECT MIN(c) AS min_spots, ROUND(AVG(c), 1) AS avg_spots, MAX(c) AS max_spots
        FROM (SELECT game_id, COUNT(*) c FROM game_rosters GROUP BY game_id)
    """, con)
    _print_df("composite key uniqueness (dupes should be 0)", """
        SELECT COUNT(*) AS dupes FROM (
            SELECT game_id, player_id, COUNT(*) c
            FROM game_rosters GROUP BY game_id, player_id HAVING COUNT(*) > 1
        )
    """, con)

    _section("silver.teams")
    _print_df("counts", """
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT team_id) AS distinct_team_ids,
               SUM(CASE WHEN team_id IS NULL THEN 1 ELSE 0 END) AS null_team_id,
               SUM(CASE WHEN abbrev  IS NULL THEN 1 ELSE 0 END) AS null_abbrev,
               SUM(CASE WHEN name    IS NULL THEN 1 ELSE 0 END) AS null_name
        FROM teams
    """, con)
    _print_df("date span sanity (first_seen > last_seen should be 0)", """
        SELECT COUNT(*) AS teams_with_inverted_dates
        FROM teams WHERE first_seen_date > last_seen_date
    """, con)

    _section("cross-table referential integrity")
    _print_df("plays.game_id values missing from games (should be 0)", """
        SELECT COUNT(DISTINCT p.game_id) AS orphan_game_ids
        FROM plays p
        LEFT JOIN games g ON p.game_id = g.game_id
        WHERE g.game_id IS NULL
    """, con)
    _print_df("game_rosters.game_id values missing from games (should be 0)", """
        SELECT COUNT(DISTINCT gr.game_id) AS orphan_game_ids
        FROM game_rosters gr
        LEFT JOIN games g ON gr.game_id = g.game_id
        WHERE g.game_id IS NULL
    """, con)
    _print_df("game_rosters.player_id values missing from players (should be 0)", """
        SELECT COUNT(DISTINCT gr.player_id) AS orphan_player_ids
        FROM game_rosters gr
        LEFT JOIN players p ON gr.player_id = p.player_id
        WHERE p.player_id IS NULL
    """, con)
    _print_df("game_rosters.team_id values missing from teams (should be 0)", """
        SELECT COUNT(DISTINCT gr.team_id) AS orphan_team_ids
        FROM game_rosters gr
        LEFT JOIN teams t ON gr.team_id = t.team_id
        WHERE t.team_id IS NULL
    """, con)
    _print_df("games team_id values missing from teams (should be 0)", """
        SELECT COUNT(DISTINCT g.team_id) AS orphan_team_ids
        FROM (
            SELECT home_team_id AS team_id FROM games
            UNION
            SELECT away_team_id AS team_id FROM games
        ) g
        LEFT JOIN teams t ON g.team_id = t.team_id
        WHERE t.team_id IS NULL
    """, con)
    _print_df("games covered by plays (every game should have plays)", """
        SELECT COUNT(*) AS games_with_no_plays
        FROM games g
        LEFT JOIN (SELECT DISTINCT game_id FROM plays) p ON g.game_id = p.game_id
        WHERE p.game_id IS NULL
    """, con)


if __name__ == "__main__":
    main()
