#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyiceberg[s3fs,pyarrow]",
#     "duckdb",
# ]
# ///
"""Refresh a local DuckDB snapshot of every silver+gold table, then drop into
the `duckdb` CLI for interactive querying.

After the first run you can also just open the snapshot directly without
re-pulling, which is faster if you don't need fresh data:

    duckdb ~/.cache/nhl-lakehouse.duckdb

Auto-resolves Lakekeeper + SeaweedFS credentials via kubectl. Requires
port-forwards to be running:

    kubectl port-forward -n lakehouse svc/lakekeeper 8181:8181 &
    kubectl port-forward -n lakehouse svc/seaweedfs-s3 8333:8333 &

Requires the `duckdb` CLI on PATH (`brew install duckdb`).
"""

import base64
import json
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path

# DNS hijack — Lakekeeper's catalog overrides advertise in-cluster URLs that
# don't resolve from a laptop. Must run before requests/urllib3 import.
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

DB_PATH = Path.home() / ".cache" / "nhl-lakehouse.duckdb"

# Tables to materialize. (schema, table_name) — Iceberg ns and table maps
# 1:1 to DuckDB schema and table so queries read identically to the catalog.
TABLES = [
    ("silver", "games"),
    ("silver", "plays"),
    ("silver", "players"),
    ("silver", "game_rosters"),
    ("silver", "teams"),
    ("gold", "player_shots"),
]


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


def _refresh_snapshot() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    catalog = _build_catalog()
    con = duckdb.connect(str(DB_PATH))
    print(f"materializing tables into {DB_PATH}\n")
    for schema, name in TABLES:
        print(f"  {schema}.{name:<14} ", end="", flush=True)
        try:
            arrow = catalog.load_table(f"{schema}.{name}").scan().to_arrow()
        except Exception as exc:
            print(f"SKIP ({exc.__class__.__name__}: {exc})")
            continue
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        con.register("_tmp_arrow", arrow)
        con.execute(f"CREATE OR REPLACE TABLE {schema}.{name} AS SELECT * FROM _tmp_arrow")
        con.unregister("_tmp_arrow")
        print(f"{arrow.num_rows:>10,} rows")
    con.close()


def _exec_duckdb_cli() -> None:
    cli = shutil.which("duckdb")
    if not cli:
        print(
            "\nduckdb CLI not found on PATH. Install with `brew install duckdb`, "
            f"then run:\n    duckdb {DB_PATH}",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"\nopening duckdb shell against {DB_PATH}")
    print("tables: silver.games, silver.plays, silver.players, silver.game_rosters,")
    print("        silver.teams, gold.player_shots")
    print("type `.tables` to list, `.schema <table>` to inspect, Ctrl-D to quit.\n")
    os.execvp(cli, [cli, str(DB_PATH)])


def main() -> None:
    _refresh_snapshot()
    _exec_duckdb_cli()


if __name__ == "__main__":
    main()
