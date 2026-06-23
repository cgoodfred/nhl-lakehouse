#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyiceberg[s3fs,pyarrow]",
#     "duckdb",
#     "pandas",
# ]
# ///
"""Diagnostics for silver.game_rosters — eyeball spot counts per game and
look for unexpected duplicate (game_id, player_id) pairs.

Auto-resolves credentials via kubectl. Requires port-forwards:
    kubectl port-forward -n lakehouse svc/lakekeeper 8181:8181 &
    kubectl port-forward -n lakehouse svc/seaweedfs-s3 8333:8333 &

Just run it:
    ./silver/tools/check_game_rosters.py
"""

import base64
import json
import socket
import subprocess

# Lakekeeper's /v1/config response includes overrides that point follow-up
# requests at its in-cluster service URL. From a laptop we can't resolve
# that DNS, so we hijack getaddrinfo to redirect it to localhost where the
# port-forward is listening. This must happen before requests/urllib3 are
# imported transitively below.
_real_getaddrinfo = socket.getaddrinfo


def _patched_getaddrinfo(host, *args, **kwargs):
    if host == "lakekeeper.lakehouse.svc.cluster.local":
        return _real_getaddrinfo("127.0.0.1", *args, **kwargs)
    if host == "seaweedfs-s3.lakehouse.svc.cluster.local":
        return _real_getaddrinfo("127.0.0.1", *args, **kwargs)
    return _real_getaddrinfo(host, *args, **kwargs)


socket.getaddrinfo = _patched_getaddrinfo

import duckdb  # noqa: E402
from pyiceberg.catalog.rest import RestCatalog  # noqa: E402

KEYCLOAK_TOKEN_URI = (
    "https://keycloak.cluster.cgood.dev/realms/Lakehouse/protocol/openid-connect/token"
)


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


def main() -> None:
    catalog = _build_catalog()
    arrow_table = catalog.load_table("silver.game_rosters").scan().to_arrow()

    con = duckdb.connect()
    con.register("game_rosters", arrow_table)

    print("=== spot counts per game ===")
    print(con.execute("""
        SELECT
            ROUND(AVG(c), 2) AS avg_spots,
            MIN(c)           AS min_spots,
            MAX(c)           AS max_spots,
            COUNT(*)         AS games
        FROM (SELECT game_id, COUNT(*) c FROM game_rosters GROUP BY game_id)
    """).df().to_string(index=False))

    print("\n=== duplicate (game_id, player_id) pairs (should be 0) ===")
    print(con.execute("""
        SELECT COUNT(*) AS dupes
        FROM (
            SELECT game_id, player_id, COUNT(*) c
            FROM game_rosters
            GROUP BY game_id, player_id
            HAVING COUNT(*) > 1
        )
    """).df().to_string(index=False))


if __name__ == "__main__":
    main()
