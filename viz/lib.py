"""Shared helpers for Streamlit pages that read the NHL lakehouse."""

import os
import socket

import streamlit as st
from pyiceberg.catalog.rest import RestCatalog

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


@st.cache_resource
def catalog() -> RestCatalog:
    return RestCatalog(
        "nhl",
        **{
            "uri": os.environ["LAKEKEEPER_URI"],
            "warehouse": os.environ.get("LAKEKEEPER_WAREHOUSE", "nhl"),
            "credential": (
                f"{os.environ['LAKEKEEPER_CLIENT_ID']}:"
                f"{os.environ['LAKEKEEPER_CLIENT_SECRET']}"
            ),
            "scope": os.environ.get("LAKEKEEPER_SCOPE", "lakekeeper"),
            "oauth2-server-uri": os.environ["LAKEKEEPER_OAUTH2_SERVER_URI"],
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
            "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
            "s3.path-style-access": "true",
        },
    )


def fmt_season(season: int) -> str:
    s = str(season)
    return f"{s[:4]}-{s[4:]}"


CARD_BASE_STYLE = (
    "background:#1a2129; border-radius:6px; padding:14px 16px; "
    "min-height:80px; box-sizing:border-box;"
)


def metric_card(label: str, value: str, accent: str = "#ffce00") -> str:
    """Render an HTML metric card with a left-border accent."""
    return (
        f"<div style='{CARD_BASE_STYLE} border-left:4px solid {accent};'>"
        f"  <div style='color:#9aa5b1; font-size:0.75em; "
        f"text-transform:uppercase; letter-spacing:0.05em;'>{label}</div>"
        f"  <div style='color:#e8eef2; font-size:1.4em; font-weight:600; "
        f"margin-top:4px;'>{value}</div>"
        f"</div>"
    )
