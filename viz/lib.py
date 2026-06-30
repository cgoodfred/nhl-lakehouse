"""Shared helpers for Streamlit pages that read the NHL lakehouse."""

import os
import socket
import time

import requests
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


def _catalog_token() -> str:
    response = requests.post(
        os.environ["LAKEKEEPER_OAUTH2_SERVER_URI"],
        data={
            "grant_type": "client_credentials",
            "client_id": os.environ["LAKEKEEPER_CLIENT_ID"],
            "client_secret": os.environ["LAKEKEEPER_CLIENT_SECRET"],
            "scope": os.environ.get("LAKEKEEPER_SCOPE", "lakekeeper"),
        },
        timeout=10,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("OAuth token response did not include access_token")
    return token


def catalog() -> RestCatalog:
    return RestCatalog(
        "nhl",
        **{
            "uri": os.environ["LAKEKEEPER_URI"],
            "warehouse": os.environ.get("LAKEKEEPER_WAREHOUSE", "nhl"),
            "token": _catalog_token(),
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
            "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
            "s3.path-style-access": "true",
        },
    )


def load_table_arrow(
    table_name: str,
    *,
    row_filter=None,
    selected_fields: tuple[str, ...] = ("*",),
    limit: int | None = None,
    attempts: int = 3,
    delay_sec: float = 0.75,
):
    """Load an Iceberg table as Arrow with a short retry for startup races."""
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            scan_kwargs = {
                "selected_fields": selected_fields,
                "limit": limit,
            }
            if row_filter is not None:
                scan_kwargs["row_filter"] = row_filter
            return (
                catalog()
                .load_table(table_name)
                .scan(**scan_kwargs)
                .to_arrow()
            )
        except Exception as exc:
            last_exc = exc
            if attempt < attempts - 1:
                time.sleep(delay_sec * (attempt + 1))
    raise last_exc or RuntimeError(f"Could not load {table_name}")


def lakehouse_error_message(table_name: str, exc: Exception) -> str:
    detail = str(exc) or exc.__class__.__name__
    lowered = detail.lower()

    if isinstance(exc, KeyError):
        return (
            "The lakehouse connection is not configured for this Streamlit process. "
            f"Missing environment value: `{exc}`"
        )
    if "forbidden" in lowered or "403" in lowered:
        return (
            f"Could not read `{table_name}` from the lakehouse after retrying.\n\n"
            "The catalog returned `Forbidden`, which usually means the viz pod does "
            "not yet have valid Lakekeeper/S3 credentials or the catalog token is not "
            "ready. Refreshing may resolve this if the pod just started."
        )
    if "service unavailable" in lowered or "status 503" in lowered or " 503" in lowered:
        return (
            f"Could not read `{table_name}` from the lakehouse after retrying.\n\n"
            "The lakehouse returned `503 Service Unavailable`. This usually means the "
            "catalog or object storage could not serve the read quickly enough. Try "
            "again shortly; if it persists, the table read path likely needs a smaller "
            "query or a pre-shaped serving table."
        )
    if "not found" in lowered or "no such table" in lowered:
        return (
            f"Could not find `{table_name}` in the lakehouse. The upstream job may "
            "not have created it yet."
        )
    return f"Could not read `{table_name}` from the lakehouse after retrying.\n\n`{detail}`"


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
