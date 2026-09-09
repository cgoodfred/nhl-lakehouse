#!/usr/bin/env python3
"""Offline checks for monitoring assets and low-footprint guardrails."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Iterable


INFRA_DIR = Path(__file__).resolve().parents[1]


def fail(message: str) -> None:
    print(f"monitoring validation failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def read(relative_path: str) -> str:
    return (INFRA_DIR / relative_path).read_text(encoding="utf-8")


def without_hcl_comments(value: str) -> str:
    return "\n".join(line.split("#", 1)[0] for line in value.splitlines())


def require_patterns(relative_path: str, patterns: dict[str, str]) -> None:
    content = without_hcl_comments(read(relative_path))
    for description, pattern in patterns.items():
        if re.search(pattern, content, flags=re.DOTALL) is None:
            fail(f"{relative_path} no longer enforces {description}")


def walk_panels(panels: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    for panel in panels:
        yield panel
        yield from walk_panels(panel.get("panels", []))


def dashboard_expressions(panels: Iterable[dict[str, Any]]) -> Iterable[str]:
    for panel in panels:
        datasource = panel.get("datasource") or {}
        if datasource.get("uid") != "mimir":
            continue
        for target in panel.get("targets", []):
            expression = target.get("expr")
            if expression:
                yield expression


def referenced_metrics(expression: str) -> set[str]:
    # Remove label values and Grafana variables before scanning identifiers.
    expression = re.sub(r'"(?:\\.|[^"\\])*"', "", expression)
    expression = re.sub(r"'(?:\\.|[^'\\])*'", "", expression)
    expression = re.sub(r"\$[A-Za-z_][A-Za-z0-9_]*", "", expression)
    expression = re.sub(r"\{[^{}]*\}", "{}", expression)

    metrics: set[str] = set()
    for match in re.finditer(r"\b[A-Za-z_:][A-Za-z0-9_:]*\b", expression):
        name = match.group(0)
        if "_" not in name:
            continue
        # Identifiers followed by '(' are PromQL functions such as
        # max_over_time, not metric names.
        if expression[match.end() :].lstrip().startswith("("):
            continue
        metrics.add(name)
    return metrics


def validate_dashboards() -> tuple[int, set[str]]:
    dashboard_dir = INFRA_DIR / "monitoring" / "dashboards"
    paths = sorted(dashboard_dir.glob("*.json"))
    if not paths:
        fail("no Grafana dashboards were found")

    grafana_config = read("monitoring-grafana.tf")
    provisioned_uids = set(re.findall(r'\buid\s*=\s*"([^"]+)"', grafana_config))
    if not provisioned_uids:
        fail("could not find provisioned Grafana datasource UIDs")

    metric_names: set[str] = set()
    total_panels = 0
    for path in paths:
        try:
            dashboard = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as error:
            fail(f"{path.relative_to(INFRA_DIR)} is not valid JSON: {error}")

        if not dashboard.get("title") or not dashboard.get("uid"):
            fail(f"{path.name} must have a title and stable uid")

        panels = list(walk_panels(dashboard.get("panels", [])))
        if not panels:
            fail(f"{path.name} has no panels")
        total_panels += len(panels)

        panel_ids = [panel.get("id") for panel in panels]
        if None in panel_ids or len(panel_ids) != len(set(panel_ids)):
            fail(f"{path.name} has missing or duplicate panel IDs")

        for panel in panels:
            if not panel.get("title"):
                fail(f"{path.name} panel {panel.get('id')} has no title")
            datasource = panel.get("datasource") or {}
            datasource_uid = datasource.get("uid")
            if datasource_uid and datasource_uid not in provisioned_uids:
                fail(
                    f"{path.name} panel {panel['id']} uses unprovisioned "
                    f"datasource {datasource_uid!r}"
                )

        for variable in dashboard.get("templating", {}).get("list", []):
            datasource_uid = (variable.get("datasource") or {}).get("uid")
            if datasource_uid and datasource_uid not in provisioned_uids:
                fail(
                    f"{path.name} variable {variable.get('name')!r} uses "
                    f"unprovisioned datasource {datasource_uid!r}"
                )
            if variable.get("name") == "namespace":
                if not variable.get("includeAll") or variable.get("allValue") != ".+":
                    fail(
                        f"{path.name} namespace variable must use .+ for its "
                        "Loki-compatible All value"
                    )

        for expression in dashboard_expressions(panels):
            metric_names.update(referenced_metrics(expression))

    return total_panels, metric_names


def validate_metric_allowlist(metric_names: set[str]) -> None:
    otel_config = read("monitoring-otel.tf")
    match = re.search(
        r"monitoring_metric_allowlist\s*=\s*\[(.*?)\n\s*\]",
        otel_config,
        flags=re.DOTALL,
    )
    if match is None:
        fail("could not find monitoring_metric_allowlist")

    patterns = re.findall(r'^\s*"([^"]+)",', match.group(1), flags=re.MULTILINE)
    if not patterns:
        fail("monitoring_metric_allowlist is empty")

    try:
        allowlist = re.compile(f"^(?:{'|'.join(patterns)})$")
    except re.error as error:
        fail(f"monitoring_metric_allowlist is not valid regex: {error}")

    alert_expressions = re.findall(
        r"^\s*expr:\s*(.+)$",
        read("monitoring/alerts/cluster.yaml"),
        flags=re.MULTILINE,
    )
    for expression in alert_expressions:
        metric_names.update(referenced_metrics(expression))

    missing = sorted(name for name in metric_names if allowlist.fullmatch(name) is None)
    if missing:
        fail(
            "dashboard or alert metrics are absent from the OTel allowlist: "
            + ", ".join(missing)
        )


def validate_regression_guardrails() -> None:
    require_patterns(
        "monitoring-otel.tf",
        {
            "the metric allowlist processor": r'"filter/metric_allowlist"\s*=\s*\{',
            "the metric allowlist pipeline": r'processors\s*=\s*\[[^\]]*"filter/metric_allowlist"[^\]]*\]',
            "disabled resource-to-label conversion": r"resource_to_telemetry_conversion\s*=\s*\{\s*enabled\s*=\s*false\s*\}",
            "disabled target_info series": r"target_info\s*=\s*\{\s*enabled\s*=\s*false\s*\}",
            "disabled scope_info series": r"disable_scope_info\s*=\s*true",
            "serialized remote writes": r"max_batch_request_parallelism\s*=\s*1.*?remote_write_queue\s*=\s*\{.*?num_consumers\s*=\s*1",
            "node-exporter as the sole host metric source": r"hostMetrics\s*=\s*\{\s*enabled\s*=\s*false\s*\}",
            "kube-state-metrics as the sole object-state source": r"clusterMetrics\s*=\s*\{\s*enabled\s*=\s*false\s*\}",
        },
    )
    require_patterns(
        "monitoring/mimir.yaml.tftpl",
        {
            "the label-count limit": r"max_label_names_per_series:\s*30\b",
            "the active-series limit": r"max_global_series_per_user:\s*50000\b",
            "the ingestion-rate limit": r"ingestion_rate:\s*5000\b",
            "the ingestion burst limit": r"ingestion_burst_size:\s*10000\b",
        },
    )
    require_patterns(
        "monitoring-mimir.tf",
        {
            "non-blocking WaitForFirstConsumer PVC creation": r"wait_until_bound\s*=\s*false",
            "the local-path Mimir PVC": r'storage_class_name\s*=\s*"local-path"',
            "Mimir PVC deletion protection": r"prevent_destroy\s*=\s*true",
        },
    )
    require_patterns(
        "monitoring-loki.tf",
        {
            "namespace-scoped Loki RBAC": r"rbac\s*=\s*\{\s*namespaced\s*=\s*true\s*\}",
            "the disabled Loki rules sidecar": r"sidecar\s*=\s*\{\s*rules\s*=\s*\{\s*enabled\s*=\s*false\s*\}\s*\}",
            "the disabled Loki canary": r"lokiCanary\s*=\s*\{\s*enabled\s*=\s*false\s*\}",
            "the disabled canary-dependent chart test": r"test\s*=\s*\{\s*enabled\s*=\s*false\s*\}",
        },
    )
    require_patterns(
        "monitoring-ingress.tf",
        {
            "the managed Grafana service port": r"port\s*=\s*var\.monitoring_grafana_cutover\s*\?\s*80\s*:\s*3000",
            "ownership of the adopted route fields": r"force_conflicts\s*=\s*true",
        },
    )

    postgres_exporters = read("monitoring-postgres.tf")
    if 'path = "/-/healthy"' in postgres_exporters or 'path = "/-/ready"' in postgres_exporters:
        fail("Postgres exporter probes must not use unsupported health endpoints")
    if postgres_exporters.count('"/metrics"') < 4:
        fail("both Postgres exporters must scrape and probe the /metrics endpoint")


def main() -> None:
    panel_count, metric_names = validate_dashboards()
    validate_metric_allowlist(metric_names)
    validate_regression_guardrails()
    print(
        "monitoring validation passed: "
        f"{panel_count} dashboard panels, {len(metric_names)} consumed metric families"
    )


if __name__ == "__main__":
    main()
