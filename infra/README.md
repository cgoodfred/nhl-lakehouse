# infra

OpenTofu configuration for the nhl-lakehouse Pi cluster.

## Day-to-day

Infrastructure changes go through pull requests. CI runs `tofu fmt -check` and `tofu validate` on each PR. Merging to `main` triggers the `Deploy` workflow (`.github/workflows/deploy.yml`), which runs `tofu apply` on a self-hosted runner inside the cluster.

Local commands should stop at planning because the local shell and the deploy
workflow share the same Kubernetes state backend:

```bash
export KUBE_CONFIG_PATH=~/.kube/pi-config
tofu init
tofu plan
```

Do not run a local `tofu apply`, especially while `deploy.yml` is running.

## One-time bootstrap

For a fresh operator setting up against an existing cluster.

1. Install OpenTofu, kubectl, and the GitHub CLI.
2. Place the cluster kubeconfig at `~/.kube/pi-config`.
3. Export env vars (persist in shell profile):
   ```bash
   export KUBECONFIG=~/.kube/pi-config         # for kubectl and the tofu providers
   export KUBE_CONFIG_PATH=~/.kube/pi-config   # for the tofu kubernetes state backend
   ```
4. Create `terraform.tfvars` (gitignored) with values for the sensitive variables declared in `variables.tf`: `s3_access_key`, `s3_secret_key`, `github_pat`, `keycloak_issuer_url`, `lakekeeper_spark_client_secret`, `grafana_oidc_client_secret`, and `alertmanager_slack_webhook_url`.
5. `tofu init` — connects to the shared kubernetes state backend.

## Required GitHub repository secrets and variables

For the deploy workflow to apply changes via the self-hosted runner.

**Secrets** (Settings → Secrets and variables → Actions → Secrets):
- `S3_ACCESS_KEY` — mirrors `s3_access_key` in tfvars
- `S3_SECRET_KEY` — mirrors `s3_secret_key` in tfvars
- `RUNNER_GITHUB_PAT` — mirrors `github_pat` in tfvars
- `LAKEKEEPER_SPARK_CLIENT_SECRET` — mirrors `lakekeeper_spark_client_secret` in tfvars
- `GRAFANA_OIDC_CLIENT_SECRET` — existing Keycloak client secret for Grafana
- `ALERTMANAGER_SLACK_WEBHOOK_URL` — Slack incoming webhook for cluster alerts

**Variables** (same page, Variables tab — not sensitive):
- `KEYCLOAK_ISSUER_URL` — mirrors `keycloak_issuer_url` in tfvars

## Lakekeeper one-time setup

The Lakekeeper Helm release deploys the catalog server and runs DB migrations, but bootstrap and warehouse creation are operational steps you do once per fresh install. Lakekeeper's docs recommend the UI; the equivalent REST calls are below.

Port-forward the catalog locally:

```bash
kubectl port-forward -n lakehouse svc/lakekeeper 8181:8181
```

**Bootstrap** (sets the initial admin and creates the default project):

```bash
SECRET=$(kubectl get secret -n lakehouse lakekeeper-client-secret -o jsonpath='{.data.client-secret}' | base64 -d)
TOKEN=$(curl -sf -X POST 'https://<your-keycloak>/realms/<realm>/protocol/openid-connect/token' \
  -d "grant_type=client_credentials" -d "client_id=lakekeeper-spark" -d "client_secret=$SECRET" \
  | jq -r .access_token)

curl -X POST http://localhost:8181/management/v1/bootstrap \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"accept-terms-of-use": true}'
```

**Create the `nhl` warehouse** pointed at SeaweedFS:

```bash
AWS_ACCESS_KEY_ID=<from-tfvars>
AWS_SECRET_ACCESS_KEY=<from-tfvars>

curl -X POST http://localhost:8181/management/v1/warehouse \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "$(jq -n --arg ak "$AWS_ACCESS_KEY_ID" --arg sk "$AWS_SECRET_ACCESS_KEY" '{
    "warehouse-name": "nhl",
    "project-id": "00000000-0000-0000-0000-000000000000",
    "storage-profile": {
      "type": "s3",
      "bucket": "nhl-warehouse",
      "endpoint": "http://seaweedfs-s3.lakehouse.svc.cluster.local:8333",
      "region": "us-east-1",
      "path-style-access": true,
      "flavor": "s3-compat",
      "sts-enabled": false
    },
    "storage-credential": {
      "type": "s3",
      "credential-type": "access-key",
      "aws-access-key-id": $ak,
      "aws-secret-access-key": $sk
    }
  }')"
```

Verify with `curl -sf http://localhost:8181/management/v1/warehouse -H "Authorization: Bearer $TOKEN" | jq .`.

## Argo Workflows UI

Workflow orchestration is co-located in the `lakehouse` namespace alongside the SparkApplications it submits. The UI is reached via port-forward (no Ingress in V1):

```bash
kubectl port-forward -n lakehouse svc/argo-workflows-server 2746:2746
# then open http://localhost:2746
```

The chart default is `server.secure=false`, so the listener is plain HTTP. Auth is `--auth-mode=server`, which bypasses login for local development. Lift to `--secure` + `--auth-mode=client` with SSO before exposing via Ingress.

## Monitoring

The managed monitoring stack runs alongside the legacy Prometheus/Grafana
resources during migration:

- Mimir for 30-day metrics retention
- Loki for 14-day log retention
- Tempo for 7-day trace retention
- OpenTelemetry Collector agents on every node plus one cluster gateway
- Grafana, Alertmanager, kube-state-metrics, node exporter, and Postgres exporters

Mimir, Loki, and Tempo use the `monitoring-mimir`, `monitoring-loki`, and
`monitoring-tempo` SeaweedFS buckets. Their local-path PVCs retain WAL/cache
state. The namespace and public `grafana` IngressRoute are imported into
OpenTofu rather than recreated.

The first apply intentionally leaves `grafana.cluster.cgood.dev` pointing at
the legacy `grafana` Service. Inspect the replacement locally during its soak:

```bash
kubectl port-forward -n monitoring svc/monitoring-grafana 3000:3000
```

Validate at least the following before cutover:

```bash
kubectl get pods,pvc -n monitoring
kubectl logs -n monitoring deploy/monitoring-otel-gateway --since=15m
kubectl logs -n monitoring daemonset/monitoring-otel-agent --since=15m
kubectl port-forward -n monitoring svc/monitoring-mimir 9009:9009
curl -fsS http://localhost:9009/ready
```

In Grafana, confirm Keycloak login, all four provisioned data sources, recent
logs, node metrics, and the `Pi Cluster Overview` dashboard. Allow a minimum
48-hour parallel soak. Then change `monitoring_grafana_cutover` to `true` in a
small PR; this changes only the adopted route backend to `monitoring-grafana`.

Keep the legacy Prometheus, Grafana, Alertmanager, and kube-state-metrics for a
further 72-hour rollback window. They are deliberately not represented as
OpenTofu resources, so deleting them cannot accidentally remove a managed PVC.
After that window, inventory their owning resources and delete only the named
legacy Deployments, Services, ConfigMaps, Secrets, and ServiceAccounts. Never
delete the `monitoring` namespace or any monitoring PVC as part of cleanup.

Applications can send OTLP to
`monitoring-otel-gateway.monitoring.svc.cluster.local:4317` (gRPC) or port 4318
(HTTP). The scheduled-ingestion implementation should attach its run/season/job
attributes there and expose outcome, duration, record-count, and lag metrics.

## State

State persists as a Kubernetes Secret named `tfstate-default-lakehouse-state` in the `lakehouse` namespace. Both local applies and the in-cluster runner read and write through the same backend.
