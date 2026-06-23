# viz

Streamlit dashboard for the NHL lakehouse. Reads `gold.player_shots` from the Iceberg catalog and renders an interactive goal map.

## What it shows

Filter cascade — season → team → player. For the selected player, plot every goal as a marker on a top-down NHL rink at the (x, y) coordinates where the shot was taken from. Right panel shows a metric card with total goal count and a date / period / time / shot-type table.

## Image

`viz/Dockerfile` produces `ghcr.io/cgoodfred/nhl-lakehouse/viz:<tag>` from `python:3.12-slim` with the lockfile-pinned Python deps installed via `uv sync --frozen --no-dev`. Built on every push to `main` touching `viz/**` (`.github/workflows/build-viz-image.yml`).

## In-cluster deploy

`infra/viz.tf` defines a Deployment + ClusterIP Service + Traefik Ingress under `nhl.cluster.cgood.dev`. Cloudflare Tunnel handles HTTPS at the edge; the Ingress is plain HTTP. Env vars come from existing Secrets (`lakekeeper-client-secret`, `ingest-s3-creds`) so no new secrets are needed.

## Local development

```bash
cd viz
uv sync

# In two other shells:
kubectl port-forward -n lakehouse svc/lakekeeper 8181:8181
kubectl port-forward -n lakehouse svc/seaweedfs-s3 8333:8333

# Pull env vars from cluster Secrets
export LAKEKEEPER_URI=http://lakekeeper.lakehouse.svc.cluster.local:8181/catalog
export LAKEKEEPER_WAREHOUSE=nhl
export LAKEKEEPER_SCOPE=lakekeeper
export LAKEKEEPER_OAUTH2_SERVER_URI="https://keycloak.cluster.cgood.dev/realms/Lakehouse/protocol/openid-connect/token"
export LAKEKEEPER_CLIENT_ID=lakekeeper-spark
export LAKEKEEPER_CLIENT_SECRET=$(kubectl get secret -n lakehouse lakekeeper-client-secret -o jsonpath='{.data.client-secret}' | base64 -d)
export S3_ENDPOINT=http://seaweedfs-s3.lakehouse.svc.cluster.local:8333
export S3_ACCESS_KEY=$(kubectl get secret -n lakehouse seaweedfs-s3-config -o jsonpath='{.data.seaweedfs_s3_config}' | base64 -d | jq -r '.identities[0].credentials[0].accessKey')
export S3_SECRET_KEY=$(kubectl get secret -n lakehouse seaweedfs-s3-config -o jsonpath='{.data.seaweedfs_s3_config}' | base64 -d | jq -r '.identities[0].credentials[0].secretKey')

uv run streamlit run app.py
```

The app's DNS hijack auto-engages when run outside a K8s pod (no `KUBERNETES_SERVICE_HOST` env var), redirecting Lakekeeper's catalog overrides to your port-forwards.
