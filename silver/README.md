# silver

PySpark transformations from bronze JSON to Iceberg tables in the `nhl` warehouse.

## Image

The `silver/Dockerfile` produces `ghcr.io/cgoodfred/nhl-lakehouse/silver:<tag>` with:

- `apache/spark:3.5.7-python3` base
- `iceberg-spark-runtime-3.5_2.12-1.10.0.jar`
- `iceberg-aws-bundle-1.10.0.jar`
- `hadoop-aws-3.3.4.jar`
- `aws-java-sdk-bundle-1.12.770.jar`
- All PySpark jobs under `/opt/jobs/`

The image is built and pushed on every push to `main` that touches `silver/**` (`.github/workflows/build-silver-image.yml`). One image powers every silver job; jobs differ only in their SparkApplication manifest's `mainApplicationFile`.

## Running a job

Each silver job is a separate `SparkApplication` CRD under `silver/k8s/`. The Spark Operator (installed in `lakehouse` namespace) picks up the CR and launches driver + executor pods.

Apply, watch, verify:

```bash
# Apply the job (creates a fresh SparkApplication; replaces any existing one with the same name)
kubectl apply -f silver/k8s/silver-games.yaml

# Watch status
kubectl get sparkapplication -n lakehouse -w

# Tail driver logs while the job runs
kubectl logs -n lakehouse silver-games-driver -f
```

Final state:

- `kubectl get sparkapplication silver-games -n lakehouse` shows `COMPLETED`
- Driver log ends with `silver-games: complete (rows=N)`
- Iceberg metadata files visible via `aws s3 ls s3://nhl-warehouse/silver/games/metadata/` (use port-forward to SeaweedFS S3 as documented in `infra/README.md`)
- Table appears in Lakekeeper's REST list-tables call for the `silver` namespace

## Iterating

`SparkApplication` is immutable once created. After merging a code change to `main`:

1. Wait for the `Build silver image` workflow to complete (`gh run list --workflow=build-silver-image.yml --limit 1`).
2. Re-apply:
   ```bash
   kubectl delete sparkapplication silver-games -n lakehouse
   kubectl apply -f silver/k8s/silver-games.yaml
   ```

The manifest omits `imagePullPolicy`, so K8s applies its default behavior based on the image tag:

- **`:latest`** (current manifest default) → K8s pulls fresh on every pod start. Good for iteration; you'll always run the newest image without manual tag changes. ~30s pull on a Pi per pod start.
- **`:<sha>`** → K8s caches and reuses (IfNotPresent). Good for reproducible runs — pin a known-good SHA when you want stability.

To pin a SHA, edit `spec.image` to `ghcr.io/cgoodfred/nhl-lakehouse/silver:<full-sha>` and `kubectl apply` — no other changes needed. The tag itself dictates the pull behavior.

## Available jobs

| Job manifest | PySpark | Target table | Source |
|---|---|---|---|
| `silver-games.yaml` | `games.py` | `nhl.silver.games` | bronze PBP envelopes |
