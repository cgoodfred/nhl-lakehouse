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

The manifest sets `imagePullPolicy: Always` because the default image tag is `:latest`, which moves between builds. The Spark Operator's own default is `IfNotPresent`, which would reuse a stale cached image on the node — leaving to K8s's auto-default doesn't help here because the operator sets the policy before the pod spec reaches the kubelet.

For reproducible runs, pin `spec.image` to an immutable SHA tag (`ghcr.io/cgoodfred/nhl-lakehouse/silver:<full-sha>`) and you can flip `imagePullPolicy` to `IfNotPresent` for faster repeated pod starts on the same SHA. SHA pin is the right move when you want to know exactly which build ran.

## Available jobs

| Job manifest | PySpark | Target table | Source |
|---|---|---|---|
| `silver-games.yaml` | `games.py` | `nhl.silver.games` | bronze PBP envelopes |
| `silver-plays.yaml` | `plays.py` | `nhl.silver.plays` (partitioned by `season`) | bronze PBP envelopes (plays array) |
| `silver-players.yaml` | `players.py` | `nhl.silver.players` (SCD-1 dim) | bronze PBP envelopes (rosterSpots array, deduped) |
| `silver-game-rosters.yaml` | `game_rosters.py` | `nhl.silver.game_rosters` (bridge, partitioned by `season`) | bronze PBP envelopes (rosterSpots array, per-game grain) |
| `silver-teams.yaml` | `teams.py` | `nhl.silver.teams` (SCD-1 dim) | `nhl.silver.games` (silver-from-silver) |

`silver-teams.yaml` depends on `silver.games` existing — apply silver-games first.

Jobs share `silver/common.py` (just a `get_spark(app_name)` helper today). The Dockerfile copies it next to the jobs so `from common import ...` works in-cluster.

## Tests

Transformation logic lives in pure functions (e.g. `transform_plays` in `plays.py`) so it can be exercised against fixtures with a local SparkSession. Tests live under `silver/tests/`.

Dependencies (`pyspark`, `pytest`, `ruff`) and tool config (ruff lint rules, pytest test discovery) are declared in `silver/pyproject.toml` and pinned in `silver/uv.lock`. Install [uv](https://docs.astral.sh/uv/) (`brew install uv`) then:

```bash
cd silver
uv sync --group dev
uv run pytest
uv run ruff check .
```

Requires Java 17 on the path (`brew install openjdk@17` on macOS) so PySpark can launch a local JVM. `conftest.py` adjusts `sys.path` so test modules can `from plays import ...` the same way the in-container jobs do.
