# spark

PySpark transformations producing silver and gold Iceberg tables in the `nhl` warehouse.

## Layout

```
spark/
  Dockerfile          # one image powers all tiers
  entrypoint.sh       # wrapper that materializes Spark creds into spark.properties
  jobs/
    common.py         # shared helpers (get_spark, etc.)
    silver/           # bronze → silver transforms
    gold/             # silver → gold transforms (added in phase 2)
  k8s/
    silver/           # SparkApplication manifests, one per silver table
    gold/             # SparkApplication manifests, one per gold table
  tests/              # pytest, fixtures shared across tiers
  tools/              # ad-hoc query/verification scripts
```

`jobs/` is copied into the image at `/opt/jobs/` with `PYTHONPATH=/opt/jobs`, so a job in any tier subdir can `from common import get_spark` without package boilerplate.

## Image

The `spark/Dockerfile` produces `ghcr.io/cgoodfred/nhl-lakehouse/spark:<tag>` with:

- `apache/spark:3.5.7-python3` base
- `iceberg-spark-runtime-3.5_2.12-1.10.0.jar`
- `iceberg-aws-bundle-1.10.0.jar`
- `hadoop-aws-3.3.4.jar`
- `aws-java-sdk-bundle-1.12.770.jar`
- All PySpark jobs under `/opt/jobs/<tier>/`

Built and pushed on every push to `main` that touches `spark/**` (`.github/workflows/build-spark-image.yml`). One image powers every job across both tiers; jobs differ only in their SparkApplication manifest's `mainApplicationFile`.

## Running a job

Each job is a separate `SparkApplication` CRD under `spark/k8s/<tier>/`. The Spark Operator (installed in the `lakehouse` namespace) picks up the CR and launches driver + executor pods.

Apply, watch, verify:

```bash
kubectl apply -f spark/k8s/silver/silver-games.yaml

kubectl get sparkapplication -n lakehouse -w

kubectl logs -n lakehouse silver-games-driver -f
```

Final state:

- `kubectl get sparkapplication silver-games -n lakehouse` shows `COMPLETED`
- Driver log ends with `silver-games: complete (rows=N)`
- Table appears in Lakekeeper's REST list-tables call for the `silver` namespace

## Iterating

`SparkApplication` is immutable once created. After merging a code change to `main`:

1. Wait for the `Build spark image` workflow to complete (`gh run list --workflow=build-spark-image.yml --limit 1`).
2. Re-apply:
   ```bash
   kubectl delete sparkapplication silver-games -n lakehouse
   kubectl apply -f spark/k8s/silver/silver-games.yaml
   ```

The manifest sets `imagePullPolicy: Always` because the default image tag is `:latest`, which moves between builds. The Spark Operator's own default is `IfNotPresent`, which would reuse a stale cached image on the node.

For reproducible runs, pin `spec.image` to an immutable SHA tag (`ghcr.io/cgoodfred/nhl-lakehouse/spark:<full-sha>`) and you can flip `imagePullPolicy` to `IfNotPresent` for faster repeated pod starts on the same SHA.

## Available jobs

### Silver

| Job manifest | PySpark | Target table | Source |
|---|---|---|---|
| `silver/silver-games.yaml` | `silver/games.py` | `nhl.silver.games` | bronze PBP envelopes |
| `silver/silver-plays.yaml` | `silver/plays.py` | `nhl.silver.plays` (partitioned by `season`) | bronze PBP envelopes (plays array) |
| `silver/silver-players.yaml` | `silver/players.py` | `nhl.silver.players` (SCD-1 dim) | bronze PBP envelopes (rosterSpots array, deduped) |
| `silver/silver-game-rosters.yaml` | `silver/game_rosters.py` | `nhl.silver.game_rosters` (bridge, partitioned by `season`) | bronze PBP envelopes (rosterSpots array, per-game grain) |
| `silver/silver-teams.yaml` | `silver/teams.py` | `nhl.silver.teams` (SCD-1 dim) | `nhl.silver.games` (silver-from-silver) |

`silver-teams.yaml` depends on `silver.games` existing — apply silver-games first.

## Tests

Transformation logic lives in pure functions (e.g. `transform_plays` in `silver/plays.py`) so it can be exercised against fixtures with a local SparkSession. Tests live under `spark/tests/`.

Dependencies (`pyspark`, `pytest`, `ruff`) and tool config (ruff lint rules, pytest test discovery) are declared in `spark/pyproject.toml` and pinned in `spark/uv.lock`. Install [uv](https://docs.astral.sh/uv/) (`brew install uv`) then:

```bash
cd spark
uv sync --group dev
uv run pytest
uv run ruff check .
```

Requires Java 17 on the path (`brew install openjdk@17` on macOS) so PySpark can launch a local JVM. `conftest.py` mirrors the in-container `PYTHONPATH`/tier-subdir layout so test modules can `from common import ...` and `from games import ...` the same way the jobs do at runtime.
