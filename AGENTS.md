# AGENTS.md

Guidance for AI agents in this repo. Read the load-bearing conventions before editing.

## What this repo is

Personal end-to-end NHL data lakehouse on a 3-node on-prem Raspberry Pi Kubernetes cluster. Single operator. Public UI: `https://nhl.cluster.cgood.dev`.

Flow: NHL API → `ingest` (Go, → S3 bronze) → Spark medallion jobs → Iceberg tables via Lakekeeper → Streamlit `viz`. Orchestrated by Argo Workflows submitting SparkApplication CRDs. Provisioned by OpenTofu with state in a K8s Secret.

Deep dives: [`docs/architecture.md`](docs/architecture.md) (full topology + rationale) · [`docs/data-model.md`](docs/data-model.md) (every silver/gold table schema).

## Repo map

| Path | What / deep-dive doc |
|---|---|
| `ingest/` | Go CLI: NHL API → S3 bronze JSON |
| `spark/` | PySpark jobs (`jobs/{bronze,silver,gold}/`), tests, K8s manifests, image · **`spark/README.md`** |
| `viz/` | Streamlit dashboard · **`viz/README.md`** |
| `workflows/` | Argo `WorkflowTemplate`s + `Workflow`s · **`workflows/README.md`** |
| `infra/` | OpenTofu (flat, no modules) · **`infra/README.md`** |
| `docs/` | Cross-cutting docs · `architecture.md`, `data-model.md` |
| `.github/workflows/` | 6 workflows: per-component image builds, tests, `tofu apply` |

Iceberg namespaces: `nhl.silver.{games, plays, players, teams, game_rosters, tracking_frames, tracking_attempts}`, `nhl.gold.{player_shots, goal_tracking_status, goal_tracking_sequences}`. `spark/viz/` is an empty stub — ignore.

## Load-bearing conventions

Each of these looks like something you might "clean up." Don't — every one has a real reason.

- **Spark runtime is Python 3.8** (`apache/spark:3.5.7-python3`); dev is 3.12. Jobs using modern annotation syntax must use `from __future__ import annotations`. Runtime code must be 3.8-compatible (no `datetime.UTC`, no PEP 604 unions outside annotations).
- **`spark/entrypoint.sh`** materializes Lakekeeper/S3 creds into `spark.properties` before launching the driver. Iceberg's `SparkCatalog` reads options via `SparkConf.getAllWithPrefix`, which bypasses Spark's `${env:VAR}` substitution — so env-var interpolation for `spark.sql.catalog.nhl.*` doesn't work.
- **Every SparkApplication manifest** sets `spark.sql.catalog.nhl.rest.metrics-reporter-impl: org.apache.iceberg.metrics.LoggingMetricsReporter`. Default reporter has an OAuth token-refresh bug that flips completed jobs to FAILED after the snapshot commits.
- **`workflows/templates/silver-full-rebuild.yaml` uses `parallelism: 1`** after a real quota-starvation failure. Argo polling pods (~500m) + Spark drivers (2c) at parallelism ≥3 overshoot the 10-CPU namespace quota. Raising parallelism requires also raising the quota in `infra/namespace.tf`. Math is in `workflows/README.md`.
- **`viz/lib.py` monkey-patches `socket.getaddrinfo`** when `KUBERNETES_SERVICE_HOST` is unset, redirecting `*.svc.cluster.local` to `127.0.0.1` for local port-forward dev. Update the hijack list if in-cluster service names change.
- **Image pinning is asymmetric.** Viz is SHA-pinned via an auto-opened `chore/pin-viz-image` PR (`build-viz-image.yml` rewrites `infra/viz.tf` with perl regex + `tofu fmt`); merging the PR deploys. Ingest and Spark still use `:latest` with `imagePullPolicy: Always` — no rollback safety net.
- **Terraform state is a K8s Secret** (`tfstate-default-lakehouse-state` in `lakehouse` ns; `backend "kubernetes"` in `infra/versions.tf`). Never run `tofu apply` locally while `deploy.yml` is running — both share the state.
- **`infra/seaweedfs.tf` hardcodes node selectors** to `pi-master`, `pi-node-one`, `pi-node-two` by hostname. Rename a node and SeaweedFS won't schedule.
- **`infra/terraform.tfvars` is plaintext secrets** (gitignored, not encrypted). Don't `cat`, `echo`, print, or commit its contents.

## Conventions

- **Commits**: Conventional Commits with component scope — `feat(spark): ...`, `fix(viz): ...`, `chore(infra): ...`. Scopes: `ingest`, `spark`, `viz`, `workflows`, `infra`, `deps`.
- **Pre-PR**: Spark → `uv run ruff check . && uv run pytest`; Ingest → `go vet ./... && go test -race ./...`; Infra → `tofu fmt && tofu validate`; Argo templates have no automated lint yet.
- **Deploy**: path-filtered image builds on `main` push to `ghcr.io/cgoodfred/nhl-lakehouse/*` (no workload restart — new Ingest/Spark pods pull `:latest` on their next submission). **Every** push to `main` (no paths filter) triggers `deploy.yml` → `tofu apply` on the self-hosted `pi-cluster` runner, so shared TF state is touched on every merge.

## Don't

- Run `tofu apply` locally, or `git push --force` on `main`.
- Delete Lakekeeper or Argo Postgres PVCs (data loss).
- Upgrade Iceberg without confirming the metrics-reporter bug is fixed.
- Remove any of the load-bearing conventions above as "cleanup."
- Surface or commit `infra/terraform.tfvars` values.
