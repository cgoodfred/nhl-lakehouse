# Architecture

End-to-end topology and data flow for the nhl-lakehouse. Pairs with [data-model.md](./data-model.md) for table shapes.

## System overview

```
   ┌───────────────────┐          ┌───────────────────┐
   │  NHL Stats API    │          │  NHL CDN (wsr)    │
   │  api-web.nhle.com │          │  wsr.nhle.com     │
   └─────────┬─────────┘          └─────────┬─────────┘
             │ Go CLI batch                 │ PySpark driver-side
             │ (rate-limited)               │ (rate-limited)
             ▼                              ▼
   ┌─────────────────────────────────────────────────┐
   │ SeaweedFS S3 · nhl-bronze bucket · raw JSON     │
   └───────────────────────┬─────────────────────────┘
                           │
                           ▼  Spark medallion jobs (Iceberg tables)
   ┌─────────────────────────────────────────────────┐
   │ Lakekeeper (Iceberg REST catalog)               │
   │   Postgres backing store                        │
   │   OAuth2 (Keycloak client-credentials)          │
   │                                                 │
   │   Data files in SeaweedFS S3 · nhl-warehouse    │
   │                                                 │
   │   Namespaces:                                   │
   │     nhl.silver.*  (normalized dim/fact)         │
   │     nhl.gold.*    (denormalized serving)        │
   └───────────────────────┬─────────────────────────┘
                           │
                           ▼
   ┌─────────────────────────────────────────────────┐
   │ Streamlit viz (Goal Map + Pipeline Health)      │
   │   PyIceberg → DuckDB in-memory joins            │
   │   Traefik IngressRoute → Cloudflare Tunnel      │
   │   nhl.cluster.cgood.dev                         │
   └─────────────────────────────────────────────────┘

   Orchestration: Argo Workflows submits SparkApplication CRDs;
                  Spark Operator runs drivers + executors.
```

## Ingest paths

Two independent ingest paths, both landing raw JSON in the `nhl-bronze` bucket.

**`ingest/` (Go CLI)** — batch pull from `api-web.nhle.com`. Rate-limited to 2 req/s + burst 5, exponential backoff, Retry-After honored. Runs as a Kubernetes Job on demand (see `infra/ingest-backfill.tf` for the backfill variant). Writes:

- `schedule/date=<YYYY-MM-DD>/schedule.json` — daily schedule envelopes
- `play-by-play/season=<YYYYYYYY>/date=<YYYY-MM-DD>/game_<id>.json` — one file per game
- `_runs/run=<runID>/failures.json` — per-run failure manifest for observability

**`spark/jobs/bronze/tracking_ingest.py`** — driver-side fetch of per-goal tracking payloads from `wsr.nhle.com`. Same rate-limited pattern, but runs inside a SparkApplication so it can maintain a `tracking_attempts` Iceberg table alongside the raw fetches. Writes:

- `tracking/season=<YYYYYYYY>/game_id=<id>/event_id=<id>/tracking.json`

## Medallion transforms

All Spark jobs live in one container image (`ghcr.io/cgoodfred/nhl-lakehouse/spark`). Different `mainApplicationFile` per SparkApplication manifest selects which job runs.

- **Silver** (7 tables) reads bronze JSON, produces normalized Iceberg tables. Includes 2 SCD-1 dimensions (players, teams), 4 fact/bridge tables (games, plays, game_rosters, tracking_frames), and 1 current-state audit table (tracking_attempts).
- **Gold** (3 tables) joins silver into denormalized serving shapes. `player_shots` for the map view; `goal_tracking_sequences` for animation playback; `goal_tracking_status` as a per-goal state machine the viz switches on.

Silver → gold has one silver-from-silver dependency: `silver.teams` is derived from `silver.games`.

## Storage layout

**SeaweedFS S3 buckets** (both provisioned in `infra/seaweedfs.tf`):

- `nhl-bronze` — raw JSON, ingest sink. Path-partitioned by date/season/game.
- `nhl-warehouse` — Iceberg data + metadata files under `nhl.silver.*` and `nhl.gold.*` prefixes.

SeaweedFS itself: master (`pi-master`, 5Gi), filer (`pi-node-one`, 20Gi), volume server (`pi-node-two`, 100Gi). Node selectors are hardcoded by hostname — see AGENTS.md landmines.

**Postgres × 2** (both Bitnami Helm, separate lifecycles):

- Lakekeeper catalog store (`infra/postgres.tf`, 2Gi PVC)
- Argo Workflows history store (`infra/argo-workflows.tf`, 2Gi PVC)

## Orchestration

Argo Workflows (`workflows/`) submits SparkApplication CRDs via the `resource:` action with `successCondition: status.applicationState.state == COMPLETED`. Argo polls the CRD to a terminal state; Spark Operator handles the driver/executor lifecycle. SparkApplications inlined by `workflows/templates/silver-single-table.yaml` set `timeToLiveSeconds: 60` for auto-GC of driver pods; the standalone `spark/k8s/**/*.yaml` manifests don't set a TTL and require manual cleanup.

Templates:

- `silver-single-table` — parameterized wrapper (tier, job_name, file_stem, executor sizing).
- `silver-full-rebuild` — 5-node DAG rebuilding the PBP silver tier (games → plays/players/game_rosters/teams). Serialized (`parallelism: 1`) due to a real quota-starvation failure.

No `CronWorkflow`s yet — schedule triggers are a v2 item. Today everything runs on demand via `argo submit` or `kubectl apply -f workflows/workflows/*.yaml`.

## Auth flow

Single Keycloak realm (`Lakehouse`) hosted at `keycloak.cluster.cgood.dev`. One shared OAuth2 client-credentials client:

- **Lakekeeper** validates the token on every REST request.
- **Spark jobs** authenticate to Lakekeeper via `spark.sql.catalog.nhl.credential=<client-id>:<client-secret>`, materialized into `spark.properties` by `entrypoint.sh` (see AGENTS.md for why env-var interpolation doesn't work here).
- **Viz** authenticates identically via PyIceberg.

Client credentials live in the K8s Secret `lakekeeper-client-secret` (managed by Terraform).

No per-user auth anywhere; the viz is public behind Cloudflare Tunnel.

## Kubernetes topology

Two namespaces:

- **`lakehouse`** — data platform. SeaweedFS, Lakekeeper (+Postgres), Argo Workflows (+Postgres), Spark Operator + SparkApplication CRs, viz Deployment + Traefik IngressRoute, ingest backfill Jobs.
- **`ci`** — self-hosted GitHub runner. Broad RBAC into `lakehouse` so it can `tofu apply` there.

Resource quota on `lakehouse`: 10 CPU / 32Gi memory / 10 PVCs / 400Gi storage. This is the constraint that forces `parallelism: 1` on `silver-full-rebuild`.

Ingress: viz is the only exposed service, via Traefik IngressRoute on `nhl.cluster.cgood.dev` (plain HTTP internally; TLS terminates at the Cloudflare Tunnel edge). Argo UI + Lakekeeper are port-forward only.

## Deploy topology

Terraform state lives as a K8s Secret (`tfstate-default-lakehouse-state` in `lakehouse`), so both local operators and the in-cluster runner share it. Never `tofu apply` locally while `deploy.yml` is running.

Push flow:

1. Push to `main` → path-filtered image builds (`build-image.yml`, `build-spark-image.yml`, `build-viz-image.yml`) push to `ghcr.io/cgoodfred/nhl-lakehouse/{ingest,spark,viz}` with `:latest` and `:${sha}` tags.
2. Push to `main` touching `infra/**` → `deploy.yml` runs `tofu apply` on the self-hosted `pi-cluster` runner.
3. Viz builds additionally open `chore/pin-viz-image` PRs that pin the new SHA into `infra/viz.tf`. Merging the PR is the actual viz rollout.

Ingest and Spark deploy immediately via `imagePullPolicy: Always` on `:latest` — no rollback safety net.

## Stack rationale

Short version of "why this stack for a one-person Pi cluster":

- **SeaweedFS over MinIO** — lighter footprint, S3-compatible enough for both raw JSON ingest and Iceberg data files.
- **Lakekeeper over Nessie / Hive Metastore** — first-class Iceberg REST catalog with modern OAuth2; single Postgres backing store.
- **Spark Operator over spark-submit** — CRD lifecycle is inspectable via kubectl, integrates cleanly with Argo's `resource:` action.
- **Argo Workflows over Airflow / Prefect** — no additional Python runtime to maintain, CRD-native, low idle footprint.
- **Streamlit + PyIceberg + DuckDB** — no separate query engine needed; DuckDB handles the joins in-memory in the same pod as the UI. Fast enough for the goal-map scale (~10⁴ goals/season).
- **OpenTofu with K8s Secret backend** — no external state store, no cloud dependency; self-hosted runner shares state cleanly.

The whole stack is chosen so it fits inside 10 CPU / 32Gi across 3 Raspberry Pi 5s with room for future features.
