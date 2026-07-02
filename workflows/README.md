# Argo Workflows

Argo platform configured by `infra/argo-workflows.tf`; this directory holds the WorkflowTemplate + Workflow YAML applied imperatively (mirroring the `spark/k8s/` pattern).

## Layout

```
workflows/
  templates/      # WorkflowTemplate definitions — parameterized, reusable
  workflows/      # One-shot Workflow definitions that submit templates
  cron/           # Scheduled CronWorkflows (V2)
```

## Day-to-day

```bash
# Apply (or update) a WorkflowTemplate so it's available for submission
kubectl apply -f workflows/templates/silver-single-table.yaml

# Submit a one-shot Workflow and follow its progress
argo submit -n lakehouse workflows/workflows/silver-games-example.yaml --watch

# Just create without watching — useful for the UI flow
kubectl create -n lakehouse -f workflows/workflows/silver-games-example.yaml

# Watch all Workflows
argo list -n lakehouse
argo get -n lakehouse <workflow-name>

# UI (port-forward; see infra/README.md for the namespace + URL)
kubectl port-forward -n lakehouse svc/argo-workflows-server 2746:2746
```

## What's here

### `templates/silver-single-table.yaml`

Reusable WorkflowTemplate. Takes `{tier, job_name, file_stem, executor_cores, executor_core_limit, executor_instances, executor_memory}` parameters and inlines a full SparkApplication spec that mirrors the existing `spark/k8s/silver/silver-games.yaml`. Uses `metadata.generateName` so each run creates a uniquely-named SparkApplication that can coexist with the imperatively-applied ones during the migration.

Two name params instead of one because K8s resource names use RFC 1123 (hyphens, no underscores) but Python module filenames use snake_case. For most jobs the two are trivially related — `game_rosters` (file) ↔ `game-rosters` (K8s name).

Executor sizing is required (no defaults) so every DAG node reads its own shape explicitly rather than inheriting a hidden default that might mask a mis-sized job. Driver sizing is fixed at 2c / 2g — all current silver + gold jobs use the same driver shape.

The step waits for the SparkApplication via `successCondition: status.applicationState.state == COMPLETED`. Without this, the step would return as soon as the resource was created — completely bypassing the Spark Operator's actual work.

Each generated SparkApplication is stamped with labels for cleanup + debugging: `app.kubernetes.io/managed-by=argo-workflows`, `workflows.argoproj.io/template=silver-single-table`, `workflows.argoproj.io/workflow=<parent workflow name>`, `nhl-lakehouse/tier=<tier>`, `nhl-lakehouse/job=<job_name>`.

### `templates/silver-full-rebuild.yaml`

DAG WorkflowTemplate that rebuilds the core PBP silver tier — `silver-games` runs first, then `silver-plays`, `silver-players`, `silver-game-rosters`, and `silver-teams` fan out in parallel (each depending only on `silver-games`). `silver-teams` was verified against `spark/jobs/silver/teams.py` to read only from `silver.games`, so it belongs in the fan-out, not as a terminal sequential step.

**Does NOT include `silver-tracking-frames`.** That table sits in a separate pipeline branch (`bronze-tracking-ingest` → `silver-tracking-frames` → gold tracking tables) whose upstream is the Python PPT bronze fetch, not the Go PBP ingest. That branch gets its own DAG in V2 alongside converting `bronze-tracking-ingest` to a WorkflowTemplate.

Per-node executor sizing mirrors the existing `spark/k8s/silver/*.yaml` manifests exactly (games/plays at 2g, players/game_rosters/teams at 1g). `parallelism: 1` serializes the DAG because the naive CPU math missed two costs — each Argo `resource:` step costs ~500m CPU on its own polling pod, and Spark Operator leaves completed driver pods around hoarding quota. First real run of the DAG (silver-full-rebuild-8dzkl) hit `SubmissionFailed` on players AND teams because their drivers requested 2c against a 9300m-used quota. The template header comment shows the corrected CPU accounting. Bump back to 2+ once the Pi quota widens or driver sizes shrink.

`silver-single-table.yaml` sets `timeToLiveSeconds: 60` on the inlined SparkApplication so completed / failed drivers get GC'd 60 seconds after any terminal state. Without this, drivers linger indefinitely, and even parallelism: 1 would eventually starve on lingering completions across runs.

### `workflows/silver-games-example.yaml`

One-shot Workflow that invokes `silver-single-table` with the games job. Smoke test for the install — proves the Workflow → SparkApplication CRD → COMPLETED loop works end-to-end.

### `workflows/silver-full-rebuild-example.yaml`

One-shot Workflow that invokes `silver-full-rebuild`. This is the DAG smoke test — rebuilds the core PBP silver tables (games/plays/players/game_rosters/teams) in one Argo submission. Wall-clock ~15-25 min on the Pi cluster at current data volumes.

## Cleanup

Argo doesn't garbage-collect the SparkApplications its Workflows create. Completed runs accumulate as their named CRDs. Filter by the Workflow-managed label so the query only targets Workflow output — a blanket "delete every COMPLETED SparkApplication in lakehouse" would also catch the imperatively-applied `silver-games`, `silver-plays`, etc.:

```bash
kubectl get sparkapplication -n lakehouse \
  -l app.kubernetes.io/managed-by=argo-workflows -o json \
  | jq -r '.items[] | select(.status.applicationState.state == "COMPLETED") | .metadata.name' \
  | xargs -r kubectl delete sparkapplication -n lakehouse
```

V2 may add `ttlSecondsAfterFinished` to the inlined SparkApplication template, or an `onExit:` cleanup step at the Workflow level.

## Out of scope (V1)

- Bronze + gold conversions (V2)
- `CronWorkflow` for nightly runs (V2)
- Argo Events / push triggers (V2)
- Exit hooks that write `silver.pipeline_runs` rows (deferred to the pipeline-health-dashboard Phase 2)
