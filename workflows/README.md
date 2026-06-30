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

Reusable WorkflowTemplate. Takes `{tier, table}` parameters and inlines a full SparkApplication spec that mirrors the existing `spark/k8s/silver/silver-games.yaml`. Uses `metadata.generateName` so each run creates a uniquely-named SparkApplication that can coexist with the imperatively-applied ones during the migration.

The step waits for the SparkApplication via `successCondition: status.applicationState.state == COMPLETED`. Without this, the step would return as soon as the resource was created — completely bypassing the Spark Operator's actual work.

### `workflows/silver-games-example.yaml`

One-shot Workflow that invokes `silver-single-table` with `tier=silver, table=games`. Smoke test for the install — proves the Workflow → SparkApplication CRD → COMPLETED loop works end-to-end. Once verified, the same pattern extends to other silver/gold jobs by changing the two parameters.

## Cleanup

Argo doesn't garbage-collect the SparkApplications its Workflows create. Completed runs accumulate as their named CRDs:

```bash
# Drop all COMPLETED SparkApplications from prior Workflow runs
kubectl get sparkapplication -n lakehouse -o json \
  | jq -r '.items[] | select(.status.applicationState.state == "COMPLETED") | .metadata.name' \
  | xargs -r kubectl delete sparkapplication -n lakehouse
```

V2 may add `ttlSecondsAfterFinished` to the inlined SparkApplication template, or an `onExit:` cleanup step at the Workflow level.

## Out of scope (V1)

- `silver-full-rebuild` DAG (PR C) — coordinates all silver jobs in topological order
- Bronze + gold conversions (V2)
- `CronWorkflow` for nightly runs (V2)
- Argo Events / push triggers (V2)
- Exit hooks that write `silver.pipeline_runs` rows (deferred to the pipeline-health-dashboard Phase 2)
