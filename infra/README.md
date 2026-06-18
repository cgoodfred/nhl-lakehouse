# infra

OpenTofu configuration for the nhl-lakehouse Pi cluster.

## Day-to-day

Infrastructure changes go through pull requests. CI runs `tofu fmt -check` and `tofu validate` on each PR. Merging to `main` triggers the `Deploy` workflow (`.github/workflows/deploy.yml`), which runs `tofu apply` on a self-hosted runner inside the cluster.

To apply changes locally as an escape hatch:

```bash
export KUBE_CONFIG_PATH=~/.kube/pi-config
tofu init
tofu apply
```

## One-time bootstrap

For a fresh operator setting up against an existing cluster.

1. Install OpenTofu, kubectl, and the GitHub CLI.
2. Place the cluster kubeconfig at `~/.kube/pi-config`.
3. Export env vars (persist in shell profile):
   ```bash
   export KUBECONFIG=~/.kube/pi-config         # for kubectl and the tofu providers
   export KUBE_CONFIG_PATH=~/.kube/pi-config   # for the tofu kubernetes state backend
   ```
4. Create `terraform.tfvars` (gitignored) with values for the sensitive variables declared in `variables.tf`: `s3_access_key`, `s3_secret_key`, `github_pat`.
5. `tofu init` — connects to the shared kubernetes state backend.

## Required GitHub repository secrets

For the deploy workflow to apply changes via the self-hosted runner:

- `S3_ACCESS_KEY` — mirrors `s3_access_key` in tfvars
- `S3_SECRET_KEY` — mirrors `s3_secret_key` in tfvars
- `RUNNER_GITHUB_PAT` — mirrors `github_pat` in tfvars

Set under Settings → Secrets and variables → Actions.

## State

State persists as a Kubernetes Secret named `tfstate-default-lakehouse-state` in the `lakehouse` namespace. Both local applies and the in-cluster runner read and write through the same backend.
