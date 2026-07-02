resource "kubernetes_secret" "github_runner_pat" {
  metadata {
    name      = "github-runner-pat"
    namespace = kubernetes_namespace.ci.metadata[0].name
  }
  data = {
    access_token = var.github_pat
  }
}

resource "kubernetes_service_account" "github_runner" {
  metadata {
    name      = "github-runner"
    namespace = kubernetes_namespace.ci.metadata[0].name
  }
}

# Admin RBAC + the default auto-mounted SA token means any workflow running on
# this runner can read every Secret in ci and lakehouse, including its own
# PAT. Accepted because only trusted workflows target the pi-cluster label
# (branch protection on main + fork-PR approval setting on the repo).
resource "kubernetes_role_binding" "github_runner_lakehouse" {
  metadata {
    name      = "github-runner-lakehouse-admin"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = "admin"
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.github_runner.metadata[0].name
    namespace = kubernetes_service_account.github_runner.metadata[0].namespace
  }
}

resource "kubernetes_role_binding" "github_runner_ci" {
  metadata {
    name      = "github-runner-ci-admin"
    namespace = kubernetes_namespace.ci.metadata[0].name
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = "admin"
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.github_runner.metadata[0].name
    namespace = kubernetes_service_account.github_runner.metadata[0].namespace
  }
}

# Cluster-scoped permissions for managing Helm charts that install ClusterRoles
# and ClusterRoleBindings (e.g. seaweedfs creates `seaweedfs-rw-cr`). Without
# this, `tofu apply` on the runner fails to upgrade those charts because Helm
# can't read the existing cluster-scoped resources to compute the diff.
#
# Narrower than full cluster-admin (no node/secret/pod-exec access), but does
# allow a malicious workflow to grant itself cluster-admin via a crafted
# ClusterRoleBinding.
resource "kubernetes_cluster_role" "github_runner_helm_cluster_scope" {
  metadata {
    name = "github-runner-helm-cluster-scope"
  }
  rule {
    api_groups = ["rbac.authorization.k8s.io"]
    resources  = ["clusterroles", "clusterrolebindings"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }
}

resource "kubernetes_cluster_role_binding" "github_runner_helm_cluster_scope" {
  metadata {
    name = "github-runner-helm-cluster-scope"
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.github_runner_helm_cluster_scope.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.github_runner.metadata[0].name
    namespace = kubernetes_service_account.github_runner.metadata[0].namespace
  }
}

# Read-only CRD discovery so the kubernetes_manifest provider can look up the
# GVK for arbitrary CRD-backed resources at plan time (Traefik IngressRoute,
# Spark Operator CRDs, etc.). Without this, every workflow run that touches a
# kubernetes_manifest fails at refresh with:
#   "customresourcedefinitions is forbidden ... cannot list ... at the cluster scope"
resource "kubernetes_cluster_role" "github_runner_crd_reader" {
  metadata {
    name = "github-runner-crd-reader"
  }
  rule {
    api_groups = ["apiextensions.k8s.io"]
    resources  = ["customresourcedefinitions"]
    verbs      = ["get", "list", "watch"]
  }
}

resource "kubernetes_cluster_role_binding" "github_runner_crd_reader" {
  metadata {
    name = "github-runner-crd-reader"
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.github_runner_crd_reader.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.github_runner.metadata[0].name
    namespace = kubernetes_service_account.github_runner.metadata[0].namespace
  }
}

# Full management of traefik.io CRD instances (IngressRoutes, middlewares,
# etc.) in the lakehouse namespace. The runner's `admin` ClusterRole binding
# covers built-in K8s resources but not arbitrary CRD instances — without
# this, `tofu plan` refresh of kubernetes_manifest.viz_ingressroute fails
# with "ingressroutes.traefik.io \"viz\" is forbidden" even after the
# cluster-scoped CRD discovery perm is granted.
#
# Scoped to lakehouse namespace since that's where our Traefik resources
# live. If we ever manage Traefik routes in other namespaces from this
# repo, mirror this Role + binding there.
resource "kubernetes_role" "github_runner_lakehouse_traefik" {
  metadata {
    name      = "github-runner-traefik"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
  rule {
    api_groups = ["traefik.io"]
    resources = [
      "ingressroutes",
      "ingressroutetcps",
      "ingressrouteudps",
      "middlewares",
      "middlewaretcps",
      "serverstransports",
      "tlsoptions",
      "tlsstores",
      "traefikservices",
    ]
    verbs = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }
}

resource "kubernetes_role_binding" "github_runner_lakehouse_traefik" {
  metadata {
    name      = "github-runner-traefik"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.github_runner_lakehouse_traefik.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.github_runner.metadata[0].name
    namespace = kubernetes_service_account.github_runner.metadata[0].namespace
  }
}

# patch/update on resourcequotas and limitranges in the lakehouse namespace.
# Kubernetes' built-in `admin` ClusterRole intentionally excludes these to
# stop a namespace admin from raising their own caps; the runner needs them
# to apply the namespace.tf and limit-range changes we make periodically.
resource "kubernetes_role" "github_runner_lakehouse_quota_patch" {
  metadata {
    name      = "github-runner-quota-patch"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
  rule {
    api_groups = [""]
    resources  = ["resourcequotas", "limitranges"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }
}

resource "kubernetes_role_binding" "github_runner_lakehouse_quota_patch" {
  metadata {
    name      = "github-runner-quota-patch"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.github_runner_lakehouse_quota_patch.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.github_runner.metadata[0].name
    namespace = kubernetes_service_account.github_runner.metadata[0].namespace
  }
}

resource "kubernetes_deployment" "github_runner" {
  metadata {
    name      = "github-runner"
    namespace = kubernetes_namespace.ci.metadata[0].name
  }
  spec {
    replicas = 1
    # Recreate (vs default RollingUpdate) prevents two pods registering with
    # the same RUNNER_NAME during rollouts: the myoung34 entrypoint's --replace
    # flag would evict the old registration mid-job otherwise.
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = {
        app = "github-runner"
      }
    }
    template {
      metadata {
        labels = {
          app = "github-runner"
        }
      }
      spec {
        service_account_name = kubernetes_service_account.github_runner.metadata[0].name

        container {
          name  = "runner"
          image = "myoung34/github-runner:2.335.1-ubuntu-jammy"

          env {
            name  = "REPO_URL"
            value = "https://github.com/cgoodfred/nhl-lakehouse"
          }
          env {
            name  = "RUNNER_NAME"
            value = "pi-cluster-runner"
          }
          env {
            name  = "LABELS"
            value = "pi-cluster,k8s"
          }
          env {
            name  = "RUNNER_WORKDIR"
            value = "/tmp/runner"
          }
          env {
            name  = "DISABLE_AUTO_UPDATE"
            value = "true"
          }
          env {
            name = "ACCESS_TOKEN"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.github_runner_pat.metadata[0].name
                key  = "access_token"
              }
            }
          }
        }
      }
    }
  }
}

# The runner builds the argo-workflow-runner Role in `lakehouse` (see
# infra/argo-workflows.tf). Kubernetes' RBAC privilege-escalation guard
# refuses to let it grant permissions it doesn't itself hold, so we have
# to hand the runner the same verbs on the same resources first via a
# local bootstrap apply. After this lands once, CI can apply the Argo
# resources cleanly.
#
# Three resource sets it didn't have before:
#   - pods/log (write verbs — the runner already has the read verbs via the
#     built-in admin ClusterRole)
#   - argoproj.io/workflowtaskresults (Argo's per-step status surface)
#   - sparkoperator.k8s.io/sparkapplications (the CRD the Workflow steps create)
resource "kubernetes_role" "github_runner_lakehouse_argo" {
  metadata {
    name      = "github-runner-argo"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
  rule {
    api_groups = [""]
    resources  = ["pods/log"]
    verbs      = ["create", "update", "patch", "delete"]
  }
  rule {
    api_groups = ["argoproj.io"]
    resources  = ["workflowtaskresults"]
    verbs      = ["create", "delete", "get", "list", "patch", "update", "watch"]
  }
  rule {
    api_groups = ["sparkoperator.k8s.io"]
    resources  = ["sparkapplications"]
    verbs      = ["create", "delete", "get", "list", "patch", "update", "watch"]
  }
}

resource "kubernetes_role_binding" "github_runner_lakehouse_argo" {
  metadata {
    name      = "github-runner-argo"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.github_runner_lakehouse_argo.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.github_runner.metadata[0].name
    namespace = kubernetes_service_account.github_runner.metadata[0].namespace
  }
}
