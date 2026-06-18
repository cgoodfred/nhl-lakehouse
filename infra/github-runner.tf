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
