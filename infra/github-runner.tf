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
