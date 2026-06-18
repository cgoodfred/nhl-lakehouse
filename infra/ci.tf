resource "kubernetes_namespace" "ci" {
  metadata {
    name = "ci"
    labels = {
      "app.kubernetes.io/part-of" = "github-runner"
    }
  }
}

resource "kubernetes_resource_quota" "ci_quota" {
  metadata {
    name      = "ci-quota"
    namespace = kubernetes_namespace.ci.metadata[0].name
  }
  spec {
    hard = {
      "requests.cpu"    = "2"
      "requests.memory" = "4Gi"
      "limits.cpu"      = "2"
      "limits.memory"   = "4Gi"
    }
  }
}

resource "kubernetes_limit_range" "ci_defaults" {
  metadata {
    name      = "ci-defaults"
    namespace = kubernetes_namespace.ci.metadata[0].name
  }
  spec {
    limit {
      type = "Container"
      default_request = {
        cpu    = "100m"
        memory = "256Mi"
      }
      default = {
        cpu    = "1"
        memory = "2Gi"
      }
    }
  }
}
