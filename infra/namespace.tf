resource "kubernetes_namespace" "lakehouse" {
  metadata {
    name = "lakehouse"
  }
}

resource "kubernetes_resource_quota" "lakehouse_quota" {
  metadata {
    name      = "lakehouse-quota"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }

  spec {
    hard = {
      "requests.cpu"    = "10"
      "requests.memory" = "32Gi"
      "limits.cpu"      = "10"
      "limits.memory"   = "32Gi"

      "persistentvolumeclaims" = "10"
      "requests.storage"       = "400Gi"
    }
  }
}

resource "kubernetes_limit_range" "lakehouse_default" {
  metadata {
    name      = "lakehouse-default"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }

  spec {
    limit {
      type = "Container"
      default_request = {
        cpu    = "100m"
        memory = "256Mi"
      }
      default = {
        cpu    = "500m"
        memory = "1Gi"
      }
    }
  }
}
