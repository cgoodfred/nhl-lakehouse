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
      "requests.cpu"    = "8"
      "requests.memory" = "16Gi"
      "limits.cpu"      = "8"
      "limits.memory"   = "16Gi"

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
