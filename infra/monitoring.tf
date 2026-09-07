# The monitoring namespace predates this OpenTofu root. The import block adopts
# it without a delete/recreate cutover; deleting the namespace would also remove
# every legacy and replacement monitoring workload at once.
import {
  to = kubernetes_namespace.monitoring
  id = "monitoring"
}

resource "kubernetes_namespace" "monitoring" {
  metadata {
    name = "monitoring"
    labels = {
      "app.kubernetes.io/part-of"    = "nhl-lakehouse"
      "app.kubernetes.io/managed-by" = "opentofu"
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "kubernetes_resource_quota" "monitoring" {
  metadata {
    name      = "monitoring-quota"
    namespace = kubernetes_namespace.monitoring.metadata[0].name
  }

  spec {
    hard = {
      "requests.cpu"            = "3"
      "limits.cpu"              = "8"
      "requests.memory"         = "8Gi"
      "limits.memory"           = "16Gi"
      "persistentvolumeclaims"  = "10"
      "requests.storage"        = "30Gi"
      "count/deployments.apps"  = "20"
      "count/statefulsets.apps" = "10"
      "count/daemonsets.apps"   = "10"
      "count/services"          = "30"
      "count/configmaps"        = "50"
      "count/secrets"           = "30"
    }
  }
}

resource "kubernetes_limit_range" "monitoring" {
  metadata {
    name      = "monitoring-defaults"
    namespace = kubernetes_namespace.monitoring.metadata[0].name
  }

  spec {
    limit {
      type = "Container"
      default_request = {
        cpu    = "25m"
        memory = "64Mi"
      }
      default = {
        cpu    = "500m"
        memory = "1Gi"
      }
    }
  }
}

locals {
  monitoring_namespace = kubernetes_namespace.monitoring.metadata[0].name
  monitoring_labels = {
    "app.kubernetes.io/part-of"    = "nhl-lakehouse"
    "app.kubernetes.io/managed-by" = "opentofu"
  }
  monitoring_s3_endpoint = "seaweedfs-s3.lakehouse.svc.cluster.local:8333"
}
