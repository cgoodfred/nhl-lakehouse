locals {
  mimir_config = templatefile("${path.module}/monitoring/mimir.yaml.tftpl", {
    s3_endpoint = local.monitoring_s3_endpoint
  })
}

resource "kubernetes_config_map" "monitoring_mimir" {
  metadata {
    name      = "monitoring-mimir-config"
    namespace = local.monitoring_namespace
    labels    = local.monitoring_labels
  }

  data = {
    "mimir.yaml" = local.mimir_config
  }
}

resource "kubernetes_config_map" "monitoring_mimir_rules" {
  metadata {
    name      = "monitoring-mimir-rules"
    namespace = local.monitoring_namespace
    labels    = local.monitoring_labels
  }

  data = {
    "cluster.yaml" = file("${path.module}/monitoring/alerts/cluster.yaml")
  }
}

resource "kubernetes_persistent_volume_claim" "monitoring_mimir" {
  # local-path uses WaitForFirstConsumer. Waiting here deadlocks because the
  # Mimir Deployment is the consumer and depends on this PVC resource.
  wait_until_bound = false

  metadata {
    name      = "monitoring-mimir-data"
    namespace = local.monitoring_namespace
    labels    = local.monitoring_labels
  }

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = "local-path"
    resources {
      requests = {
        storage = "5Gi"
      }
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "kubernetes_deployment" "monitoring_mimir" {
  metadata {
    name      = "monitoring-mimir"
    namespace = local.monitoring_namespace
    labels = merge(local.monitoring_labels, {
      "app.kubernetes.io/name"      = "mimir"
      "app.kubernetes.io/instance"  = "monitoring-mimir"
      "app.kubernetes.io/component" = "metrics"
    })
  }

  spec {
    replicas = 1
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = {
        "app.kubernetes.io/instance" = "monitoring-mimir"
      }
    }
    template {
      metadata {
        labels = merge(local.monitoring_labels, {
          "app.kubernetes.io/name"      = "mimir"
          "app.kubernetes.io/instance"  = "monitoring-mimir"
          "app.kubernetes.io/component" = "metrics"
        })
        annotations = {
          "checksum/config"      = sha256(local.mimir_config)
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "9009"
          "prometheus.io/path"   = "/metrics"
        }
      }
      spec {
        security_context {
          fs_group = 10001
        }
        container {
          name              = "mimir"
          image             = "grafana/mimir:3.2.0"
          image_pull_policy = "IfNotPresent"
          args = [
            "-config.file=/etc/mimir/mimir.yaml",
            "-config.expand-env=true",
            "-target=all",
          ]

          port {
            name           = "http-metrics"
            container_port = 9009
          }

          env_from {
            secret_ref {
              name = kubernetes_secret.monitoring_object_store.metadata[0].name
            }
          }

          resources {
            requests = {
              cpu    = "250m"
              memory = "512Mi"
            }
            limits = {
              cpu    = "1500m"
              memory = "2Gi"
            }
          }

          readiness_probe {
            http_get {
              path = "/ready"
              port = "http-metrics"
            }
            initial_delay_seconds = 20
            period_seconds        = 10
            timeout_seconds       = 5
            failure_threshold     = 6
          }

          liveness_probe {
            http_get {
              path = "/ready"
              port = "http-metrics"
            }
            initial_delay_seconds = 60
            period_seconds        = 20
            timeout_seconds       = 5
            failure_threshold     = 6
          }

          volume_mount {
            name       = "config"
            mount_path = "/etc/mimir"
            read_only  = true
          }
          volume_mount {
            name       = "rules"
            mount_path = "/etc/mimir/rules"
            read_only  = true
          }
          volume_mount {
            name       = "data"
            mount_path = "/data"
          }
        }

        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.monitoring_mimir.metadata[0].name
          }
        }
        volume {
          name = "rules"
          config_map {
            name = kubernetes_config_map.monitoring_mimir_rules.metadata[0].name
            items {
              key  = "cluster.yaml"
              path = "anonymous/cluster.yaml"
            }
          }
        }
        volume {
          name = "data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.monitoring_mimir.metadata[0].name
          }
        }
      }
    }
  }

  depends_on = [helm_release.seaweedfs]
}

resource "kubernetes_service" "monitoring_mimir" {
  metadata {
    name      = "monitoring-mimir"
    namespace = local.monitoring_namespace
    labels    = local.monitoring_labels
  }
  spec {
    selector = {
      "app.kubernetes.io/instance" = "monitoring-mimir"
    }
    port {
      name        = "http-metrics"
      port        = 9009
      target_port = "http-metrics"
    }
    type = "ClusterIP"
  }
}
