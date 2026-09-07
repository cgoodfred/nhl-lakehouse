resource "kubernetes_secret" "monitoring_lakekeeper_postgres_exporter" {
  metadata {
    name      = "monitoring-lakekeeper-postgres-exporter"
    namespace = local.monitoring_namespace
    labels    = local.monitoring_labels
  }

  data = {
    DATA_SOURCE_NAME = "postgresql://lakekeeper:${random_password.lakekeeper_pg.result}@lakekeeper-pg-postgresql.lakehouse.svc.cluster.local:5432/lakekeeper?sslmode=disable"
  }
}

resource "kubernetes_secret" "monitoring_argo_postgres_exporter" {
  metadata {
    name      = "monitoring-argo-postgres-exporter"
    namespace = local.monitoring_namespace
    labels    = local.monitoring_labels
  }

  data = {
    DATA_SOURCE_NAME = "postgresql://argo:${random_password.argo_pg.result}@argo-workflows-pg-postgresql.lakehouse.svc.cluster.local:5432/argo?sslmode=disable"
  }
}

resource "kubernetes_deployment" "monitoring_lakekeeper_postgres_exporter" {
  metadata {
    name      = "monitoring-lakekeeper-postgres-exporter"
    namespace = local.monitoring_namespace
    labels = merge(local.monitoring_labels, {
      "app.kubernetes.io/name"     = "postgres-exporter"
      "app.kubernetes.io/instance" = "lakekeeper-postgres"
    })
  }

  spec {
    replicas = 1
    selector {
      match_labels = {
        "app.kubernetes.io/instance" = "lakekeeper-postgres"
      }
    }
    template {
      metadata {
        labels = merge(local.monitoring_labels, {
          "app.kubernetes.io/name"     = "postgres-exporter"
          "app.kubernetes.io/instance" = "lakekeeper-postgres"
        })
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "9187"
          "prometheus.io/path"   = "/metrics"
        }
      }
      spec {
        container {
          name              = "postgres-exporter"
          image             = "quay.io/prometheuscommunity/postgres-exporter:v0.18.1"
          image_pull_policy = "IfNotPresent"
          port {
            name           = "metrics"
            container_port = 9187
          }
          env_from {
            secret_ref {
              name = kubernetes_secret.monitoring_lakekeeper_postgres_exporter.metadata[0].name
            }
          }
          resources {
            requests = { cpu = "10m", memory = "32Mi" }
            limits   = { cpu = "100m", memory = "128Mi" }
          }
          readiness_probe {
            http_get {
              path = "/-/ready"
              port = "metrics"
            }
            initial_delay_seconds = 5
            period_seconds        = 10
          }
          liveness_probe {
            http_get {
              path = "/-/healthy"
              port = "metrics"
            }
            initial_delay_seconds = 15
            period_seconds        = 20
          }
        }
      }
    }
  }
}

resource "kubernetes_deployment" "monitoring_argo_postgres_exporter" {
  metadata {
    name      = "monitoring-argo-postgres-exporter"
    namespace = local.monitoring_namespace
    labels = merge(local.monitoring_labels, {
      "app.kubernetes.io/name"     = "postgres-exporter"
      "app.kubernetes.io/instance" = "argo-postgres"
    })
  }

  spec {
    replicas = 1
    selector {
      match_labels = {
        "app.kubernetes.io/instance" = "argo-postgres"
      }
    }
    template {
      metadata {
        labels = merge(local.monitoring_labels, {
          "app.kubernetes.io/name"     = "postgres-exporter"
          "app.kubernetes.io/instance" = "argo-postgres"
        })
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "9187"
          "prometheus.io/path"   = "/metrics"
        }
      }
      spec {
        container {
          name              = "postgres-exporter"
          image             = "quay.io/prometheuscommunity/postgres-exporter:v0.18.1"
          image_pull_policy = "IfNotPresent"
          port {
            name           = "metrics"
            container_port = 9187
          }
          env_from {
            secret_ref {
              name = kubernetes_secret.monitoring_argo_postgres_exporter.metadata[0].name
            }
          }
          resources {
            requests = { cpu = "10m", memory = "32Mi" }
            limits   = { cpu = "100m", memory = "128Mi" }
          }
          readiness_probe {
            http_get {
              path = "/-/ready"
              port = "metrics"
            }
            initial_delay_seconds = 5
            period_seconds        = 10
          }
          liveness_probe {
            http_get {
              path = "/-/healthy"
              port = "metrics"
            }
            initial_delay_seconds = 15
            period_seconds        = 20
          }
        }
      }
    }
  }
}
