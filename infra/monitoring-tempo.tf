resource "helm_release" "monitoring_tempo" {
  name       = "monitoring-tempo"
  repository = "https://grafana-community.github.io/helm-charts"
  chart      = "tempo"
  version    = "2.3.0"
  namespace  = local.monitoring_namespace

  atomic  = true
  lint    = true
  wait    = true
  timeout = 900

  values = [
    yamlencode({
      fullnameOverride = "monitoring-tempo"
      replicas         = 1

      tempo = {
        tag               = "2.10.8"
        reportingEnabled  = false
        memBallastSizeMbs = 128
        retention         = "168h"
        resources = {
          requests = {
            cpu    = "100m"
            memory = "256Mi"
          }
          limits = {
            cpu    = "500m"
            memory = "1Gi"
          }
        }
        metricsGenerator = {
          enabled = false
        }
        querier = {
          max_concurrent_queries = 4
        }
        queryFrontend = {
          search = {
            concurrent_jobs = 100
          }
        }
        storage = {
          trace = {
            backend = "s3"
            s3 = {
              bucket         = "monitoring-tempo"
              endpoint       = local.monitoring_s3_endpoint
              access_key     = "$${S3_ACCESS_KEY}"
              secret_key     = "$${S3_SECRET_KEY}"
              insecure       = true
              forcepathstyle = true
              region         = "us-east-1"
            }
            wal = {
              path = "/var/tempo/wal"
            }
          }
        }
        receivers = {
          otlp = {
            protocols = {
              grpc = { endpoint = "0.0.0.0:4317" }
              http = { endpoint = "0.0.0.0:4318" }
            }
          }
        }
        extraArgs = {
          "config.expand-env" = "true"
        }
        extraEnvFrom = [{
          secretRef = {
            name = kubernetes_secret.monitoring_object_store.metadata[0].name
          }
        }]
      }

      persistence = {
        enabled                        = true
        enableStatefulSetAutoDeletePVC = false
        storageClassName               = "local-path"
        accessModes                    = ["ReadWriteOnce"]
        size                           = "5Gi"
      }

      podAnnotations = {
        "prometheus.io/scrape" = "true"
        "prometheus.io/port"   = "3200"
        "prometheus.io/path"   = "/metrics"
      }

      serviceMonitor = {
        enabled = false
      }
    })
  ]

  depends_on = [helm_release.seaweedfs]
}
