resource "helm_release" "monitoring_loki" {
  name       = "monitoring-loki"
  repository = "https://grafana-community.github.io/helm-charts"
  chart      = "loki"
  version    = "18.12.1"
  namespace  = local.monitoring_namespace

  atomic  = true
  lint    = true
  wait    = true
  timeout = 900

  values = [
    yamlencode({
      fullnameOverride = "monitoring-loki"
      deploymentMode   = "Monolithic"

      # The chart enables a rules sidecar by default and otherwise grants it
      # cluster-wide ConfigMap and Secret reads. Rules live in Mimir here, so
      # disable the unused sidecar and keep any chart RBAC namespace-scoped.
      rbac = {
        namespaced = true
      }
      sidecar = {
        rules = {
          enabled = false
        }
      }

      loki = {
        auth_enabled = false
        commonConfig = {
          replication_factor = 1
        }
        schemaConfig = {
          configs = [{
            from         = "2024-04-01"
            store        = "tsdb"
            object_store = "s3"
            schema       = "v13"
            index = {
              prefix = "index_"
              period = "24h"
            }
          }]
        }
        storage = {
          type = "s3"
          bucketNames = {
            chunks = "monitoring-loki"
            ruler  = "monitoring-loki"
          }
          s3 = {
            endpoint          = local.monitoring_s3_endpoint
            region            = "us-east-1"
            accessKeyId       = "$${S3_ACCESS_KEY}"
            secretAccessKey   = "$${S3_SECRET_KEY}"
            s3ForcePathStyle  = true
            insecure          = true
            disable_dualstack = true
          }
        }
        limits_config = {
          allow_structured_metadata  = true
          retention_period           = "336h"
          reject_old_samples         = true
          reject_old_samples_max_age = "168h"
          ingestion_rate_mb          = 4
          ingestion_burst_size_mb    = 8
          max_query_parallelism      = 4
        }
        compactor = {
          retention_enabled    = true
          delete_request_store = "s3"
        }
        analytics = {
          reporting_enabled = false
        }
        pattern_ingester = {
          enabled = false
        }
      }

      singleBinary = {
        replicas  = 1
        extraArgs = ["-config.expand-env=true"]
        extraEnvFrom = [{
          secretRef = {
            name = kubernetes_secret.monitoring_object_store.metadata[0].name
          }
        }]
        resources = {
          requests = {
            cpu    = "200m"
            memory = "512Mi"
          }
          limits = {
            cpu    = "1"
            memory = "1536Mi"
          }
        }
        persistence = {
          enabled      = true
          storageClass = "local-path"
          size         = "5Gi"
          whenDeleted  = "Retain"
          whenScaled   = "Retain"
        }
        podAnnotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "3100"
          "prometheus.io/path"   = "/metrics"
        }
      }

      gateway = {
        enabled = false
      }
      lokiCanary = {
        enabled = false
      }
      test = {
        # The chart test queries the canary and fails template validation when
        # the canary is disabled, so these settings must move together.
        enabled = false
      }
      resultsCache = {
        enabled = false
      }
      chunksCache = {
        enabled = false
      }
      minio = {
        enabled = false
      }
      rollout_operator = {
        enabled = false
      }

      backend         = { replicas = 0 }
      read            = { replicas = 0 }
      write           = { replicas = 0 }
      ingester        = { replicas = 0 }
      querier         = { replicas = 0 }
      queryFrontend   = { replicas = 0 }
      queryScheduler  = { replicas = 0 }
      distributor     = { replicas = 0 }
      compactor       = { replicas = 0 }
      indexGateway    = { replicas = 0 }
      bloomPlanner    = { replicas = 0 }
      bloomBuilder    = { replicas = 0 }
      bloomGateway    = { replicas = 0 }
      patternIngester = { replicas = 0 }
    })
  ]

  depends_on = [helm_release.seaweedfs]
}
