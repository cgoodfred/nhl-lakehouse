resource "random_password" "lakekeeper_pg" {
  length  = 24
  special = false
}

# Single secret holds the postgres password under multiple key names so both
# the Bitnami chart (postgres-password, password) and the Lakekeeper chart's
# externalDatabase config (postgresql-user, postgresql-password) can reference
# it without duplication.
resource "kubernetes_secret" "lakekeeper_pg" {
  metadata {
    name      = "lakekeeper-pg-creds"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
  data = {
    postgres-password   = random_password.lakekeeper_pg.result
    password            = random_password.lakekeeper_pg.result
    postgresql-user     = "lakekeeper"
    postgresql-password = random_password.lakekeeper_pg.result
  }
}

resource "helm_release" "lakekeeper_pg" {
  name       = "lakekeeper-pg"
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "postgresql"
  version    = "16.7.27"
  namespace  = kubernetes_namespace.lakehouse.metadata[0].name

  values = [
    yamlencode({
      # As of mid-2025, Bitnami's free postgres images moved from `bitnami/`
      # to `bitnamilegacy/` (the `bitnami/` namespace now holds paid Premium
      # builds). The chart still defaults to `bitnami/postgresql:<tag>` which
      # 404s on Docker Hub. Override to bitnamilegacy/.
      image = {
        repository = "bitnamilegacy/postgresql"
      }

      auth = {
        database       = "lakekeeper"
        username       = "lakekeeper"
        existingSecret = kubernetes_secret.lakekeeper_pg.metadata[0].name
        secretKeys = {
          adminPasswordKey       = "postgres-password"
          userPasswordKey        = "password"
          replicationPasswordKey = "password"
        }
      }
      primary = {
        persistence = {
          size         = "2Gi"
          storageClass = "local-path"
        }
        resources = {
          requests = {
            cpu    = "100m"
            memory = "256Mi"
          }
          limits = {
            cpu    = "500m"
            memory = "512Mi"
          }
        }
      }
    })
  ]
}
