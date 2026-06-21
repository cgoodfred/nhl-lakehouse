resource "random_password" "lakekeeper_encryption_key" {
  length  = 32
  special = false
}

resource "kubernetes_secret" "lakekeeper_encryption_key" {
  metadata {
    name      = "lakekeeper-encryption-key"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
  data = {
    encryption-key = random_password.lakekeeper_encryption_key.result
  }
}

resource "helm_release" "lakekeeper" {
  name       = "lakekeeper"
  repository = "https://lakekeeper.github.io/lakekeeper-charts"
  chart      = "lakekeeper"
  version    = "0.11.0"
  namespace  = kubernetes_namespace.lakehouse.metadata[0].name

  values = [
    yamlencode({
      # Disable the embedded postgres; we use the Bitnami chart instead.
      postgresql = {
        enabled = false
      }

      externalDatabase = {
        type              = "postgres"
        host_read         = "lakekeeper-pg-postgresql.${kubernetes_namespace.lakehouse.metadata[0].name}.svc.cluster.local"
        host_write        = "lakekeeper-pg-postgresql.${kubernetes_namespace.lakehouse.metadata[0].name}.svc.cluster.local"
        port              = 5432
        database          = "lakekeeper"
        user              = "lakekeeper"
        userSecret        = kubernetes_secret.lakekeeper_pg.metadata[0].name
        userSecretKey     = "postgresql-user"
        passwordSecret    = kubernetes_secret.lakekeeper_pg.metadata[0].name
        passwordSecretKey = "postgresql-password"
      }

      auth = {
        oauth2 = {
          providerUri = var.keycloak_issuer_url
          audience    = "lakekeeper"
        }
      }

      catalog = {
        image = {
          # Pin to a specific version rather than latest for reproducibility.
          # Bump along with the chart version.
          tag = "v0.11.0"
        }
        config = {
          LAKEKEEPER__BASE_URI = "http://lakekeeper.${kubernetes_namespace.lakehouse.metadata[0].name}.svc.cluster.local:8181"
        }
        resources = {
          requests = {
            cpu    = "50m"
            memory = "128Mi"
          }
          limits = {
            cpu    = "500m"
            memory = "512Mi"
          }
        }
      }
    })
  ]

  depends_on = [helm_release.lakekeeper_pg]
}
