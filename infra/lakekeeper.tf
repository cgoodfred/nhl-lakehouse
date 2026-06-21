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
