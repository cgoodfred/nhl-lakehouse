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

      # Run the db-migration Job as a regular resource (not a post-install hook).
      # Default `helmWait: false` annotates the Job as post-install, but the
      # catalog Deployment's check-db init container blocks until migrations
      # are applied, so the Deployment is never Ready and the post-install hook
      # never fires — chart install times out. helmWait: true runs the Job
      # alongside the Deployment so the init container can wait on it.
      helmWait = true

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
