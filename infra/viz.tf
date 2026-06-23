# Streamlit viz app — reads gold tables from the Iceberg catalog and
# renders an NHL goal map. HTTP-only at the Ingress; TLS is terminated
# at Cloudflare Tunnel.

resource "kubernetes_deployment" "viz" {
  metadata {
    name      = "viz"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
    labels = {
      app = "viz"
    }
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "viz"
      }
    }
    template {
      metadata {
        labels = {
          app = "viz"
        }
      }
      spec {
        container {
          name              = "viz"
          image             = "ghcr.io/cgoodfred/nhl-lakehouse/viz:latest"
          image_pull_policy = "Always"
          port {
            container_port = 8501
            name           = "http"
          }

          # Lakekeeper catalog — pod talks to it via in-cluster service DNS.
          env {
            name  = "LAKEKEEPER_URI"
            value = "http://lakekeeper.lakehouse.svc.cluster.local:8181/catalog"
          }
          env {
            name  = "LAKEKEEPER_WAREHOUSE"
            value = "nhl"
          }
          env {
            name  = "LAKEKEEPER_SCOPE"
            value = "lakekeeper"
          }
          # Keycloak token endpoint — public URL so the JWT issuer matches
          # what Lakekeeper validates against.
          env {
            name  = "LAKEKEEPER_OAUTH2_SERVER_URI"
            value = "${var.keycloak_issuer_url}/protocol/openid-connect/token"
          }
          env {
            name = "LAKEKEEPER_CLIENT_ID"
            value_from {
              secret_key_ref {
                name = "lakekeeper-client-secret"
                key  = "client-id"
              }
            }
          }
          env {
            name = "LAKEKEEPER_CLIENT_SECRET"
            value_from {
              secret_key_ref {
                name = "lakekeeper-client-secret"
                key  = "client-secret"
              }
            }
          }

          # SeaweedFS S3 access — pod talks to it via in-cluster service DNS.
          env {
            name  = "S3_ENDPOINT"
            value = "http://seaweedfs-s3.lakehouse.svc.cluster.local:8333"
          }
          env {
            name = "S3_ACCESS_KEY"
            value_from {
              secret_key_ref {
                name = "ingest-s3-creds"
                key  = "access_key"
              }
            }
          }
          env {
            name = "S3_SECRET_KEY"
            value_from {
              secret_key_ref {
                name = "ingest-s3-creds"
                key  = "secret_key"
              }
            }
          }

          readiness_probe {
            http_get {
              path = "/_stcore/health"
              port = 8501
            }
            initial_delay_seconds = 10
            period_seconds        = 10
          }
          liveness_probe {
            http_get {
              path = "/_stcore/health"
              port = 8501
            }
            initial_delay_seconds = 30
            period_seconds        = 30
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "256Mi"
            }
            limits = {
              cpu    = "1000m"
              memory = "1Gi"
            }
          }
        }
      }
    }
  }
  depends_on = [
    kubernetes_secret.ingest_s3_creds,
  ]
}

resource "kubernetes_service" "viz" {
  metadata {
    name      = "viz"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
  spec {
    selector = {
      app = "viz"
    }
    port {
      name        = "http"
      port        = 8501
      target_port = 8501
    }
    type = "ClusterIP"
  }
}

resource "kubernetes_ingress_v1" "viz" {
  metadata {
    name      = "viz"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
    annotations = {
      # Traefik routes plain HTTP; Cloudflare Tunnel terminates TLS at the edge.
      "traefik.ingress.kubernetes.io/router.entrypoints" = "web"
    }
  }
  spec {
    ingress_class_name = "traefik"
    rule {
      host = "nhl.cluster.cgood.dev"
      http {
        path {
          path      = "/"
          path_type = "Prefix"
          backend {
            service {
              name = kubernetes_service.viz.metadata[0].name
              port {
                number = 8501
              }
            }
          }
        }
      }
    }
  }
}
