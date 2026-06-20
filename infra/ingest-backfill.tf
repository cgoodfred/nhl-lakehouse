resource "kubernetes_job_v1" "ingest_backfill" {
  count = var.backfill_season != "" ? 1 : 0

  # Don't block tofu apply on backfill duration (~5-7 min per season). Job
  # success/failure is observable via kubectl logs and the failure manifest
  # in S3; coupling terraform state to runtime is the wrong layering.
  wait_for_completion = false

  metadata {
    name      = "ingest-backfill-${var.backfill_season}"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
    labels    = { app = "ingest", role = "backfill" }
  }

  spec {
    ttl_seconds_after_finished = 86400 # auto-delete 24h after completion
    backoff_limit              = 0     # ingest exits 1 on per-game failures; manifest captures them, no point retrying the whole pod

    template {
      metadata {
        labels = { app = "ingest", role = "backfill" }
      }
      spec {
        restart_policy = "Never"
        container {
          name  = "ingest"
          image = "ghcr.io/cgoodfred/nhl-lakehouse/ingest:latest"

          args = [
            "--season=${var.backfill_season}",
            "--s3-endpoint=http://seaweedfs-s3.lakehouse.svc.cluster.local:8333",
            "--s3-bucket=nhl-bronze",
          ]

          env {
            name = "AWS_ACCESS_KEY_ID"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.ingest_s3_creds.metadata[0].name
                key  = "access_key"
              }
            }
          }
          env {
            name = "AWS_SECRET_ACCESS_KEY"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.ingest_s3_creds.metadata[0].name
                key  = "secret_key"
              }
            }
          }

          resources {
            requests = { cpu = "200m", memory = "128Mi" }
            limits   = { cpu = "1", memory = "512Mi" }
          }
        }
      }
    }
  }
}
