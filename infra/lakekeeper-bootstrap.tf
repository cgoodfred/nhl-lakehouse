resource "kubernetes_secret" "lakekeeper_client_secret" {
  metadata {
    name      = "lakekeeper-client-secret"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
  data = {
    client-id     = var.lakekeeper_spark_client_id
    client-secret = var.lakekeeper_spark_client_secret
  }
}

# One-shot Job: waits for Lakekeeper to be ready, fetches an OIDC token from
# Keycloak, calls /bootstrap, then creates the `nhl` warehouse pointed at the
# nhl-warehouse SeaweedFS bucket. Idempotent: 409 from bootstrap (already done)
# and 409 from warehouse create (already exists) are both treated as success.
resource "kubernetes_job_v1" "lakekeeper_bootstrap" {
  metadata {
    name      = "lakekeeper-bootstrap"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
    labels    = { app = "lakekeeper", role = "bootstrap" }
  }

  spec {
    ttl_seconds_after_finished = 86400
    backoff_limit              = 5

    template {
      metadata {
        labels = { app = "lakekeeper", role = "bootstrap" }
      }
      spec {
        restart_policy = "OnFailure"
        container {
          name    = "bootstrap"
          image   = "alpine:3.20"
          command = ["sh", "-c"]
          args = [<<-EOT
            set -eu
            apk add --no-cache curl jq > /dev/null

            LAKEKEEPER_URL="http://lakekeeper.${kubernetes_namespace.lakehouse.metadata[0].name}.svc.cluster.local:8181"
            TOKEN_URL="${var.keycloak_issuer_url}/protocol/openid-connect/token"

            echo "Waiting for Lakekeeper /health..."
            until curl -sf "$LAKEKEEPER_URL/health" > /dev/null 2>&1; do
              sleep 5
            done
            echo "Lakekeeper is up."

            echo "Fetching OIDC token from $TOKEN_URL..."
            TOKEN=$(curl -sf -X POST "$TOKEN_URL" \
              -d "grant_type=client_credentials" \
              -d "client_id=$CLIENT_ID" \
              -d "client_secret=$CLIENT_SECRET" \
              | jq -r .access_token)
            if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
              echo "Failed to obtain token from Keycloak"
              exit 1
            fi
            echo "Token obtained."

            echo "Calling /management/v1/bootstrap..."
            BOOT_STATUS=$(curl -s -o /tmp/boot.out -w "%%{http_code}" -X POST \
              "$LAKEKEEPER_URL/management/v1/bootstrap" \
              -H "Authorization: Bearer $TOKEN" \
              -H "Content-Type: application/json" \
              -d '{"accept-terms-of-use": true}')
            echo "Bootstrap HTTP $BOOT_STATUS:"
            cat /tmp/boot.out || true
            echo
            if [ "$BOOT_STATUS" != "201" ] && [ "$BOOT_STATUS" != "204" ] && [ "$BOOT_STATUS" != "409" ]; then
              echo "Bootstrap failed."
              exit 1
            fi

            echo "Creating warehouse 'nhl'..."
            WH_BODY=$(jq -n \
              --arg bucket "nhl-warehouse" \
              --arg endpoint "http://seaweedfs-s3.${kubernetes_namespace.lakehouse.metadata[0].name}.svc.cluster.local:8333" \
              --arg ak "$AWS_ACCESS_KEY_ID" \
              --arg sk "$AWS_SECRET_ACCESS_KEY" \
              '{
                "warehouse-name": "nhl",
                "project-id": "00000000-0000-0000-0000-000000000000",
                "storage-profile": {
                  "type": "s3",
                  "bucket": $bucket,
                  "key-prefix": "",
                  "endpoint": $endpoint,
                  "region": "us-east-1",
                  "path-style-access": true,
                  "flavor": "minio",
                  "sts-enabled": false
                },
                "storage-credential": {
                  "type": "s3",
                  "credential-type": "access-key",
                  "aws-access-key-id": $ak,
                  "aws-secret-access-key": $sk
                }
              }')

            WH_STATUS=$(curl -s -o /tmp/wh.out -w "%%{http_code}" -X POST \
              "$LAKEKEEPER_URL/management/v1/warehouse" \
              -H "Authorization: Bearer $TOKEN" \
              -H "Content-Type: application/json" \
              -d "$WH_BODY")
            echo "Warehouse HTTP $WH_STATUS:"
            cat /tmp/wh.out || true
            echo
            if [ "$WH_STATUS" != "201" ] && [ "$WH_STATUS" != "200" ] && [ "$WH_STATUS" != "409" ]; then
              echo "Warehouse creation failed."
              exit 1
            fi

            echo "Bootstrap complete."
          EOT
          ]

          env {
            name = "CLIENT_ID"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.lakekeeper_client_secret.metadata[0].name
                key  = "client-id"
              }
            }
          }
          env {
            name = "CLIENT_SECRET"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.lakekeeper_client_secret.metadata[0].name
                key  = "client-secret"
              }
            }
          }
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
            requests = { cpu = "50m", memory = "64Mi" }
            limits   = { cpu = "200m", memory = "128Mi" }
          }
        }
      }
    }
  }

  wait_for_completion = false

  depends_on = [helm_release.lakekeeper]
}
