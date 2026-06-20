resource "kubernetes_secret" "seaweedfs_s3_config" {
  metadata {
    name      = "seaweedfs-s3-config"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }

  data = {
    seaweedfs_s3_config = jsonencode({
      identities = [
        {
          name = "admin"
          credentials = [
            {
              accessKey = var.s3_access_key
              secretKey = var.s3_secret_key
            }
          ]
          actions = ["Admin"]
        }
      ]
    })
  }
}

resource "kubernetes_secret" "ingest_s3_creds" {
  metadata {
    name      = "ingest-s3-creds"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
  data = {
    access_key = var.s3_access_key
    secret_key = var.s3_secret_key
  }
}
