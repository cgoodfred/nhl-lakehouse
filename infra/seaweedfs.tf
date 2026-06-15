resource "helm_release" "seaweedfs" {
  name       = "seaweedfs"
  repository = "https://seaweedfs.github.io/seaweedfs/helm"
  chart      = "seaweedfs"
  version    = "4.33.0"
  namespace  = kubernetes_namespace.lakehouse.metadata[0].name
  values = [
    yamlencode({
      master = {
        data = {
          type         = "persistentVolumeClaim"
          size         = "5Gi"
          storageClass = "local-path"
        }
      }

      filer = {
        data = {
          type         = "persistentVolumeClaim"
          size         = "20Gi"
          storageClass = "local-path"
        }
      }

      volume = {
        dataDirs = [
          {
            name         = "data1"
            type         = "persistentVolumeClaim"
            size         = "100Gi"
            storageClass = "local-path"
          }
        ]
      }

      s3 = {
        enabled    = true
        enableAuth = true
        credentials = {
          admin = {
            accessKey = var.s3_access_key
            secretKey = var.s3_secret_key
          }
        }
        createBuckets = [
          { name = "nhl-bronze" },
          { name = "nhl-silver" },
          { name = "nhl-gold" },
        ]
      }
    })
  ]
}
