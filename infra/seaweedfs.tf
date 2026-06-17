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
        nodeSelector = <<-EOT
          kubernetes.io/hostname: pi-master
          EOT
      }

      filer = {
        data = {
          type         = "persistentVolumeClaim"
          size         = "20Gi"
          storageClass = "local-path"
        }
        nodeSelector = <<-EOT
          kubernetes.io/hostname: pi-node-one
          EOT
      }

      volume = {
        dataDirs = [
          {
            name         = "data1"
            type         = "persistentVolumeClaim"
            size         = "100Gi"
            storageClass = "local-path"
            maxVolumes   = 100
          }
        ]
        nodeSelector = <<-EOT
          kubernetes.io/hostname: pi-node-two
          EOT
      }

      s3 = {
        enabled              = true
        enableAuth           = true
        existingConfigSecret = kubernetes_secret.seaweedfs_s3_config.metadata[0].name
        createBuckets = [
          { name = "nhl-bronze" },
          { name = "nhl-silver" },
          { name = "nhl-gold" },
        ]
      }
    })
  ]
}
