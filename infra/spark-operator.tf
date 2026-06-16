resource "helm_release" "spark_operator" {
  name       = "spark-operator"
  repository = "https://kubeflow.github.io/spark-operator"
  chart      = "spark-operator"
  version    = "2.5.1"
  namespace  = kubernetes_namespace.lakehouse.metadata[0].name
  values = [
    yamlencode({
      spark = {
        jobNamespaces = [
          kubernetes_namespace.lakehouse.metadata[0].name
        ]
      }
    })
  ]
}
