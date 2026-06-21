# RBAC for Spark drivers running silver/gold transformations. The driver pod
# needs to create executor pods + services + read pod logs; the namespace
# default ServiceAccount lacks these permissions. Narrower than full namespace
# admin (no secrets/exec/configmaps write).
resource "kubernetes_service_account" "silver_spark" {
  metadata {
    name      = "silver-spark"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
}

resource "kubernetes_role" "silver_spark" {
  metadata {
    name      = "silver-spark"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "services", "configmaps", "persistentvolumeclaims"]
    verbs      = ["*"]
  }

  rule {
    api_groups = [""]
    resources  = ["pods/log", "pods/exec"]
    verbs      = ["get", "list", "create"]
  }
}

resource "kubernetes_role_binding" "silver_spark" {
  metadata {
    name      = "silver-spark"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.silver_spark.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.silver_spark.metadata[0].name
    namespace = kubernetes_service_account.silver_spark.metadata[0].namespace
  }
}
