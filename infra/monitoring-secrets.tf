resource "random_password" "grafana_admin" {
  length  = 24
  special = false
}

resource "kubernetes_secret" "monitoring_object_store" {
  metadata {
    name      = "monitoring-object-store-creds"
    namespace = local.monitoring_namespace
    labels    = local.monitoring_labels
  }

  data = {
    S3_ACCESS_KEY = var.s3_access_key
    S3_SECRET_KEY = var.s3_secret_key
  }
}

resource "kubernetes_secret" "monitoring_grafana_admin" {
  metadata {
    name      = "monitoring-grafana-admin"
    namespace = local.monitoring_namespace
    labels    = local.monitoring_labels
  }

  data = {
    admin-user     = "cgood"
    admin-password = random_password.grafana_admin.result
  }
}

resource "kubernetes_secret" "monitoring_grafana_oidc" {
  metadata {
    name      = "monitoring-grafana-oidc"
    namespace = local.monitoring_namespace
    labels    = local.monitoring_labels
  }

  data = {
    client-secret = var.grafana_oidc_client_secret
  }
}

resource "kubernetes_secret" "monitoring_alertmanager_slack" {
  metadata {
    name      = "monitoring-alertmanager-slack"
    namespace = local.monitoring_namespace
    labels    = local.monitoring_labels
  }

  data = {
    webhook-url = var.alertmanager_slack_webhook_url
  }
}
