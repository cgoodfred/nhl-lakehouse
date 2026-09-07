resource "helm_release" "monitoring_kube_state_metrics" {
  name       = "monitoring-kube-state-metrics"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-state-metrics"
  version    = "8.4.2"
  namespace  = local.monitoring_namespace

  atomic  = true
  lint    = true
  wait    = true
  timeout = 600

  values = [
    yamlencode({
      fullnameOverride = "monitoring-kube-state-metrics"
      replicas         = 1
      resources = {
        requests = {
          cpu    = "25m"
          memory = "64Mi"
        }
        limits = {
          cpu    = "200m"
          memory = "256Mi"
        }
      }
      podAnnotations = {
        "prometheus.io/scrape" = "true"
        "prometheus.io/port"   = "8080"
        "prometheus.io/path"   = "/metrics"
      }
      prometheus = {
        monitor = {
          enabled = false
        }
      }
    })
  ]
}

resource "helm_release" "monitoring_node_exporter" {
  name       = "monitoring-node-exporter"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "prometheus-node-exporter"
  version    = "4.56.3"
  namespace  = local.monitoring_namespace

  atomic  = true
  lint    = true
  wait    = true
  timeout = 600

  values = [
    yamlencode({
      fullnameOverride = "monitoring-node-exporter"
      resources = {
        requests = {
          cpu    = "20m"
          memory = "32Mi"
        }
        limits = {
          cpu    = "100m"
          memory = "128Mi"
        }
      }
      podAnnotations = {
        "prometheus.io/scrape" = "true"
        "prometheus.io/port"   = "9100"
        "prometheus.io/path"   = "/metrics"
      }
      prometheus = {
        monitor = {
          enabled = false
        }
      }
    })
  ]
}
