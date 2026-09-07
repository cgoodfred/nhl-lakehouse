resource "helm_release" "monitoring_alertmanager" {
  name       = "monitoring-alertmanager"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "alertmanager"
  version    = "1.42.0"
  namespace  = local.monitoring_namespace

  atomic  = true
  lint    = true
  wait    = true
  timeout = 600

  values = [
    yamlencode({
      fullnameOverride = "monitoring-alertmanager"
      replicaCount     = 1

      automountServiceAccountToken = false

      extraSecretMounts = [{
        name       = "slack-webhook"
        mountPath  = "/etc/alertmanager/secrets"
        subPath    = ""
        secretName = kubernetes_secret.monitoring_alertmanager_slack.metadata[0].name
        readOnly   = true
      }]

      persistence = {
        enabled      = true
        storageClass = "local-path"
        accessModes  = ["ReadWriteOnce"]
        size         = "1Gi"
      }

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
        "prometheus.io/port"   = "9093"
        "prometheus.io/path"   = "/metrics"
      }

      config = {
        enabled = true
        global = {
          resolve_timeout    = "5m"
          slack_api_url_file = "/etc/alertmanager/secrets/webhook-url"
        }
        route = {
          receiver        = "slack-default"
          group_by        = ["alertname", "severity"]
          group_wait      = "30s"
          group_interval  = "5m"
          repeat_interval = "4h"
          routes = [{
            receiver = "watchdog"
            matchers = ["alertname = Watchdog"]
          }]
        }
        receivers = [
          {
            name = "watchdog"
          },
          {
            name = "slack-default"
            slack_configs = [{
              send_resolved = true
              title_link    = "https://grafana.cluster.cgood.dev/alerting/list?dataSource=Alertmanager"
              title         = "{{ if eq .Status \"firing\" }}:rotating_light:{{ else }}:white_check_mark:{{ end }} [{{ .Status | toUpper }}] {{ .GroupLabels.alertname }}"
              text          = "{{ range .Alerts }}*Severity:* {{ .Labels.severity }}\n*Summary:* {{ .Annotations.summary }}\n{{ if .Annotations.description }}*Description:* {{ .Annotations.description }}{{ end }}\n{{ end }}"
            }]
          },
        ]
        inhibit_rules = [{
          source_matchers = ["severity = critical"]
          target_matchers = ["severity = warning"]
          equal           = ["alertname", "instance"]
        }]
      }
    })
  ]
}
