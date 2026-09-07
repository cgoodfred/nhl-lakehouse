resource "kubernetes_config_map" "monitoring_grafana_dashboards" {
  metadata {
    name      = "monitoring-grafana-dashboards"
    namespace = local.monitoring_namespace
    labels    = local.monitoring_labels
  }

  data = {
    "cluster-overview.json" = file("${path.module}/monitoring/dashboards/cluster-overview.json")
  }
}

resource "helm_release" "monitoring_grafana" {
  name       = "monitoring-grafana"
  repository = "https://grafana-community.github.io/helm-charts"
  chart      = "grafana"
  version    = "13.2.2"
  namespace  = local.monitoring_namespace

  atomic  = true
  lint    = true
  wait    = true
  timeout = 900

  values = [
    yamlencode({
      fullnameOverride = "monitoring-grafana"
      replicas         = 1

      admin = {
        existingSecret = kubernetes_secret.monitoring_grafana_admin.metadata[0].name
        userKey        = "admin-user"
        passwordKey    = "admin-password"
      }

      envValueFrom = {
        GF_AUTH_GENERIC_OAUTH_CLIENT_SECRET = {
          secretKeyRef = {
            name = kubernetes_secret.monitoring_grafana_oidc.metadata[0].name
            key  = "client-secret"
          }
        }
      }

      "grafana.ini" = {
        analytics = {
          check_for_updates = false
          reporting_enabled = false
        }
        server = {
          domain   = "grafana.cluster.cgood.dev"
          root_url = "https://grafana.cluster.cgood.dev"
        }
        users = {
          allow_sign_up        = false
          auto_assign_org      = true
          auto_assign_org_role = "Admin"
        }
        "auth.anonymous" = {
          enabled = false
        }
        "auth.generic_oauth" = {
          enabled                    = true
          name                       = "Keycloak"
          allow_sign_up              = true
          auto_login                 = true
          use_pkce                   = true
          client_id                  = var.grafana_oidc_client_id
          scopes                     = "openid profile email"
          auth_url                   = "${var.grafana_oidc_issuer_url}/protocol/openid-connect/auth"
          token_url                  = "${var.grafana_oidc_issuer_url}/protocol/openid-connect/token"
          api_url                    = "${var.grafana_oidc_issuer_url}/protocol/openid-connect/userinfo"
          role_attribute_strict      = false
          allow_assign_grafana_admin = true
        }
      }

      persistence = {
        enabled      = true
        storageClass = "local-path"
        accessModes  = ["ReadWriteOnce"]
        size         = "2Gi"
      }

      resources = {
        requests = { cpu = "100m", memory = "256Mi" }
        limits   = { cpu = "500m", memory = "768Mi" }
      }

      podAnnotations = {
        "prometheus.io/scrape" = "true"
        "prometheus.io/port"   = "3000"
        "prometheus.io/path"   = "/metrics"
      }

      datasources = {
        "datasources.yaml" = {
          apiVersion = 1
          deleteDatasources = [
            { name = "Mimir", orgId = 1 },
            { name = "Loki", orgId = 1 },
            { name = "Tempo", orgId = 1 },
            { name = "Alertmanager", orgId = 1 },
          ]
          datasources = [
            {
              name      = "Mimir"
              uid       = "mimir"
              type      = "prometheus"
              access    = "proxy"
              url       = "http://monitoring-mimir:9009/prometheus"
              isDefault = true
              editable  = false
              jsonData = {
                prometheusType    = "Mimir"
                prometheusVersion = "3.2.0"
                timeInterval      = "30s"
                manageAlerts      = true
                alertmanagerUid   = "alertmanager"
              }
            },
            {
              name     = "Loki"
              uid      = "loki"
              type     = "loki"
              access   = "proxy"
              url      = "http://monitoring-loki:3100"
              editable = false
              jsonData = {
                derivedFields = [{
                  datasourceUid = "tempo"
                  matcherRegex  = "(?:trace_id|traceid)[=\\\": ]+([a-fA-F0-9]{16,32})"
                  name          = "TraceID"
                  url           = "$${__value.raw}"
                }]
              }
            },
            {
              name     = "Tempo"
              uid      = "tempo"
              type     = "tempo"
              access   = "proxy"
              url      = "http://monitoring-tempo:3200"
              editable = false
              jsonData = {
                nodeGraph = { enabled = true }
                tracesToLogsV2 = {
                  datasourceUid      = "loki"
                  spanStartTimeShift = "-1m"
                  spanEndTimeShift   = "1m"
                  filterByTraceID    = true
                  filterBySpanID     = false
                }
              }
            },
            {
              name     = "Alertmanager"
              uid      = "alertmanager"
              type     = "alertmanager"
              access   = "proxy"
              url      = "http://monitoring-alertmanager:9093"
              editable = false
              jsonData = {
                implementation             = "prometheus"
                handleGrafanaManagedAlerts = false
              }
            },
          ]
        }
      }

      dashboardProviders = {
        "dashboardproviders.yaml" = {
          apiVersion = 1
          providers = [{
            name            = "nhl-lakehouse"
            orgId           = 1
            folder          = "NHL Lakehouse"
            type            = "file"
            disableDeletion = true
            editable        = false
            options = {
              path = "/var/lib/grafana/dashboards/nhl-lakehouse"
            }
          }]
        }
      }
      dashboardsConfigMaps = {
        "nhl-lakehouse" = kubernetes_config_map.monitoring_grafana_dashboards.metadata[0].name
      }

      testFramework = {
        enabled = false
      }
    })
  ]

  depends_on = [
    kubernetes_deployment.monitoring_mimir,
    helm_release.monitoring_loki,
    helm_release.monitoring_tempo,
    helm_release.monitoring_alertmanager,
  ]
}
