locals {
  otel_disabled_ports = {
    "jaeger-compact" = { enabled = false }
    "jaeger-thrift"  = { enabled = false }
    "jaeger-grpc"    = { enabled = false }
    zipkin           = { enabled = false }
  }

  otel_agent_config = {
    exporters = {
      otlp = {
        endpoint = "monitoring-otel-gateway:4317"
        tls = {
          insecure = true
        }
        sending_queue = {
          enabled    = true
          queue_size = 512
        }
        retry_on_failure = {
          enabled = true
        }
      }
    }
    extensions = {
      health_check = {
        endpoint = "$${env:MY_POD_IP}:13133"
      }
    }
    processors = {
      memory_limiter = {
        check_interval         = "5s"
        limit_percentage       = 80
        spike_limit_percentage = 25
      }
      batch = {
        timeout         = "5s"
        send_batch_size = 1024
      }
      resource = {
        attributes = [{
          key    = "k8s.cluster.name"
          value  = "pi-cluster"
          action = "upsert"
        }]
      }
    }
    receivers = {
      otlp = {
        protocols = {
          grpc = { endpoint = "$${env:MY_POD_IP}:4317" }
          http = { endpoint = "$${env:MY_POD_IP}:4318" }
        }
      }
    }
    service = {
      telemetry = {
        metrics = {
          readers = [{
            pull = {
              exporter = {
                prometheus = {
                  host = "$${env:MY_POD_IP}"
                  port = 8888
                }
              }
            }
          }]
        }
      }
      extensions = ["health_check"]
      pipelines = {
        logs = {
          receivers  = ["otlp"]
          processors = ["memory_limiter", "resource", "batch"]
          exporters  = ["otlp"]
        }
        metrics = {
          receivers  = ["otlp"]
          processors = ["memory_limiter", "resource", "batch"]
          exporters  = ["otlp"]
        }
        traces = {
          receivers  = ["otlp"]
          processors = ["memory_limiter", "resource", "batch"]
          exporters  = ["otlp"]
        }
      }
    }
  }

  otel_gateway_config = {
    exporters = {
      prometheusremotewrite = {
        endpoint = "http://monitoring-mimir:9009/api/v1/push"
        resource_to_telemetry_conversion = {
          enabled = true
        }
        retry_on_failure = {
          enabled = true
        }
      }
      "otlphttp/loki" = {
        endpoint = "http://monitoring-loki:3100/otlp"
        tls = {
          insecure = true
        }
        retry_on_failure = {
          enabled = true
        }
      }
      "otlp/tempo" = {
        endpoint = "monitoring-tempo:4317"
        tls = {
          insecure = true
        }
        retry_on_failure = {
          enabled = true
        }
      }
    }
    extensions = {
      health_check = {
        endpoint = "$${env:MY_POD_IP}:13133"
      }
    }
    processors = {
      memory_limiter = {
        check_interval         = "5s"
        limit_percentage       = 80
        spike_limit_percentage = 25
      }
      batch = {
        timeout         = "5s"
        send_batch_size = 1024
      }
      resource = {
        attributes = [{
          key    = "k8s.cluster.name"
          value  = "pi-cluster"
          action = "upsert"
        }]
      }
    }
    receivers = {
      otlp = {
        protocols = {
          grpc = { endpoint = "$${env:MY_POD_IP}:4317" }
          http = { endpoint = "$${env:MY_POD_IP}:4318" }
        }
      }
      prometheus = {
        config = {
          scrape_configs = [
            {
              job_name        = "kubernetes-pods"
              scrape_interval = "30s"
              kubernetes_sd_configs = [{
                role = "pod"
              }]
              relabel_configs = [
                {
                  action        = "keep"
                  source_labels = ["__meta_kubernetes_pod_annotation_prometheus_io_scrape"]
                  regex         = "true"
                },
                {
                  action        = "replace"
                  source_labels = ["__meta_kubernetes_pod_annotation_prometheus_io_path"]
                  target_label  = "__metrics_path__"
                  regex         = "(.+)"
                },
                {
                  action        = "replace"
                  source_labels = ["__address__", "__meta_kubernetes_pod_annotation_prometheus_io_port"]
                  target_label  = "__address__"
                  regex         = "([^:]+)(?::\\d+)?;(\\d+)"
                  replacement   = "$1:$2"
                },
                {
                  action        = "replace"
                  source_labels = ["__meta_kubernetes_namespace"]
                  target_label  = "namespace"
                },
                {
                  action        = "replace"
                  source_labels = ["__meta_kubernetes_pod_name"]
                  target_label  = "pod"
                },
                {
                  action        = "replace"
                  source_labels = ["__meta_kubernetes_pod_node_name"]
                  target_label  = "node"
                },
                {
                  action        = "replace"
                  source_labels = ["__meta_kubernetes_pod_label_app_kubernetes_io_name"]
                  target_label  = "app"
                },
              ]
            },
            {
              job_name        = "seaweedfs"
              scrape_interval = "30s"
              static_configs = [{
                targets = [
                  "seaweedfs-master.lakehouse.svc.cluster.local:9327",
                  "seaweedfs-filer.lakehouse.svc.cluster.local:9327",
                  "seaweedfs-volume.lakehouse.svc.cluster.local:9327",
                  "seaweedfs-s3.lakehouse.svc.cluster.local:9327",
                ]
              }]
            },
            {
              job_name        = "coredns"
              scrape_interval = "30s"
              static_configs = [{
                targets = ["kube-dns.kube-system.svc.cluster.local:9153"]
              }]
            },
            {
              job_name        = "lakekeeper"
              scrape_interval = "30s"
              metrics_path    = "/metrics"
              kubernetes_sd_configs = [{
                role = "pod"
                namespaces = {
                  names = ["lakehouse"]
                }
              }]
              relabel_configs = [
                {
                  action        = "keep"
                  source_labels = ["__meta_kubernetes_pod_label_app_kubernetes_io_name"]
                  regex         = "lakekeeper"
                },
                {
                  action        = "replace"
                  source_labels = ["__meta_kubernetes_pod_ip"]
                  target_label  = "__address__"
                  replacement   = "$1:9000"
                },
              ]
            },
          ]
        }
      }
    }
    service = {
      telemetry = {
        metrics = {
          readers = [{
            pull = {
              exporter = {
                prometheus = {
                  host = "$${env:MY_POD_IP}"
                  port = 8888
                }
              }
            }
          }]
        }
      }
      extensions = ["health_check"]
      pipelines = {
        logs = {
          receivers  = ["otlp"]
          processors = ["memory_limiter", "resource", "batch"]
          exporters  = ["otlphttp/loki"]
        }
        metrics = {
          receivers  = ["otlp", "prometheus"]
          processors = ["memory_limiter", "resource", "batch"]
          exporters  = ["prometheusremotewrite"]
        }
        traces = {
          receivers  = ["otlp"]
          processors = ["memory_limiter", "resource", "batch"]
          exporters  = ["otlp/tempo"]
        }
      }
    }
  }
}

resource "helm_release" "monitoring_otel_agent" {
  name       = "monitoring-otel-agent"
  repository = "https://open-telemetry.github.io/opentelemetry-helm-charts"
  chart      = "opentelemetry-collector"
  version    = "0.172.1"
  namespace  = local.monitoring_namespace

  atomic  = true
  lint    = true
  wait    = true
  timeout = 900

  values = [
    yamlencode({
      fullnameOverride = "monitoring-otel-agent"
      mode             = "daemonset"
      image = {
        repository = "otel/opentelemetry-collector-contrib"
        tag        = "0.159.0"
      }
      command = {
        name = "otelcol-contrib"
      }
      presets = {
        logsCollection = {
          enabled              = true
          includeCollectorLogs = false
          storeCheckpoints     = false
        }
        hostMetrics          = { enabled = true }
        kubeletMetrics       = { enabled = true }
        kubernetesAttributes = { enabled = true }
        resourceDetection    = { enabled = true }
      }
      alternateConfig = local.otel_agent_config
      ports = merge(local.otel_disabled_ports, {
        otlp      = { enabled = true, hostPort = null }
        otlp-http = { enabled = true, hostPort = null }
        metrics   = { enabled = true }
      })
      service = {
        enabled = false
      }
      resources = {
        requests = { cpu = "50m", memory = "128Mi" }
        limits   = { cpu = "300m", memory = "384Mi" }
      }
      podAnnotations = {
        "prometheus.io/scrape" = "true"
        "prometheus.io/port"   = "8888"
        "prometheus.io/path"   = "/metrics"
      }
    })
  ]

  depends_on = [helm_release.monitoring_otel_gateway]
}

resource "helm_release" "monitoring_otel_gateway" {
  name       = "monitoring-otel-gateway"
  repository = "https://open-telemetry.github.io/opentelemetry-helm-charts"
  chart      = "opentelemetry-collector"
  version    = "0.172.1"
  namespace  = local.monitoring_namespace

  atomic  = true
  lint    = true
  wait    = true
  timeout = 900

  values = [
    yamlencode({
      fullnameOverride = "monitoring-otel-gateway"
      mode             = "deployment"
      replicaCount     = 1
      image = {
        repository = "otel/opentelemetry-collector-contrib"
        tag        = "0.159.0"
      }
      command = {
        name = "otelcol-contrib"
      }
      presets = {
        clusterMetrics       = { enabled = true }
        kubernetesAttributes = { enabled = true }
        kubernetesEvents     = { enabled = true, useK8sEventsReceiver = true }
        resourceDetection    = { enabled = true }
      }
      alternateConfig = local.otel_gateway_config
      ports = merge(local.otel_disabled_ports, {
        otlp      = { enabled = true, hostPort = null }
        otlp-http = { enabled = true, hostPort = null }
        metrics   = { enabled = true }
      })
      clusterRole = {
        rules = [{
          apiGroups = [""]
          resources = ["nodes", "nodes/proxy", "nodes/stats", "services", "endpoints", "pods", "namespaces"]
          verbs     = ["get", "list", "watch"]
          }, {
          apiGroups = ["discovery.k8s.io"]
          resources = ["endpointslices"]
          verbs     = ["get", "list", "watch"]
        }]
      }
      resources = {
        requests = { cpu = "100m", memory = "256Mi" }
        limits   = { cpu = "500m", memory = "768Mi" }
      }
      podAnnotations = {
        "prometheus.io/scrape" = "true"
        "prometheus.io/port"   = "8888"
        "prometheus.io/path"   = "/metrics"
      }
    })
  ]

  depends_on = [
    kubernetes_deployment.monitoring_mimir,
    helm_release.monitoring_loki,
    helm_release.monitoring_tempo,
  ]
}
