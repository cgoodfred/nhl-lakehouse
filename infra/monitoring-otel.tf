locals {
  # Metrics are opt-in. Each pattern supports a dashboard, alert, or a concrete
  # troubleshooting question for this cluster. Keep histogram count/sum series
  # where averages are useful, but omit bucket series unless a percentile is
  # explicitly needed.
  monitoring_metric_allowlist = [
    # Scrape health and Kubernetes workload state.
    "up",
    "kube_node_status_(condition|capacity|allocatable)",
    "kube_pod_status_(phase|ready)",
    "kube_pod_container_status_(waiting_reason|restarts_total)",
    "kube_deployment_(spec_replicas|status_replicas_available)",
    "kube_statefulset_(replicas|status_replicas_ready)",
    "kube_daemonset_status_(desired_number_scheduled|number_ready)",
    "kube_job_(created|status_(active|failed|succeeded|completion_time))",
    "kube_cronjob_(status_(active|last_schedule_time)|next_schedule_time|spec_suspend)",
    "kube_persistentvolumeclaim_(status_phase|resource_requests_storage_bytes)",
    "kube_resourcequota",

    # Node capacity plus aggregate pod CPU, memory, and ephemeral disk use.
    "node_cpu_seconds_total",
    "node_memory_(MemAvailable|MemTotal|SwapFree|SwapTotal)_bytes",
    "node_filesystem_(avail_bytes|size_bytes|readonly)",
    "node_load(1|5)",
    "node_boot_time_seconds",
    "node_network_(receive|transmit)_(bytes|errs|drop)_total",
    "k8s[.]pod[.](cpu[.](time|usage)|memory[.]working_set|filesystem[.]usage)",

    # Scheduled ingestion and the services it depends on.
    "argo_workflows_(cronworkflows_triggered_total|error_count|gauge|is_leader|operation_duration_seconds_(count|sum)|queue_depth_gauge|total_count|workers_busy_count|workflow_condition|workflowtemplate_(runtime|triggered_total))",
    "SeaweedFS_(master_(is_leader|leader_changes_total|pick_for_write_error_total|volume_layout_(crowded|writable))|volumeServer_(disk_error_status|file_(read|write)_failures_total|master_disconnections_total|read_only_volumes|total_disk_size|volumes)|s3_bucket_(object_count|physical_size_bytes|size_bytes)|s3_request_(seconds_(count|sum)|total)|filer_request_(seconds_(count|sum)|total))",
    "pg_(up|exporter_last_scrape_(duration_seconds|error)|database_size_bytes|locks_count|replication_(is_replica|lag_seconds|last_replay_seconds)|stat_database_(numbackends|xact_commit|xact_rollback|blks_read|blks_hit|deadlocks|temp_bytes))",
    "axum_http_requests_(duration_seconds_(count|sum)|pending|total)",
    "lakekeeper_cache_(hits_total|misses_total|size)",
    "coredns_dns_(panics_total|request_duration_seconds_(count|sum)|requests_total|responses_total)",

    # Delivery health, cardinality, and storage-backend health.
    "otelcol_exporter_(queue_(capacity|size)|send_failed_(log_records|metric_points|spans)_total|sent_(log_records|metric_points|spans)_total)",
    "otelcol_receiver_(accepted|failed|refused)_(log_records|metric_points|spans)_total",
    "otelcol_process_(cpu_seconds_total|memory_rss)",
    "cortex_discarded_samples_total",
    "cortex_distributor_ingestion_rate_samples_per_second",
    "cortex_ingester_(active_series|ingested_samples_failures_total|ingestion_rate_samples_per_second|memory_series|oldest_unshipped_block_timestamp_seconds|shipper_upload_failures_total)",
    "cortex_compactor_(disk_out_of_space_errors_total|group_compactions_failures_total|last_successful_run_timestamp_seconds)",
    "loki_distributor_(bytes|lines)_received_total",
    "loki_ingester_(chunk_stored_bytes_total|chunks_flush_failures_total|chunks_stored_total|memory_streams|wal_disk_full_failures_total|wal_disk_usage_percent)",
    "loki_compactor_apply_retention_last_successful_run_timestamp_seconds",
    "tempo_distributor_(bytes_received_total|spans_received_total)",
    "tempo_ingester_(blocks_flushed_total|failed_flushes_total|flush_queue_length)",
    "alertmanager_notifications_(failed_total|total)",
  ]

  monitoring_metric_allowlist_regex = "^(${join("|", local.monitoring_metric_allowlist)})$"

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
          enabled = false
        }
        target_info = {
          enabled = false
        }
        disable_scope_info            = true
        max_batch_request_parallelism = 1
        remote_write_queue = {
          enabled       = true
          num_consumers = 1
          queue_size    = 10000
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
      "filter/metric_allowlist" = {
        error_mode = "propagate"
        metric_conditions = [
          format("not IsMatch(metric.name, %q)", local.monitoring_metric_allowlist_regex),
        ]
      }
      "transform/metric_labels" = {
        error_mode = "propagate"
        metric_statements = [{
          context = "datapoint"
          statements = [
            "set(datapoint.attributes[\"k8s_cluster_name\"], resource.attributes[\"k8s.cluster.name\"]) where IsMatch(metric.name, \"^k8s[.]pod[.]\") and resource.attributes[\"k8s.cluster.name\"] != nil",
            "set(datapoint.attributes[\"k8s_namespace_name\"], resource.attributes[\"k8s.namespace.name\"]) where IsMatch(metric.name, \"^k8s[.]pod[.]\") and resource.attributes[\"k8s.namespace.name\"] != nil",
            "set(datapoint.attributes[\"k8s_node_name\"], resource.attributes[\"k8s.node.name\"]) where IsMatch(metric.name, \"^k8s[.]pod[.]\") and resource.attributes[\"k8s.node.name\"] != nil",
            "set(datapoint.attributes[\"k8s_pod_name\"], resource.attributes[\"k8s.pod.name\"]) where IsMatch(metric.name, \"^k8s[.]pod[.]\") and resource.attributes[\"k8s.pod.name\"] != nil",
          ]
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
          processors = ["memory_limiter", "resource", "filter/metric_allowlist", "transform/metric_labels", "batch"]
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
        # node-exporter is the single source for host metrics; collecting them
        # again in the agent produces duplicate series without adding insight.
        hostMetrics          = { enabled = false }
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
        # kube-state-metrics is the single source for Kubernetes object state.
        clusterMetrics       = { enabled = false }
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
