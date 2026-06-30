resource "kubernetes_namespace" "argo" {
  metadata {
    name = "argo"
  }
}

# Dedicated Postgres for the Argo Workflows archive. Sharing the existing
# Lakekeeper Postgres would couple the two systems' lifecycles and risk
# credential leakage; a second tiny instance is cheap on the Pi cluster.
resource "random_password" "argo_pg" {
  length  = 24
  special = false
}

resource "kubernetes_secret" "argo_pg" {
  metadata {
    name      = "argo-workflows-pg-creds"
    namespace = kubernetes_namespace.argo.metadata[0].name
  }
  data = {
    postgres-password   = random_password.argo_pg.result
    password            = random_password.argo_pg.result
    postgresql-user     = "argo"
    postgresql-password = random_password.argo_pg.result
  }
}

resource "helm_release" "argo_pg" {
  name       = "argo-workflows-pg"
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "postgresql"
  version    = "16.7.27"
  namespace  = kubernetes_namespace.argo.metadata[0].name

  values = [
    yamlencode({
      # bitnami/ on Docker Hub is paid since mid-2025; free images moved to
      # bitnamilegacy/. Same override as lakekeeper-pg in postgres.tf.
      image = {
        repository = "bitnamilegacy/postgresql"
      }
      auth = {
        database       = "argo"
        username       = "argo"
        existingSecret = kubernetes_secret.argo_pg.metadata[0].name
        secretKeys = {
          adminPasswordKey       = "postgres-password"
          userPasswordKey        = "password"
          replicationPasswordKey = "password"
        }
      }
      primary = {
        persistence = {
          size         = "2Gi"
          storageClass = "local-path"
        }
        resources = {
          requests = {
            cpu    = "100m"
            memory = "256Mi"
          }
          limits = {
            cpu    = "500m"
            memory = "512Mi"
          }
        }
      }
    })
  ]
}

# Argo Workflows controller + server. Controller watches the `lakehouse`
# namespace where Workflows submit SparkApplications; the server exposes the
# UI and is reached via port-forward (no Ingress in V1 — see plan).
resource "helm_release" "argo_workflows" {
  name       = "argo-workflows"
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argo-workflows"
  version    = "0.45.5"
  namespace  = kubernetes_namespace.argo.metadata[0].name

  depends_on = [
    helm_release.argo_pg,
    kubernetes_role_binding.argo_workflow_runner_lakehouse,
  ]

  values = [
    yamlencode({
      # Single managed namespace keeps the controller's RBAC scope tight. All
      # Workflows live in `lakehouse` alongside the SparkApplications they
      # orchestrate; argo namespace only holds the platform.
      singleNamespace = false
      workflowNamespaces = [
        kubernetes_namespace.lakehouse.metadata[0].name,
      ]

      controller = {
        # Persist completed Workflows to Postgres so /workflows shows history
        # past the in-memory retention window.
        persistence = {
          archive           = true
          archiveTTL        = "30d"
          nodeStatusOffLoad = true
          postgresql = {
            host      = "argo-workflows-pg-postgresql.${kubernetes_namespace.argo.metadata[0].name}.svc.cluster.local"
            port      = 5432
            database  = "argo"
            tableName = "argo_workflows"
            userNameSecret = {
              name = kubernetes_secret.argo_pg.metadata[0].name
              key  = "postgresql-user"
            }
            passwordSecret = {
              name = kubernetes_secret.argo_pg.metadata[0].name
              key  = "postgresql-password"
            }
          }
        }

        # Workflows submitted to `lakehouse` create SparkApplication CRDs. The
        # workflow-runner ServiceAccount below has the matching RBAC.
        workflowDefaults = {
          spec = {
            serviceAccountName = "argo-workflow-runner"
          }
        }

        resources = {
          requests = {
            cpu    = "100m"
            memory = "256Mi"
          }
          limits = {
            cpu    = "500m"
            memory = "512Mi"
          }
        }
      }

      server = {
        # Port-forward access only; auth-mode=server bypasses login for local
        # development. Lift to client + SSO when we expose via Ingress.
        extraArgs = ["--auth-mode=server"]
        resources = {
          requests = {
            cpu    = "50m"
            memory = "128Mi"
          }
          limits = {
            cpu    = "300m"
            memory = "256Mi"
          }
        }
      }

      # Workflow step pods inherit a default ServiceAccount per namespace; we
      # provide our own (argo_workflow_runner) so disable the chart's default.
      workflow = {
        rbac = {
          create = false
        }
      }
    })
  ]
}

# ServiceAccount that Workflow steps run as in the `lakehouse` namespace.
# Workflow steps that use the `resource:` action to create SparkApplication
# CRDs need permissions on the operator's CRD set; pod/log access is for the
# UI to surface step logs.
resource "kubernetes_service_account" "argo_workflow_runner" {
  metadata {
    name      = "argo-workflow-runner"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
}

resource "kubernetes_role" "argo_workflow_runner_lakehouse" {
  metadata {
    name      = "argo-workflow-runner"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }

  # SparkApplication CRD — create + watch is the load-bearing pair: create
  # submits the job, watch is how Argo's successCondition polls completion.
  rule {
    api_groups = ["sparkoperator.k8s.io"]
    resources  = ["sparkapplications"]
    verbs      = ["create", "delete", "get", "list", "patch", "update", "watch"]
  }

  # Step pods themselves + log access for the UI.
  rule {
    api_groups = [""]
    resources  = ["pods", "pods/log"]
    verbs      = ["create", "delete", "get", "list", "patch", "update", "watch"]
  }

  # Argo's executor writes step outputs as ConfigMaps when nodes are large.
  rule {
    api_groups = [""]
    resources  = ["configmaps"]
    verbs      = ["create", "get", "list", "update", "watch"]
  }

  # workflowtaskresults is how the Argo emissary reports per-step status back
  # to the controller. Without it, every step ends up `Error` even after
  # successful work because the controller can't see the result.
  rule {
    api_groups = ["argoproj.io"]
    resources  = ["workflowtaskresults"]
    verbs      = ["create", "get", "list", "patch", "update", "watch"]
  }
}

resource "kubernetes_role_binding" "argo_workflow_runner_lakehouse" {
  metadata {
    name      = "argo-workflow-runner"
    namespace = kubernetes_namespace.lakehouse.metadata[0].name
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.argo_workflow_runner_lakehouse.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.argo_workflow_runner.metadata[0].name
    namespace = kubernetes_service_account.argo_workflow_runner.metadata[0].namespace
  }
}
