# OpenTofu owns the long-lived Argo definitions. Individual Workflow runs and
# generated SparkApplications remain Argo/Spark Operator runtime objects.

# Adopt the two existing reusable templates so the TTL and pod-cleanup fixes
# are actually applied to the templates already referenced by the scheduled
# DAG. These import blocks make the first deployment non-destructive.
import {
  to = kubernetes_manifest.silver_single_table_workflow_template
  id = "apiVersion=argoproj.io/v1alpha1,kind=WorkflowTemplate,namespace=lakehouse,name=silver-single-table"
}

import {
  to = kubernetes_manifest.silver_full_rebuild_workflow_template
  id = "apiVersion=argoproj.io/v1alpha1,kind=WorkflowTemplate,namespace=lakehouse,name=silver-full-rebuild"
}

resource "kubernetes_manifest" "silver_single_table_workflow_template" {
  manifest = yamldecode(file("${path.module}/../workflows/templates/silver-single-table.yaml"))

  depends_on = [helm_release.argo_workflows]
}

resource "kubernetes_manifest" "silver_full_rebuild_workflow_template" {
  manifest = yamldecode(file("${path.module}/../workflows/templates/silver-full-rebuild.yaml"))

  depends_on = [
    helm_release.argo_workflows,
    kubernetes_manifest.silver_single_table_workflow_template,
  ]
}

resource "kubernetes_manifest" "ingest_window_workflow_template" {
  manifest = yamldecode(file("${path.module}/../workflows/templates/ingest-window.yaml"))

  depends_on = [helm_release.argo_workflows]
}

resource "kubernetes_manifest" "tracking_ingest_workflow_template" {
  manifest = yamldecode(file("${path.module}/../workflows/templates/tracking-ingest.yaml"))

  depends_on = [helm_release.argo_workflows]
}

resource "kubernetes_manifest" "scheduled_pipeline_workflow_template" {
  manifest = yamldecode(file("${path.module}/../workflows/templates/scheduled-pipeline.yaml"))

  depends_on = [
    helm_release.argo_workflows,
    kubernetes_manifest.ingest_window_workflow_template,
    kubernetes_manifest.tracking_ingest_workflow_template,
  ]
}

resource "kubernetes_manifest" "nhl_scheduled_pipeline_cronworkflow" {
  manifest = yamldecode(file("${path.module}/../workflows/cron/nhl-scheduled-pipeline.yaml"))

  depends_on = [
    helm_release.argo_workflows,
    kubernetes_manifest.scheduled_pipeline_workflow_template,
  ]
}
