# OpenTofu owns the long-lived Argo definitions. Individual Workflow runs and
# generated SparkApplications remain Argo/Spark Operator runtime objects.

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
