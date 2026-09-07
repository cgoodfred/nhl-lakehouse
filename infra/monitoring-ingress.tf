# Adopt the existing public route but preserve its current backend on the first
# apply. After the replacement has soaked and OIDC is verified, changing
# monitoring_grafana_cutover to true is the single auditable traffic switch.
import {
  to = kubernetes_manifest.monitoring_grafana_ingressroute
  id = "apiVersion=traefik.io/v1alpha1,kind=IngressRoute,namespace=monitoring,name=grafana"
}

resource "kubernetes_manifest" "monitoring_grafana_ingressroute" {
  manifest = {
    apiVersion = "traefik.io/v1alpha1"
    kind       = "IngressRoute"
    metadata = {
      name      = "grafana"
      namespace = local.monitoring_namespace
    }
    spec = {
      entryPoints = ["web"]
      routes = [{
        match = "Host(`grafana.cluster.cgood.dev`)"
        kind  = "Rule"
        services = [{
          name = var.monitoring_grafana_cutover ? "monitoring-grafana" : "grafana"
          port = var.monitoring_grafana_cutover ? 80 : 3000
        }]
      }]
    }
  }

  # This adopted route was originally managed with kubectl client-side apply.
  # OpenTofu is now authoritative for its backend and must claim those fields
  # during the one-time cutover without replacing the IngressRoute.
  field_manager {
    name            = "opentofu"
    force_conflicts = true
  }

  depends_on = [helm_release.monitoring_grafana]
}
