variable "kubeconfig_path" {
  type        = string
  description = "path to kubeconfig"
  default     = "~/.kube/pi-config"
  sensitive   = false
}

variable "s3_access_key" {
  type        = string
  description = "s3 access key"
  sensitive   = true
}

variable "s3_secret_key" {
  type        = string
  description = "s3 secret key"
  sensitive   = true
}

variable "github_pat" {
  type        = string
  description = "fine-grained github personal access token for runner registration (Administration: write on the repo)"
  sensitive   = true
}

variable "backfill_season" {
  type        = string
  description = "8 digit season to backfill (e.g. 20232024)"
  default     = ""
  sensitive   = false
}

variable "keycloak_issuer_url" {
  type        = string
  description = "Keycloak OIDC issuer URL, e.g. https://keycloak.example.com/realms/<realm>"
  sensitive   = false
}

variable "lakekeeper_spark_client_id" {
  type        = string
  description = "Keycloak client ID used by Spark and the bootstrap Job to obtain tokens via client_credentials"
  default     = "lakekeeper-spark"
  sensitive   = false
}

variable "lakekeeper_spark_client_secret" {
  type        = string
  description = "Keycloak client secret for the lakekeeper_spark_client_id client"
  sensitive   = true
}
