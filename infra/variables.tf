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
