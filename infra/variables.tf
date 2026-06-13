variable "kubeconfig_path" {
  type        = string
  description = "path to kubeconfig"
  default     = "~/.kube/pi-config"
  sensitive   = false
}
