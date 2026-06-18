terraform {
  required_version = ">= 1.8.0"

  # State persisted as a kubernetes Secret in the lakehouse namespace.
  backend "kubernetes" {
    secret_suffix = "lakehouse-state"
    namespace     = "lakehouse"
  }

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 3.0"
    }
  }
}
