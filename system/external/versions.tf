terraform {
  required_version = "~> 1.7"

  cloud {
    hostname     = "app.terraform.io"
    organization = "yona-works"

    workspaces {
      name = "homelab-system-external"
    }
  }

  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 4.30.0"
    }

    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.26.0"
    }

    http = {
      source  = "hashicorp/http"
      version = "~> 3.4.0"
    }
  }
}

provider "cloudflare" {
  email   = var.cloudflare.email
  api_key = var.cloudflare.api_key
}

provider "kubernetes" {
  # Use KUBE_CONFIG_PATH environment variables
  # Or in cluster service account
}
