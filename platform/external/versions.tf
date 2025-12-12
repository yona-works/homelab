terraform {
  required_version = "~> 1.7"

  cloud {
    hostname     = "app.terraform.io"
    organization = "jnskt"

    workspaces {
      name = "homelab-platform-external"
    }
  }

  required_providers {
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

provider "kubernetes" {
}
