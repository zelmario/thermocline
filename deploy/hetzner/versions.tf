terraform {
  required_version = ">= 1.5"
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.48"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

# The token is read from HCLOUD_TOKEN, which run.sh fills from
# ~/.config/hcloud/token, so it is never in a file Terraform writes.
provider "hcloud" {}
