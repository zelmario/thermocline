variable "location" {
  description = "Hetzner location for every server, one place so the private network stays one hop."
  type        = string
  default     = "fsn1"
}

variable "hot_server_type" {
  description = <<-EOT
    Server type for the fast tier's three mongod nodes (rsHot). Empty creates
    no fast tier, for a phase that measures the baselines alone: the account's
    Primary IP limit stops ten database nodes and the app node existing at
    once.
  EOT
  type        = string
  default     = ""
}

variable "cold_server_type" {
  description = "Server type for the archive's three mongod nodes (rsCold)."
  type        = string
}

variable "cold_volume_gb" {
  description = <<-EOT
    A network volume of this many GB for each archive node's data, which is
    what a cluster that has outgrown its hardware looks like. Zero keeps the
    data on the server's local disk.
  EOT
  type        = number
  default     = 0
}

variable "big_server_type" {
  description = <<-EOT
    Server type for a third replica set, rsBig, holding a whole copy of the
    data for the "MongoDB alone on fast nodes" baseline. Empty creates no such
    set. It is never part of Thermocline: a baseline that shares a cluster with
    the archive writes into it behind the router's back.
  EOT
  type        = string
  default     = ""
}

variable "app_server_type" {
  description = "Server type for the node running the router, the daemons and the workload."
  type        = string
}

variable "image" {
  type    = string
  default = "ubuntu-24.04"
}

variable "psmdb_version" {
  description = "Percona Server for MongoDB package version, the same on both tiers. Every daemon refuses a pair on different releases."
  type        = string
  default     = "8.0.26-11"
}

variable "admin_cidr" {
  description = "Where SSH is allowed from, normally your own address as x.x.x.x/32."
  type        = string
}

variable "ssh_public_key_file" {
  type    = string
  default = "~/.ssh/id_ed25519.pub"
}

variable "name_prefix" {
  type    = string
  default = "tc"
}
