variable "location" {
  description = "Hetzner location for every server, one place so the private network stays one hop."
  type        = string
  default     = "fsn1"
}

variable "db_server_type" {
  description = "Server type for the six mongod nodes."
  type        = string
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
