# A staging pair for Thermocline: a three-member fast tier (rsHot), a
# three-member archive (rsCold), and one node for the router, the daemons and
# the workload. Everything talks over the private network; only SSH is open on
# the public side, and only from admin_cidr.

locals {
  hot  = var.hot_server_type == "" ? {} : { for i in [1, 2, 3] : "hot${i}" => { ip = "10.20.1.${10 + i}", set = "rsHot" } }
  # Every archive node there could be, whether or not it exists right now: the
  # volumes holding their data outlive them. See cold_server_type.
  cold_all = { for i in [1, 2, 3] : "cold${i}" => { ip = "10.20.1.${20 + i}", set = "rsCold" } }
  cold     = var.cold_server_type == "" ? {} : local.cold_all
  # The baseline set, only when a type is given for it. See big_server_type.
  big    = var.big_server_type == "" ? {} : { for i in [1, 2, 3] : "big${i}" => { ip = "10.20.1.${30 + i}", set = "rsBig" } }
  dbs    = merge(local.hot, local.cold, local.big)
  sets   = toset(distinct([for k, v in local.dbs : v.set]))
  type   = { rsHot = var.hot_server_type, rsCold = var.cold_server_type, rsBig = var.big_server_type }
  app_ip = "10.20.1.5"
}

resource "hcloud_ssh_key" "admin" {
  name       = "${var.name_prefix}-admin"
  public_key = file(pathexpand(var.ssh_public_key_file))
}

resource "hcloud_network" "net" {
  name     = "${var.name_prefix}-net"
  ip_range = "10.20.0.0/16"
}

resource "hcloud_network_subnet" "sub" {
  network_id   = hcloud_network.net.id
  type         = "cloud"
  network_zone = "eu-central"
  ip_range     = "10.20.1.0/24"
}

resource "hcloud_firewall" "ssh_only" {
  name = "${var.name_prefix}-ssh-only"
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "22"
    source_ips = [var.admin_cidr]
  }
  rule {
    direction  = "in"
    protocol   = "icmp"
    source_ips = [var.admin_cidr]
  }
}

# One keyfile per replica set: members of a set authenticate to each other with
# it, and the two sets never need to.
# Every set's keyfile is kept whether or not its nodes exist, so an archive whose
# servers were removed and recreated comes back with the key its data expects.
resource "random_password" "keyfile" {
  for_each = toset(["rsHot", "rsCold", "rsBig"])
  length   = 756
  special  = false
}

resource "random_password" "dba" {
  length  = 24
  special = false
}

resource "hcloud_server" "db" {
  for_each     = local.dbs
  name         = "${var.name_prefix}-${each.key}"
  server_type  = local.type[each.value.set]
  image        = var.image
  location     = var.location
  ssh_keys     = [hcloud_ssh_key.admin.id]
  firewall_ids = [hcloud_firewall.ssh_only.id]
  labels       = { project = "thermocline", role = each.value.set }

  # IPv4 only. Hetzner gives every server an IPv6 primary IP as well unless
  # told not to, and each one counts against the project's Primary IP limit:
  # seven servers took fourteen, and an apply with ten free stopped at five.
  # Nothing here uses IPv6.
  public_net {
    ipv4_enabled = true
    ipv6_enabled = false
  }

  network {
    network_id = hcloud_network.net.id
    ip         = each.value.ip
  }

  user_data = templatefile("${path.module}/cloud-init-mongod.yaml.tftpl", {
    set           = each.value.set
    private_ip    = each.value.ip
    keyfile       = random_password.keyfile[each.value.set].result
    psmdb_version = var.psmdb_version
    data_volume   = each.value.set == "rsCold" && var.cold_volume_gb > 0
  })

  # user_data runs once, at first boot, so an edited template must not replace
  # a running cluster on the next apply. Changing admin_cidr when the admin's
  # address moves is an apply too.
  lifecycle {
    ignore_changes = [user_data]
  }

  depends_on = [hcloud_network_subnet.sub]
}

# The archive's data, on network volumes. See cold_volume_gb.
#
# Not tied to the servers. Loading the archive takes an hour at 200 GB and most
# of a day at a terabyte, so between tests its servers are removed and these
# are kept, which costs the volumes alone. Recreated servers mount them again
# and the replica set comes back with its data. prevent_destroy is what keeps a
# plain `destroy` from taking the data with it: remove the volumes on purpose.
resource "hcloud_volume" "data" {
  for_each = var.cold_volume_gb > 0 ? local.cold_all : {}
  name     = "${var.name_prefix}-${each.key}-data"
  size     = var.cold_volume_gb
  location = var.location
  labels   = { project = "thermocline", role = "rsCold" }

  lifecycle {
    prevent_destroy = true
  }
}

resource "hcloud_volume_attachment" "data" {
  for_each  = var.cold_volume_gb > 0 ? local.cold : {}
  volume_id = hcloud_volume.data[each.key].id
  server_id = hcloud_server.db[each.key].id
  automount = false
}

moved {
  from = hcloud_server.app
  to   = hcloud_server.app[0]
}

resource "hcloud_server" "app" {
  count        = var.app_server_type == "" ? 0 : 1
  name         = "${var.name_prefix}-app"
  server_type  = var.app_server_type
  image        = var.image
  location     = var.location
  ssh_keys     = [hcloud_ssh_key.admin.id]
  firewall_ids = [hcloud_firewall.ssh_only.id]
  labels       = { project = "thermocline", role = "app" }

  # IPv4 only. Hetzner gives every server an IPv6 primary IP as well unless
  # told not to, and each one counts against the project's Primary IP limit:
  # seven servers took fourteen, and an apply with ten free stopped at five.
  # Nothing here uses IPv6.
  public_net {
    ipv4_enabled = true
    ipv6_enabled = false
  }

  network {
    network_id = hcloud_network.net.id
    ip         = local.app_ip
  }

  user_data = templatefile("${path.module}/cloud-init-app.yaml.tftpl", {
    psmdb_version = var.psmdb_version
  })

  # user_data runs once, at first boot, so an edited template must not replace
  # a running cluster on the next apply. Changing admin_cidr when the admin's
  # address moves is an apply too.
  lifecycle {
    ignore_changes = [user_data]
  }

  depends_on = [hcloud_network_subnet.sub]
}
