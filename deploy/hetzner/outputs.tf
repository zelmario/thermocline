output "public_ips" {
  value = merge(
    { for k, s in hcloud_server.db : k => s.ipv4_address },
    { app = hcloud_server.app.ipv4_address },
  )
}

output "private_ips" {
  value = merge({ for k, v in local.dbs : k => v.ip }, { app = local.app_ip })
}

output "hot_uri" {
  value     = "mongodb://dba:${random_password.dba.result}@10.20.1.11:27017,10.20.1.12:27017,10.20.1.13:27017/?replicaSet=rsHot&authSource=admin"
  sensitive = true
}

output "cold_uri" {
  value     = "mongodb://dba:${random_password.dba.result}@10.20.1.21:27017,10.20.1.22:27017,10.20.1.23:27017/?replicaSet=rsCold&authSource=admin"
  sensitive = true
}

output "dba_password" {
  value     = random_password.dba.result
  sensitive = true
}

output "big_uri" {
  value     = var.big_server_type == "" ? "" : "mongodb://dba:${random_password.dba.result}@10.20.1.31:27017,10.20.1.32:27017,10.20.1.33:27017/?replicaSet=rsBig&authSource=admin"
  sensitive = true
}
