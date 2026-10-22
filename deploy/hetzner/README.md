# A staging pair on Hetzner Cloud

Every number in `docs/production-readiness.md` comes from six mongods on one
WSL2 host, and items 4 and 5 there cannot be closed on it. This directory builds
the same shape on real servers: a three-member fast tier (`rsHot`), a
three-member archive (`rsCold`), and one node for the router, the daemons and
the workload, all on one private network.

It costs money from the moment `apply` finishes until `destroy` does. Billing is
hourly.

## What you need

- A Hetzner Cloud API token with Read & Write permission, in
  `~/.config/hcloud/token` with mode 0600. `tf.sh` reads it from there, so it is
  never typed on a command line or written by Terraform.
- Terraform 1.5 or later, `jq`, Go to build `cline`, and an SSH key whose
  public half is at `~/.ssh/id_ed25519.pub` (or set `ssh_public_key_file`).

## Bring it up

Write `terraform.tfvars` (it is ignored by git):

```
db_server_type  = "<type>"
app_server_type = "<type>"
admin_cidr      = "<your address>/32"
```

Then:

```
$ bash tf.sh init
$ bash tf.sh plan
$ bash tf.sh apply
$ bash bootstrap.sh
```

`bootstrap.sh` waits for cloud-init on all seven nodes and stops if any of the
six mongods is on a different release (the daemons refuse such a pair anyway).
It initiates both sets, creates the `dba` account through the localhost
exception, copies `cline` to the app node, and writes
`/etc/thermocline/cline.yaml` there from `cline.example.yaml` with the two
private URIs filled in.

## What it does not do

- Nothing is reachable from the internet except SSH from `admin_cidr`. The
  dashboard is off in `cline.example.yaml`; set `router.dashboard:
  "127.0.0.1:27101"` and reach it with `ssh -L 27101:127.0.0.1:27101 root@<app>`.
- No TLS between nodes. The private network is the boundary, which is right for
  staging and not for production.
- The Terraform state holds the keyfiles and the `dba` password. It stays on
  this machine and is ignored by git.

## Park it between tests

With `cold_volume_gb` set, the archive's data lives on network volumes that
outlive its servers. Loading 200 million orders took 2h8m, so between tests
the servers go and the volumes stay:

```
$ bash park.sh      # stop mongod, unmount, remove every server
$ bash unpark.sh    # recreate them from terraform.tfvars and bootstrap
```

Measured on 2026-10-22: the archive came back with all 200,315,996 orders, its
indexes and its replica set, in 63 seconds of apply and 72 of bootstrap. The
fast tier and the app node start fresh. `bench -reset-shop` puts the archive
back the way the load left it, in about five minutes.

## Take it down

```
$ bash tf.sh destroy
```

The servers, the network, the firewall and the SSH key entry all go. Nothing
outside this directory's state is touched. The archive's volumes refuse to go
(`prevent_destroy`), so a destroy cannot take the data with it by accident:
remove them on purpose, by taking `prevent_destroy` out first.
