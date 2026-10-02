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

## Take it down

```
$ bash tf.sh destroy
```

The servers, the network, the firewall and the SSH key entry all go. Nothing
outside this directory's state is touched.
