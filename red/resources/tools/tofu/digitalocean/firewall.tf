# Rendered beside ONCE's main.tf, never instead of it. OpenTofu merges every
# .tf in a directory, so this adds a firewall without forking the upstream
# template — the same trick walter uses to publish an instance id.
#
# ONCE creates no firewall for DigitalOcean, and a droplet without one is on the
# public internet with every listening port exposed. Managing it here rather
# than with ufw on the box is deliberate: a firewall configured only inside the
# machine is invisible to `build` and to anyone reading desired state.
#
# It references digitalocean_droplet.node1, which is ONCE's resource address.
# scripts/golden.sh asserts that address still exists upstream, because a rename
# there would otherwise surface as an opaque `tofu validate` failure during a
# real apply, half way through a create.

resource "digitalocean_firewall" "airflow" {
  name        = "<{ digitalocean-name }>-airflow"
  droplet_ids = [digitalocean_droplet.node1.id]

  # Port 22, open to the world on purpose rather than by omission. The DAG sync
  # is pushed from GitHub-hosted runners, whose addresses are drawn from large
  # ranges that change, so pinning this would break every deploy. What protects
  # the port is key-only authentication plus the rrsync ForceCommand, which
  # confines the deploy key to writing under dags-dest and nothing else.
  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = <{ ssh-sources-hcl|safe }>
  }

  # 80 as well as 443: Caddy answers the ACME HTTP-01 challenge there, and the
  # zone's always_use_https redirects the rest.
  inbound_rule {
    protocol         = "tcp"
    port_range       = "80"
    source_addresses = <{ http-sources-hcl|safe }>
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "443"
    source_addresses = <{ http-sources-hcl|safe }>
  }

  # Egress is unrestricted. This box pulls container images, distro packages and
  # ACME certificates, and pushes WAL to R2 and mail to Resend. Narrowing it
  # would mean tracking four vendors' address ranges to no real benefit while
  # ingress is the exposed side.
  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}
