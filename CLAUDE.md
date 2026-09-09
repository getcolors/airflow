# Airflow package

Three interchangeable implementations provision one Apache Airflow server with
Docker LocalExecutor, host Postgres, WAL-G backups, Caddy authentication, and
GitHub Actions rsync DAG deployment. Read historical plans as design history.

## Compute and application ownership

Every color directly pins colors-compute. The machine adapter declares one host
and TCP 22/80/443 rules with explicit CIDRs. The library owns provider validation,
credentials, network, firewall, SSH keys and R2/S3 coordination, shared and node
state. New providers need only the direct dependency version changed.
No-infra compute and local compute state are removed.

ONCE supplies DNS, SMTP and SMTP verification. Their stage names and state keys
stay tofu-dns, tofu-smtp and tofu-smtp-post. The adapter publishes the returned
node under once/compute-params for DNS and flat for Ansible. No live fallback IP
is permitted. Build placeholders come from the library plan.

Create runs compute, SMTP, DNS and SMTP verification in series. Local SSH config
and remote Ansible follow. GitHub seed publication waits for remote Ansible to
install its restricted deploy key. Delete first inspects recorded library
inventory, then revokes GitHub credentials, cleans SSH config, deletes DNS/SMTP,
and destroys compute. A failed inventory read or cleanup stops later actions.
Delete keeps the DAG repository. compute-prevent-destroy defaults true.

Legacy <profile>/airflow-compute.tfstate requires explicit migration. Never
blindly converge a legacy deployment with the new library. Profile overrides
are refused because profile identifies state, backups and SSH aliases.

SSH config uses the package-owned canonical atomic updater. Only managed mode
adds IdentityFile/IdentitiesOnly. Ansible and the GitHub host-key probe receive
the observed login and explicit private-key path for either key mode.
Application deploy keys are separate from library compute keys.

### The machine, and its three ordering constraints

The playbook's order is load-bearing in three places, each marked where it
happens:

1. **Docker before Postgres.** `/etc/docker/daemon.json` pins `bip` to
   172.17.0.1/16, and Postgres binds that address for the containers. Postgres
   does not start at all when it cannot bind a configured address, so an unpinned
   bridge is a first boot that looks like a broken database rather than a moved
   bridge. A `flush_handlers` and an assertion sit between the two.
2. **WAL-G before archiving is switched on.** The other way round, Postgres
   starts trying to archive with no binary to run and pg_wal grows until the boot
   volume is full.
3. **The deploy account last.** It is the only thing on the machine reachable
   from the internet by design.

`pg_hba.conf` admits `172.16.0.0/12` with `scram-sha-256`, added with
`blockinfile` rather than an `include` directive — `include` in `pg_hba.conf` is
a Postgres 16 feature and `postgres-version` is a key someone can set to 14.

Everything uses `ansible.builtin` only. The `community.postgresql` modules would
need psycopg2 installed on the managed node, which is a Python database driver on
a server whose only client is a container.

### Authentication is Caddy's

Airflow 3 replaced FAB with `SimpleAuthManager`, which does not accept a password
from configuration — it generates one into a `.generated` file at startup. So
authentication is a `basic_auth` line in the Caddyfile, and the api-server
publishes **no port at all**, making Caddy the only route to it.

The bcrypt hash is generated with `htpasswd` on the machine and never rendered.
`htpasswd` rather than `caddy hash-password` because it can also *verify*: bcrypt
is salted, so regenerating unconditionally would rewrite `caddy.env` on every
converge and bounce the only thing serving the site.

Two secrets — `AIRFLOW__API__SECRET_KEY` and `AIRFLOW__API_AUTH__JWT_SECRET` —
are generated on the machine and kept. They default to a value Airflow makes up
per process, which is fine for one process and wrong for five: the api-server
signs task-identity JWTs the scheduler presents back. Neither encrypts anything
persisted, which is why they are generated rather than being desired state —
unlike `COLORS_PAR_AIRFLOW_FERNET_KEY`, which encrypts stored connections and
must survive a rebuild.

### The ForceCommand is the security crux

ONCE's `deploy` **ignores** `SSH_ORIGINAL_COMMAND`. rsync cannot work that way —
rsync-over-SSH runs `rsync --server` on the remote, so a forced command that
permits rsync must parse and validate what the client asked for. Hand-rolling
that validator is how people get owned.

So the forced command is `rrsync`, which ships with rsync and exists for exactly
this, locked write-only to `dags-dest`. It needs **no sudo at all**, unlike
ONCE's key: Airflow's dag-processor rescans on a timer, so nothing is restarted
after a sync. Strictly smaller blast radius than the design it is copied from.

The authorized-keys reconciler is ONCE's algorithm ported to Python rather than
transplanted as Babashka — the machine already has Python, and putting a whole
extra runtime on an Airflow server for one forty-line script is what the plan
rejected when it declined to reuse ONCE's Ansible stage wholesale.

## Development and verification

Run Blue pytest, Red bun test and typecheck, Green bb test, scripts/parity.sh,
scripts/golden.sh, scripts/launcher.sh and scripts/ssh-config-probe.py with Ansible.
Use temporary work directories. Never read private environment files or live
.colors output. Preserve the application ordering and secret lookup tests.
Golden differences need review before acceptance. Keep all colors equivalent.

## Documentation

`index.html` is this repository's landing page and carries two analytics tags:
GA4 measurement ID `G-4VKP1WY4QJ`, whose explicit `page_title` must exactly
equal the decoded HTML `<title>` and stay distinct and stable so one Analytics
property can separate repositories, and the self-hosted Rybbit snippet
`<script src="https://rybbit.getcolors.ai/api/script.js" data-site-id="9fb9c41a6d49" defer></script>`,
which shares one site ID across every page because `getcolors.github.io/<repo>/`
paths already encode the repository. Never add one tag without the other.

## Publishing

The user must authorize commits and pushes. Publish source first. Run bb pin
from a clean pushed checkout, then commit and push the launcher stamps. Do not
hand-edit the package SHA. Verify all three copied launchers from temporary
projects without local overrides before copying them into deployment projects.
