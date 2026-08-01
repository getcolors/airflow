# Configuring an airflow project

Desired state is one flat YAML map in `colors.yml`, found by walking up from the
working directory. It holds **non-secret values only**. Credentials arrive at run
time through `COLORS_PAR_*` environment variables, each named after the flat key
it fills: `walg-r2-bucket` would be overlaid by `COLORS_PAR_WALG_R2_BUCKET`.

A key that is **absent** is absent, and the block that reads it does not render.
A key holding `REPLACE_ME` is **present**, so the block renders the placeholder
verbatim. Required-but-unknown values therefore say `REPLACE_ME`; optional ones
are commented out rather than filled with a placeholder.

## The project

| Key | Meaning |
|---|---|
| `profile` | names the work directory, the OpenTofu state keys, the WAL-G object prefix and the ssh alias |
| `workdir` | where generated files go, resolved next to `colors.yml`. `.colors` |
| `compute-prevent-destroy` | `true` renders `lifecycle { prevent_destroy = true }` and refuses a real delete |

`profile` must be unique across every Colors project the user has. It is the
only thing separating this project's OpenTofu state from another's in a shared
bucket, for three of the four stages — see "State keys" below.

**Never export `COLORS_PAR_PROFILE`.** The overlay happens before any step runs,
so the package refuses to start rather than checking for a wrong value.

## Providers

| Key | Choices |
|---|---|
| `provider-compute` | `digitalocean`, `hcloud`, `oci`, `yandex`, `no-infra` |
| `provider-dns` | `cloudflare`, `no-infra` |
| `provider-smtp` | `resend`, `no-infra` |
| `provider-backend` | `local`, `s3`, `r2` |

Each selection brings its own required keys and credentials. The registry is
ONCE's, consumed as data, so it is the single place recording both.

### DigitalOcean

`digitalocean-name`, `-region`, `-size`, `-image`, `-ssh-keys`, and optionally
`-vpc-uuid`. Credential: `COLORS_PAR_DO_TOKEN`.

`digitalocean-ssh-keys` is interpolated into a single-element list, so exactly
one key is supported. A DigitalOcean VPC is region-scoped, so `-vpc-uuid` has to
live in `-region` or the apply fails on a mismatch rather than on anything
naming the real cause.

Size the box for what it must run continuously. 4 GB is the practical floor —
DAG parsing is what runs out of memory first, and it does so as import errors
rather than as an obvious OOM. Resizing a droplet's CPU and RAM is reversible;
growing its disk is not.

#### The firewall

`digitalocean-firewall: true` renders a `digitalocean_firewall` beside ONCE's
droplet. This is the package's own HCL — ONCE creates no firewall, and a droplet
without one sits on the public internet with every listening port exposed.

`digitalocean-ssh-sources` and `digitalocean-http-sources` are lists of CIDRs,
defaulting to the whole internet. Port 22 is open by design rather than by
omission: the DAG sync is pushed from GitHub-hosted runners whose addresses come
from large, changing ranges. What protects it is key-only authentication plus the
rrsync ForceCommand.

Managing the firewall in OpenTofu rather than with ufw on the box is deliberate:
a firewall configured only inside the machine is invisible to `build` and to
anyone reading desired state.

### Cloudflare

No non-secret keys. Credential: `COLORS_PAR_CLOUDFLARE_API_TOKEN`.

The zone is derived from `airflow-host` — its last two labels — and the token has
to reach it.

**This zone must not be one another Colors project already manages.** The reused
ONCE template manages nine `cloudflare_zone_setting` resources for the whole
zone, not just the A record. Two OpenTofu states co-owning those is invisible
while both apply identical values and destructive the first time either project
is deleted.

That constrains the zone as a whole: anything else served from it inherits
`ssl = strict`, `always_use_https`, `rocket_loader` and the rest.

### Resend

No non-secret keys — the relay is identical for every account. Credentials:
`COLORS_PAR_RESEND_API_KEY` and `COLORS_PAR_RESEND_PASSWORD`.

One sending domain, `notifications.<zone>`, is created and verified for you.
`airflow-smtp-from` must sit under it; a From address on the bare zone is not
what gets verified, and Resend rejects mail from it.

`provider-smtp: no-infra` points at an existing relay instead, and then needs
`no-infra-smtp-server`, `-port`, `-username` and
`COLORS_PAR_NO_INFRA_SMTP_PASSWORD`.

### State backends

`r2` needs `r2-bucket` and `r2-endpoint`, plus `COLORS_PAR_R2_ACCESS_KEY_ID` and
`COLORS_PAR_R2_SECRET_ACCESS_KEY`. `s3` needs `s3-bucket` and `s3-region` and
authenticates through OpenTofu's ambient AWS chain. `local` needs nothing and
keeps state in the work directory, which is fine for a throwaway and wrong for
anything a second person touches.

## Airflow

| Key | Meaning |
|---|---|
| `airflow-host` | the proxied A record, and the site address Caddy answers on |
| `airflow-image` | the Airflow image, with an explicit tag |
| `airflow-admin-username` | the username for the browser password prompt |
| `caddy-image` | the Caddy image, with an explicit tag |
| `airflow-smtp-from` | the envelope sender for Airflow's alerts |

Credentials: `COLORS_PAR_AIRFLOW_ADMIN_PASSWORD` and
`COLORS_PAR_AIRFLOW_FERNET_KEY`.

Both images are pinned rather than floating, and validation refuses a tag-less
one. An unpinned tag makes two creates months apart different deployments, and an
Airflow minor upgrade migrates the metadata database — something to do
deliberately, with a base backup taken first.

### Authentication is Caddy's, not Airflow's

`airflow-admin-username` and `COLORS_PAR_AIRFLOW_ADMIN_PASSWORD` are the
credentials for a `basic_auth` line in the Caddyfile. Airflow's api-server
publishes no port at all, so Caddy is the only route to it, and
`AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS` removes the second login screen
behind the first.

This is because Airflow 3 replaced the FAB auth manager with
`SimpleAuthManager`, which does not accept a password from configuration — it
generates one into a `.generated` file at startup. Setting an Airflow password
would mean writing that file ourselves and depending on its name.

The consequence is a real limit, not a detail: **there is no user model.** One
login for one operator, covering the REST API as well as the UI. A second person
means Cloudflare Access or FAB, not another row.

The bcrypt hash is computed on the machine and never rendered into `.colors/`.
Rotating the password converges: change the variable and run `create` again.

### The Fernet key

`COLORS_PAR_AIRFLOW_FERNET_KEY` encrypts every stored Airflow connection. It is
**part of the backup**: restoring with a different one leaves those connections
undecryptable, and the deployment looks healthy until a DAG uses one. Generate it
once, store it where it survives the machine, and never regenerate it for an
existing deployment.

```sh
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### TLS

Caddy obtains its own certificate from Let's Encrypt over HTTP-01. That is not
optional decoration: the zone is set to `ssl = strict`, so Cloudflare validates
the origin certificate rather than accepting anything.

`caddy-acme-email` is optional and gets expiry and problem notices from the CA.
Comment it out rather than leaving `REPLACE_ME` — the placeholder would render
into the Caddyfile verbatim as an address to register.

## DAGs

| Key | Meaning |
|---|---|
| `dags-repo` | `owner/name`. Created by the package if missing, seeded once |
| `dags-dest` | the absolute directory rsync writes into, and Airflow scans |
| `dags-branch` | the branch the seeded workflow syncs from |

Credential: `COLORS_PAR_GITHUB_TOKEN`, which needs only the `repo` scope.

**Not `workflow`**, and that is deliberate. The seeded deploy workflow lives at
`.github/workflows/deploy-dags.yml`, and writing that path through the REST API
would require the `workflow` scope — which GitHub gates separately, because a
workflow file is arbitrary code execution in CI with that repository's secrets.
Granting it would widen this token across every repository the org can see, for
the sake of one example file, and would contradict the posture everywhere else
here: the deploy key is write-only, confined to one directory, with no sudo.

So the seed is a **git push over SSH** instead. A push carries no OAuth scope,
so the operator's own key does it, exactly as they would by hand. That needs an
SSH key GitHub accepts — check with `ssh -T git@github.com`. The token stays
narrow and is used only for the repository, the environment and the secrets.

The repository is created **private**. DAGs carry business logic, and a public
default is a mistake you only make once.

Seeding is once-only. If the repository already exists its contents are left
entirely alone and only the environment secrets are reconciled — Colors converges
on every create, and a converging seed would overwrite real DAGs with the
example.

A deploy keypair is generated on **every** create, nothing is stored, and the
private half is published to an Actions environment named after the profile. The
box retains one previous generation, so a publication that fails leaves the old
key working until the next create prunes it.

### The ForceCommand

The key's authorized_keys entry is:

```
restrict,command="/usr/local/bin/rrsync -wo <dags-dest>" ssh-ed25519 …
```

`rrsync` ships with rsync and exists for exactly this. `-wo` makes it
write-only, so a leaked key can write DAGs into one directory, cannot read them
back, cannot run a command, and needs **no sudo at all** — Airflow's
dag-processor rescans on a timer, so nothing has to be restarted after a sync.

`dags-dest` must be absolute. A relative path would confine the key relative to
the login user's home, which is neither what the ForceCommand line says nor what
the compose file bind-mounts.

## Postgres and WAL-G

| Key | Meaning |
|---|---|
| `postgres-version` | the PGDG major version, as an integer |
| `walg-version` | the pinned WAL-G release tag, e.g. `v3.0.8` |
| `walg-r2-bucket` | the backup bucket — **not** the state bucket |
| `walg-r2-endpoint` | the S3 API endpoint for that bucket |
| `walg-r2-region` | `auto` for R2 |
| `walg-full-backup-oncalendar` | a **systemd OnCalendar** expression, not a crontab line |
| `walg-retain-full` | how many base backups to keep |
| `walg-max-backup-age-hours` | when the freshness check starts alerting |
| `alerts-email` | where a failed or missing backup reports to |

Credentials: `COLORS_PAR_POSTGRES_PASSWORD`,
`COLORS_PAR_WALG_R2_ACCESS_KEY_ID`, `COLORS_PAR_WALG_R2_SECRET_ACCESS_KEY`.

Postgres runs **on the host**, not in a container, because `archive_command` has
to invoke the wal-g binary in the same filesystem as the postgres process. It
comes from PGDG rather than the distro, which is what makes `postgres-version`
mean anything: Ubuntu 24.04 ships exactly one major version.

**The version pin is load-bearing.** WAL-G backups do not restore across Postgres
major versions, so an unpinned version turns a distro upgrade into an
unrestorable archive.

Use a bucket of its own. Retention and lifecycle rules that are right for WAL are
wrong for OpenTofu state, and these credentials live on a public-facing box while
the state credentials never leave the workstation. Scope `COLORS_PAR_WALG_R2_*`
to that bucket alone.

`walg-full-backup-oncalendar` is systemd syntax: daily at 02:00 is
`"*-*-* 02:00:00"`, not `"0 2 * * *"`. Validation catches the crontab shape.

`walg-max-backup-age-hours` should be comfortably above the backup interval. 30
rather than 24 for a daily schedule, from the arithmetic: a backup starting at
02:00 and taking an hour leaves the newest completed one about 25 hours old just
before its successor lands. 24 would page every morning.

### Two failure directions, both covered

- The backup timer carries `Persistent=true`, so a run missed while the machine
  was down fires on boot instead of being skipped, and `OnFailure=` mails
  `alerts-email` when a run fails.
- A **second** timer checks freshness hourly and fails when the newest base
  backup is older than the limit. It alerts on *absence*, which `OnFailure`
  structurally cannot: a timer that never runs never fails.

A first base backup is taken during the create itself, so there is a restore
point from minute one rather than from the first scheduled run.

What remains uncovered, and worth saying out loud: if the notifier breaks or both
timers are masked, silence returns. Closing that needs a dead man's switch off
the box.

## State keys

Remote state is keyed `<profile>/<stage>.tfstate`:

```text
<profile>/airflow-compute.tfstate     this package's own stage
<profile>/tofu-dns.tfstate            ONCE's name
<profile>/tofu-smtp.tfstate           ONCE's name
<profile>/tofu-smtp-post.tfstate      ONCE's name
```

The compute stage is deliberately not called `tofu-compute`, so a colliding
profile still cannot produce ONCE's compute state key. **The other three get no
such protection**: each ONCE step computes its own directory internally, so
renaming would mean forking them and forfeiting the reuse that motivated
delegating at all.

For those three, `profile` alone separates this project from an ONCE project in
the same bucket. Choose it accordingly.

The WAL-G archive is scoped the same way, under `s3://<walg-r2-bucket>/<profile>`.

## Credential summary

```sh
# .envrc.private — gitignored, never committed
export COLORS_PAR_DO_TOKEN=…                     # compute
export COLORS_PAR_CLOUDFLARE_API_TOKEN=…         # dns
export COLORS_PAR_RESEND_API_KEY=…               # smtp
export COLORS_PAR_RESEND_PASSWORD=…              # smtp relay
export COLORS_PAR_GITHUB_TOKEN=…                 # the DAG repository
export COLORS_PAR_R2_ACCESS_KEY_ID=…             # opentofu state
export COLORS_PAR_R2_SECRET_ACCESS_KEY=…
export COLORS_PAR_WALG_R2_ACCESS_KEY_ID=…        # backups — a different bucket
export COLORS_PAR_WALG_R2_SECRET_ACCESS_KEY=…
export COLORS_PAR_POSTGRES_PASSWORD=…            # the metadata database
export COLORS_PAR_AIRFLOW_FERNET_KEY=…           # KEEP THIS — see above
export COLORS_PAR_AIRFLOW_ADMIN_PASSWORD=…       # the browser prompt
```

The five that reach the machine — the WAL-G pair, the database password, the
Fernet key, the admin password, and the relay password — are read from the
process environment by Ansible and written into 0600 files on the host. They
never pass through `.colors/`.
