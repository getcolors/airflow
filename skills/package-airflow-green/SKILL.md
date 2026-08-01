---
name: package-airflow-green
description: Creates and operates a single-node Apache Airflow server with Green, OpenTofu and Ansible — Airflow under Docker with LocalExecutor, a host Postgres backed up to R2 with WAL-G, TLS and authentication through Caddy, and DAGs pushed from GitHub Actions over rsync. Use when initializing an airflow project, generating colors.yml, selecting a compute, DNS, SMTP or state provider, building or dry-running configuration, provisioning or destroying the server, or restoring the metadata database.
license: MIT
---

# An Apache Airflow server, with Colors

Use this skill to initialize or operate an airflow project in the user's current
directory. It provisions one VPS running Airflow, a host Postgres archiving
continuously to object storage, and a Caddy that terminates TLS and holds the
only login. DAGs are pushed to it from a GitHub Actions workflow.

## Requirements

Babashka runs the launcher. `create` and `delete` also need OpenTofu, Ansible
and the `gh` CLI. Provider credentials use `COLORS_PAR_*` variables, except OCI,
which uses the profile named in `~/.oci/config`, and S3, which uses OpenTofu's
ambient AWS credential chain.

## Non-negotiable safety rules

- Never ask the user to paste a secret into chat.
- Never put API tokens, passwords, private keys, Fernet keys or access keys in
  `colors.yml`, in the `airflow` launcher, in shell history, or in generated
  examples. Every credential arrives through a `COLORS_PAR_*` environment
  variable named after the key it fills. Suggest a gitignored `.envrc.private`,
  never an inline export a shell history records.
- **Never set `COLORS_PAR_PROFILE`.** The package refuses to run when it is set,
  and suggesting it as a workaround defeats the guard. It matters more here than
  in the other packages in this stack: three of the four OpenTofu stages carry
  ONCE's stage names, so the profile is the *only* thing separating this
  project's state from another Colors project's in the same bucket.
- **`COLORS_PAR_AIRFLOW_FERNET_KEY` is part of the backup.** It encrypts every
  stored Airflow connection. Restoring a database with a different one leaves
  those connections undecryptable — a broken restore that looks like a
  successful one until a DAG uses a connection. Never regenerate it for an
  existing deployment, and make sure the user has it stored somewhere that
  survives the machine.
- Do not overwrite an existing `airflow` launcher or `colors.yml` without
  explicit approval. If a project is already valid, operate it rather than
  regenerating it.
- Default to `build` and `create --dry-run`. Run a real `create` or `delete`
  only after the user confirms that exact operation.
- `build` and `create --dry-run` are credential-free by design and check no
  `COLORS_PAR_*` at all. A clean dry-run says nothing about whether real
  provisioning would authenticate; never report it as credential validation.
- **Before `delete`, say plainly that the metadata database goes with the
  droplet.** Every DAG run history, connection, variable and XCom lives on the
  boot volume. It is recoverable from the WAL-G archive by the documented
  procedure below, and that procedure is manual. `compute-prevent-destroy`
  defaults to `true`; authorize an intentional delete with
  `COLORS_PAR_COMPUTE_PREVENT_DESTROY=false` rather than editing desired state.
- `delete` does **not** delete the DAG repository, and must not be described as
  if it might. It revokes the deploy key and clears the Actions environment.
- Never edit anything under `.colors/` — it is generated output.

Read [references/configuration.md](references/configuration.md) before
generating or changing desired state, and before any real `create` or `delete`.

## Commands

```sh
./airflow build              # render .colors/<profile>/ only; contacts nothing
./airflow create --dry-run   # print the graph; touches nothing
./airflow create             # provision, configure, and publish the deploy key
./airflow delete             # revoke the key, then destroy — the repo is kept
```

`-f/--file` overrides the `colors.yml` found by walking up from the working
directory.

There is no `stop`, no `start` and no `describe`. The power verbs are walter's
and are implemented for OCI only; an Airflow scheduler runs continuously anyway,
so a box that cannot be parked costs nothing that was not already being paid.

## What a create does, in order

```text
start ─ compute ─ smtp ─ dns ─ smtp-post ─┬─ ansible-local
                                          ├─ ansible-remote
                                          └─ github
```

1. **compute** provisions the VPS and, on DigitalOcean, a firewall.
2. **smtp** registers `notifications.<zone>` at Resend.
3. **dns** points the host at the machine and publishes the verification records.
4. **smtp-post** verifies the sending domain now that DNS resolves.
5. **ansible-local** writes a `Host <profile>` block into `~/.ssh/config`.
6. **ansible-remote** installs Docker, Postgres, WAL-G, Airflow, Caddy and the
   deploy account.
7. **github** creates the DAG repository if it is missing, publishes the deploy
   key to an Actions environment named after the profile, and — only for a
   repository this run created — seeds a workflow and a hello-world DAG.

The SMTP ordering is why steps 2–4 cannot be collapsed: the sending domain must
exist before its verification records can be rendered into DNS, and DNS must be
live before verification runs.

On `delete` the graph reverses and `github` runs **first**, so credentials are
withdrawn before anything is destroyed.

## After the first create

Tell the user these four things, because none of them are obvious:

1. **The web UI is at `https://<airflow-host>`**, behind a browser password
   prompt. The username is `airflow-admin-username`; the password is
   `COLORS_PAR_AIRFLOW_ADMIN_PASSWORD`. That prompt is Caddy's, not Airflow's —
   there is one login for one operator and no user model behind it.
2. **The certificate takes a minute.** Caddy answers an ACME HTTP-01 challenge
   on port 80 the first time. Until it completes, Cloudflare returns 526,
   because the zone is set to `ssl = strict` and there is no origin certificate
   yet. That is the expected first-minute state, not a broken deploy.
3. **DAGs are deployed by pushing to the repository**, not by copying files to
   the server. The seeded workflow syncs `dags/` on every push to
   `dags-branch`. Airflow's dag-processor rescans on a timer, so nothing needs
   restarting and the deploy key needs no sudo.
4. **A base backup was taken during the create.** After that they run on
   `walg-full-backup-oncalendar`, and a second timer alerts `alerts-email` if
   the newest one ever ages past `walg-max-backup-age-hours`.

## Restoring the metadata database

There is deliberately **no `restore` verb**. A command whose purpose is
overwriting a live database, one typo away from `create` in the same CLI, is a
hazard that outweighs the convenience. Restoring is manual, and it goes:

```sh
# On the machine, as root.
systemctl stop docker                        # nothing may write while this runs
systemctl stop postgresql@16-main
rm -rf /var/lib/postgresql/16/main           # WAL-G restores into an empty dir
sudo -u postgres /usr/local/bin/wal-g-wrapper backup-fetch \
  /var/lib/postgresql/16/main LATEST

# Tell Postgres to replay the archive, then let it start.
sudo -u postgres tee /var/lib/postgresql/16/main/recovery.signal </dev/null
sudo -u postgres tee -a /etc/postgresql/16/main/conf.d/airflow.conf <<'EOF'
restore_command = '/usr/local/bin/wal-g-wrapper wal-fetch %f %p'
EOF
systemctl start postgresql@16-main           # watch the log until recovery ends
systemctl start docker
```

Before recommending it, check three things with the user:

- **The Fernet key must be the one the backup was taken with.** See the safety
  rules above. This is the failure that looks like a success.
- **The Postgres major version must match.** WAL-G backups do not restore across
  major versions, which is why `postgres-version` is pinned. Replace `16` above
  with whatever that key says.
- **`walg-r2-bucket` and the profile must match**, because the archive is stored
  under `s3://<bucket>/<profile>`. Restoring into a project with a different
  profile finds nothing.

## Reporting

Say what actually happened. If Ansible failed at a task, name the task. If a
`gh` call failed, say which credential it was publishing. Never report a
successful `build` as evidence that a `create` would work — it renders from
desired state alone and contacts nothing.
