# Airflow v1 — a single-node Airflow server as a Package Skill

Status: accepted, being built. Revised 2026-08-01 with what implementation found.
Date: 2026-08-01.

This records why the design is what it is, including the alternatives that were
rejected and why. It is history, not specification — read the code before acting
on anything here. Sections marked **revised** replaced an earlier decision that
turned out not to survive contact with the code; the original reasoning is kept
so the change is legible rather than silent.

## What airflow is

`airflow` provisions and operates one **Apache Airflow server**: a single VPS
running Airflow under Docker with LocalExecutor, a host Postgres for the
metadata database, continuous archiving to R2 with WAL-G, TLS through Caddy, and
a public hostname behind Cloudflare. DAGs are pushed to it from a GitHub Actions
workflow over rsync, using a disposable deploy key.

It is the third Package Skill on the Colors SDK, after ONCE and walter, and like
walter it is **green only** — no red, no blue, no parity harness.
`scripts/golden.sh` is the regression net, and it carries more weight here than
it does in walter (see "The reuse surface").

The first consumer is `airflow-digitalocean/`, whose `colors.yml` is a complete
and heavily commented statement of the configuration surface. Read it alongside
this document; between them they are the specification.

## Build status

| Part | State |
|---|---|
| `deps.edn`, `bb.edn` | done — `clojure -P` resolves green and once |
| `utils.clj` | done — contract, host alias, the ONCE adapter |
| `validate.clj` | done — zero state errors against the real `colors.yml` |
| `tools.clj` — compute + the three delegated stages | done |
| `tofu/digitalocean/firewall.tf` | done |
| `ansible-local` stage | done |
| `ansible-remote` stage | done — playbook syntax-checks under ansible-core 2.21 |
| `github.clj` | done |
| `workflow.clj`, launcher, skill payload | done — `./scripts/launcher.sh`, 11 checks |
| tests | done — `bb test`, 85 tests / 295 assertions |
| `scripts/golden.sh` | done — 10 variants + 16 assertions |
| a real `create` | **run — Airflow is live and serving**; see "What only a real create found" |
| the DAG deploy path | **blocked** — needs `workflow` scope on `COLORS_PAR_GITHUB_TOKEN` |

That last line was true for the first three revisions of this document and is
not any more. Airflow 3.1.3 is serving on the real host, behind Caddy, with
Postgres 16 archiving to R2 and both backup timers armed. What remains unproven
end to end is the DAG deploy path, which is blocked on a token scope rather than
on anything in this package.

## Decisions

| Decision | Chosen | Why |
|---|---|---|
| Executor | LocalExecutor | One box. Celery adds Redis, workers and a broker for no benefit at this size. |
| Metadata DB | Postgres **on the host**, not in a container | `archive_command` must invoke the `wal-g` binary in the same filesystem as the postgres process. Containerising it means building and hosting a custom image — infrastructure this stack does not have. |
| Backups | WAL-G to its own R2 bucket | Retires the block-volume question entirely. Data lives on the boot volume; recovery is recreate-and-restore. |
| Restore | Documented procedure, **not a verb** | A command whose purpose is overwriting a live database, one typo from `create` in the same CLI, is a hazard that outweighs the convenience. |
| Exposure | Public, behind Cloudflare | Chosen over private-only. A seam is left for Cloudflare Access. |
| Authentication | **Caddy `basic_auth`**, Airflow open behind it (revised) | See "Authentication moved to the edge". |
| TLS | Caddy automatic HTTPS, HTTP-01 | See "TLS through a proxied, strict-SSL zone". |
| DAG delivery | Push from GitHub Actions over rsync | No key on the machine for pulling, no timer, no agent forwarding. |
| ForceCommand | `rrsync -wo <dags-dest>` | See "The ForceCommand". |
| DAG repo | **Created** by the package, seeded once | The github stage creates `dags-repo` private, seeds a workflow and a hello-world DAG, and never writes content again. |
| Colours | green only | Same reasoning as walter: no second colour to match. |
| Power verbs | Not implemented for DigitalOcean in v1 | `stoppable` is `#{}`. An Airflow scheduler runs continuously; a box you cannot park costs nothing you were not already paying. |

## The DAG

`wire-fn` returns a different graph per `:green/event`, the mechanism ONCE and
walter both use.

```text
create / build   start ─ compute ─ smtp ─ dns ─ smtp-post ─┬─ ansible-local
                                                           ├─ ansible-remote
                                                           └─ github

delete           start ─ github ─ ansible-cleanup ─ smtp-post ─ dns ─┬─ smtp
                                                                     └─ compute
```

This is ONCE's shape, not walter's, and the SMTP ordering is why. The Resend
sending domain must exist before its verification records can be rendered into
DNS, and DNS must be live before verification runs — so `smtp → dns → smtp-post`
cannot be collapsed into fewer stages.

Compute and SMTP run in **series** here where ONCE forks them. ONCE's
`joined-params` reads the compute output out of `:green/branches` at the DNS
join; running in series means it reads it off `opts` instead, which is the same
function's second fallback. One less concurrency edge for no lost time — the
SMTP stage is an API call, not a machine build.

`github` runs last on create, after `ansible-remote`, for ONCE's reason: the
credentials it publishes describe a configured host, and a workstation-side
failure should not gate them. On delete it runs **first**, revoking before
anything is destroyed — a withdrawn credential against a live host is a loud,
recoverable broken deploy, while a live credential against a destroyed host is
silent.

`delete` never deletes the DAG repository. Destroying compute is recoverable
through WAL-G; destroying the repository is not, and it is the one artifact here
that cannot be rebuilt from desired state.

## The reuse surface — the widest in the stack

Walter consumes exactly two things from ONCE and its `CLAUDE.md` is emphatic
about keeping it that way. This package consumes six:

1. `io.github.getcolors.once.validate/providers` — the provider registry, as data.
2. The compute templates, by classpath keyword.
3. `tools/tofu-smtp-step` — the function, not just the template.
4. `tools/tofu-dns-step` — likewise.
5. `tools/tofu-smtp-post-step` — likewise.
6. `tools/tool-dir` — likewise, so the backend advice writes `backend.tf.json`
   into exactly the directory the delegated step will run in. Two copies of that
   resolution agreeing today is not the same as agreeing after a pin bump.

Items 3–6 are a **code-level API**, not data. Nothing upstream promises any of
it; ONCE's own rules treat its internals as free to move as long as three
colours move together. Walter deliberately refused to reuse ONCE's
`ansible-local` for a much smaller version of this exposure.

This was a deliberate trade — the instruction was to work like ONCE and reuse as
much as possible, and the DNS and SMTP stages are most of ONCE's value. The
consequence is that `golden.sh` covering every one of those renders is not a
nice-to-have. It is the only thing standing between an ONCE refactor and a
silently broken Airflow deploy.

**Bump the ONCE pin deliberately and rarely.** Run `bb golden` immediately after
and read the diff rather than accepting it.

### The adapter, and why desired state is not ONCE-shaped

`airflow-host` is a flat key. The `once: applications:` map is assembled at the
call boundary, immediately before handing `opts` to ONCE's step:

```clojure
(defn once-applications [opts]
  {:applications [{:host (:airflow-host opts)}]})
```

One line, in `utils.clj`. It keeps ONCE's internal data shape out of the
documented configuration surface, so an upstream rename is a one-line fix here
rather than a breaking change to every consumer's `colors.yml`.

No `:image`, `:github` or `:env` on that entry: those drive ONCE's container
deployment, which this package does not use. Only the host reaches the DNS and
SMTP templates.

### The delegated stages keep ONCE's names — revised

The original design named the stages `airflow-dns`, `airflow-smtp` and
`airflow-smtp-post`, on walter's reasoning that a package-specific stage name
means a colliding `profile` still cannot address another package's state.

**That is not achievable while delegating to ONCE's step functions.** Each of
them computes its own directory internally — `(tool-dir opts "tofu-dns")` is
hard-coded inside `tofu-dns-step` — so the stage name is ONCE's, not a
parameter. Renaming would mean forking the three steps, which forfeits exactly
the reuse that motivated delegating to them.

So the state keys are:

```text
<profile>/airflow-compute.tfstate     this package's own step
<profile>/tofu-dns.tfstate            ONCE's name
<profile>/tofu-smtp.tfstate           ONCE's name
<profile>/tofu-smtp-post.tfstate      ONCE's name
```

For the three delegated stages, **`profile` alone separates this project from
once-colors**, where walter has two independent separations.
`airflow-digitalocean` and `once-colors` do differ, so nothing collides today —
but the belt-and-braces property walter has is genuinely absent here, and that
is the price of the reuse.

A second consequence, and the sharper one: ONCE's `tofu-dns-step` reads
`:once/compute-params` to find the address it points DNS at. Its own compute
step sets that key; this package's compute step is its own, so **it must publish
`:once/compute-params` too**. Dropping it would not fail — it would fall through
to ONCE's `fallback-compute-params` and point every A record at `192.168.0.1`,
a create that succeeds and resolves nowhere. That is an undocumented internal
contract, not an API, and it is precisely the fragility this section is about.
`golden.sh` asserts the rendered A record carries the machine's address.

### Records inherit ONCE's namespace

`add-fqn-suffix ::app-dns` uses the calling namespace, so this package's DNS
records get addresses like
`cloudflare_dns_record.io_github_getcolors_once_tools_app_dns_<host>` in
**airflow's** state. Cosmetic, but serialised into state — changing it later is a
`tofu state mv`, not an edit. Accepted rather than forked.

### Zone settings are why the domain differs

ONCE's Cloudflare template manages nine `cloudflare_zone_setting` resources for
the whole zone, not only the A record. Two OpenTofu states co-owning those is
invisible while both apply identical values and destructive the first time
either project is deleted. `airflow.bigconfig.online` is therefore on a zone
once-colors does not manage — it holds `getcolors.ai` and `bigconfig.ai`.

This constrains the zone as a whole: anything else served from `bigconfig.online`
inherits `ssl = strict`, `always_use_https`, `rocket_loader` and the rest.

## TLS through a proxied, strict-SSL zone

Caddy gets its certificate from Let's Encrypt over **HTTP-01**, the same way
ONCE's proxy does, and the firewall opens 80 for that reason as much as for the
redirect.

This deserves stating because it looks like it cannot work. The A record is
proxied, `always_use_https = on` would redirect the challenge to HTTPS, and
`ssl = strict` means Cloudflare then refuses an origin without an already-valid
certificate — a chicken-and-egg that ends in a 526.

The reason it works anyway is that Cloudflare does not apply Always Use HTTPS to
`/.well-known/acme-challenge/`, so the challenge reaches the origin over plain
HTTP. The evidence is not a doc page: **once-colors runs these exact zone
settings and `https://www.getcolors.ai` answers 200 through Cloudflare**, which
under `strict` is only possible with a publicly trusted certificate on the
origin, obtained this way.

Rejected:

- **Cloudflare Origin CA** (`cloudflare_origin_ca_certificate`). Robust and
  ACME-free, but the private key is generated by `tls_private_key` and therefore
  lands in OpenTofu state, which lives in R2. Trading a working mechanism for a
  secret in a state file is a bad trade.
- **DNS-01 with the Cloudflare Caddy module.** Needs a custom Caddy build and a
  zone-scoped Cloudflare API token *on the machine* — a credential that can edit
  every record in the zone, sitting on the internet-facing box, to solve a
  problem that is not occurring.
- **Turning the record off the proxy.** ONCE's `render-fn` hard-codes
  `proxied: true`. Changing it means forking the DNS render, which is item 4 of
  the reuse surface.

## Authentication moved to the edge — revised

The original plan said "Airflow's own auth". That was written against Airflow 2's
FAB model, where `airflow users create` takes a username and password. **Airflow
3 changed it**: FAB is no longer the default auth manager, and the default
`SimpleAuthManager` does not accept a password from configuration — it generates
one into `simple_auth_manager_passwords.json.generated` at startup. Setting the
admin password would mean writing that file ourselves and depending on its
name.

So authentication sits in **Caddy**:

- `basic_auth` with a bcrypt hash of `airflow-admin-password`, computed on the
  machine at play time with `caddy hash-password` and never rendered into
  `.colors/`.
- Airflow's api-server binds **127.0.0.1:8080**, so Caddy is the only route to
  it. `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true` then removes the
  second login screen behind the first.

This is more robust than it is elegant. It survives Airflow's auth manager
churn, it covers the REST API as well as the UI, and it keeps one credential.
The cost is that Airflow has no user model: there is one login for one operator,
and multi-user access means moving to Cloudflare Access or FAB, not adding rows.

`airflow-admin-username` and `airflow-admin-password` keep their names — they are
still "the admin login" — but they now name the Caddy credentials.
**`airflow-admin-email` no longer has a job** and comes out of `colors.yml` and
out of `validate/own-required`; leaving a key that reaches no rendered file would
make the required list a lie.

## Postgres, WAL-G and restore

Postgres runs on the host from the PGDG apt repository, pinned by
`postgres-version`. The pin is load-bearing rather than tidy: **WAL-G backups do
not restore across Postgres major versions**, so an unpinned version turns a
distro upgrade into an unrestorable archive. PGDG rather than the distro package
so the pin means something — Ubuntu 24.04 ships exactly one major version, and
`postgres-version` would otherwise be documentation.

### Reaching it from the containers

Airflow's containers reach Postgres over the Docker bridge. Postgres binds
`localhost` and the bridge gateway only — never a public interface, which matters
more than usual because a DigitalOcean droplet has no firewall unless one is
created, and `digitalocean-firewall: false` is a configuration a user can choose.

That requires knowing the bridge address before Postgres starts, so
`/etc/docker/daemon.json` **pins `bip` to 172.17.0.1/16** and Docker installs
before Postgres. Left to Docker's own allocation the address is stable in
practice and unpinned in principle, and Postgres does not start at all when it
cannot bind a configured address — a first-boot failure that looks like a broken
database rather than a moved bridge.

Compose puts the containers on their own network, not the default bridge. They
still reach 172.17.0.1 because it is an address *on the host*, routed via their
own gateway. `pg_hba.conf` admits `172.16.0.0/12` with `scram-sha-256`, added
with `blockinfile` rather than an `include` directive — `include` in `pg_hba.conf`
is a Postgres 16 feature, and `postgres-version` is a key someone can set to 14.

### WAL-G is a pinned release binary

There is no apt package. The playbook downloads
`wal-g-pg-<ubuntu-version>-<arch>.tar.gz` from the GitHub release named by a new
key, **`walg-version`**, verifies it against the published `.sha256`, and installs
the binary to `/usr/local/bin/wal-g`.

`walg-version` is desired state for the same reason `airflow-image` and
`postgres-version` are: an unpinned download makes two creates months apart
different deployments. The Ubuntu version and architecture come from Ansible
facts rather than from keys — they are properties of the machine the compute
stage already chose, and asking for them twice is how they disagree.

Credentials reach it through `/etc/wal-g.d/env`, 0600 and owned by `postgres`,
written by Ansible from `lookup('env', …)`. `archive_command` invokes a wrapper
that sources that file, because `archive_command` runs from the postmaster's
environment, which has none of it.

### Backups fail in two directions, and both are covered

- `walg-full-backup-oncalendar` drives a systemd timer, `Persistent=true`, so a
  run missed while the droplet was down fires on boot instead of being skipped.
- `OnFailure=` on that service starts a notifier that mails `alerts-email`
  through the Resend relay with `smtplib`. **No local MTA**: Airflow speaks SMTP
  directly, and installing `s-nail` or postfix to catch cron output would be a
  second mail path that delivers to a spool nobody reads.
- A **second** timer checks freshness against `walg-max-backup-age-hours` and
  exits non-zero when the newest base backup is older. It alerts on *absence*,
  which `OnFailure` structurally cannot: a timer that never runs never fails.

The freshness check is a systemd timer rather than an Airflow DAG on purpose. A
check that runs inside the thing it monitors goes quiet exactly when the machine
is unhealthy, and this keeps one alerting path rather than two.

`walg-max-backup-age-hours` is 30, not 24, from the arithmetic: a backup starting
at 02:00 and taking an hour leaves the newest completed one ~25 hours old just
before its successor lands. 24 would page every morning.

### The Fernet key is part of the backup

Restoring with a different `COLORS_PAR_AIRFLOW_FERNET_KEY` leaves every stored
connection undecryptable — a broken restore that looks like a successful one
until a DAG uses a connection. The restore procedure must say so.

## The DAG repo and the ForceCommand

ONCE's `github.clj` transplants nearly whole: a keypair per repository per
create, nothing stored, the private half published to an Actions environment
named after the profile alongside host, user and the server's host key. The
`authorized-keys` reconciler transplants too — it groups by key comment and
preserves foreign lines, so retaining one previous generation works unchanged;
only the `managed-marker` constant changes.

The placeholder public key stays, for a shifted reason: in ONCE it exists so
three colours render byte-identical artifacts. Here parity is moot, but
`golden.sh` diffs rendered output, and a fresh key per build would break the
goldens identically.

### The ForceCommand is where this diverges, and it is the security crux

ONCE's `deploy` **ignores** `SSH_ORIGINAL_COMMAND`: the client sends nothing and
all authority comes from the authorized_keys entry. rsync cannot work that way —
rsync-over-SSH runs `rsync --server` on the remote, so a forced command that
permits rsync must parse and validate what the client asked for. That is
strictly weaker, and hand-rolling the validator is how people get owned: `-e`,
`--rsh`, `--daemon` and protocol-level path handling all have to be refused
correctly.

So the forced command is **`rrsync`** — shipped with rsync, maintained upstream,
built for exactly this — locked write-only to `dags-dest`:

```
command="/usr/local/bin/rrsync -wo /srv/airflow/dags",restrict ssh-ed25519 …
```

The property that makes disposable keys affordable is preserved: a leaked key can
write DAGs into one directory and do nothing else. It needs **no sudo at all**,
unlike ONCE's key, which needs `sudo once update`. Strictly smaller blast radius
than the design it is copied from.

Airflow's dag-processor rescans its DAG directory on a timer, so nothing needs
restarting after a sync — which is what removes the need for sudo.

### Seeding is once-only

If the repository exists, its contents are left entirely alone and only the
environment secrets are reconciled. Colors converges on every `create`, and DAGs
are the user's work: a converging seed would overwrite real DAGs with
hello-world. Walter's clone-once rule is the precedent.

Ordering matters on first run: create the repository, publish the secrets, *then*
push the seed commit. Pushing first triggers the workflow before its key exists,
which looks like a broken install on a fresh setup.

`build` and `--dry-run` reach GitHub not at all.

## Mitigations

### Golden-file build tests

`scripts/golden.sh` renders every provider variant and diffs against committed
output. Beyond the per-provider diffs it must assert outright:

- ONCE's DigitalOcean template still declares `digitalocean_droplet.node1`,
  because this package's `firewall.tf` references that address. A rename upstream
  would otherwise surface as an opaque `tofu validate` failure during a real
  apply, half way through a create.
- The compute stage is still named `airflow-compute`, and the three delegated
  stages still render into `tofu-dns`, `tofu-smtp` and `tofu-smtp-post` — the
  names are ONCE's, so a change there moves this package's state keys.
- The rendered A record carries the compute stage's address and not
  `192.168.0.1`, which is what a broken `:once/compute-params` contract would
  produce.
- The SMTP verification records still render.
- Providers other than DigitalOcean render **no** `firewall.tf`, since it names
  a resource their template never declares.
- The seeded workflow and hello-world DAG render byte-identically.

Never accept a golden to make it pass without reading why it moved.

### Secrets never reach `.colors/`

This package departs from walter, which puts no secret on the server at all.
WAL-G credentials, the Postgres password, the Fernet key, the admin password and
the Resend relay key all have to be on the machine. Ansible reads them from the
process environment with `lookup('env', …)` and writes a 0600 file on the host —
they never pass through the rendered work directory. The bcrypt hash Caddy uses
is computed on the machine for the same reason, under `no_log`.

### `COLORS_PAR_PROFILE` is rejected outright

Inherited from walter unchanged, for the same reason: `read-pars` overlays any
flat key before any step runs, and the profile is the only thing separating this
project's state from once-colors' and walter-oci's in the shared bucket. It is
also, per the section above, the *only* separation for three of the four stages.

## Known limits of v1

- **No `stop`/`start`.** `stoppable` is `#{}`; the verbs are a reported no-op.
  The droplet bills continuously.
- **No `describe`.** Walter has none either.
- **SSH is open to the world.** The DAG sync is pushed from GitHub-hosted
  runners whose addresses come from large, changing ranges. Key-only auth plus
  the rrsync ForceCommand is what protects port 22.
- **One SSH key.** ONCE's DigitalOcean template interpolates
  `digitalocean-ssh-keys` into a single-element list.
- **The last alerting gap is open.** If the notifier breaks or both timers are
  masked, silence returns. Closing it needs a dead man's switch off the box.
- **One credential, one operator.** See "Authentication moved to the edge".
- **Three of four stage names are ONCE's.** See the reuse surface.
- ~~**`walg-r2-endpoint` is inferred, not verified.**~~ **Resolved.** A preflight
  sweep against the live account confirmed `postgres-backup` answers at that
  endpoint with the WAL-G credentials. The assumption — same Cloudflare account,
  same EU jurisdiction as the state bucket — held.

## Rejected

- **Airflow as an ONCE application.** ONCE's model is one image per host behind
  the `once` CLI. Airflow is multi-process with a host database; it does not fit
  the `applications:` shape without fighting it.
- **Forking walter.** Walter's `ansible-remote` is entirely dev-machine — nix,
  asdf, corepack, Emacs, dotfiles, atuin. Almost none of it belongs on a server.
  The compute stage's structure was worth taking; the playbook was not.
- **Reusing ONCE's `ansible` stage wholesale.** It installs the `once` CLI, the
  deploy user, the ForceCommand wrapper and the authorized-key reconciler —
  machinery for single-container push-to-deploy. The Docker-install block was
  worth lifting; the rest was not.
- **A minimal own DNS template.** Considered when the plan was a shared zone,
  which made ONCE's zone settings dangerous. A separate zone removed the hazard
  and the reuse won.
- **Postgres in a container with a wal-g sidecar.** `archive_command` runs inside
  the postgres container, so wal-g must be in that image. Custom image, and
  somewhere to push it.
- **Airflow's own login.** See "Authentication moved to the edge".
- **Cloudflare Origin CA, and ACME DNS-01.** See "TLS through a proxied,
  strict-SSL zone".
- **A hand-rolled rsync validator.** See the ForceCommand section.
- **A `restore` verb.** See Decisions.
- **Cron for backups.** systemd gives `Persistent=true`, `OnFailure=`, and
  visible state in `systemctl list-timers`. Cron's failure story is mail to a
  spool with no MTA to deliver it.
- **A local MTA (`s-nail`, postfix, msmtp).** Airflow speaks SMTP directly via
  `smtplib`. Nothing on this box needs `sendmail`.
- **The distro Postgres package.** Ubuntu 24.04 ships one major version, which
  would make `postgres-version` documentation rather than a pin.

## Configuration changes this revision requires

`airflow-digitalocean/colors.yml` was written before these decisions and needed
two edits before a build would validate. Both are done, in `colors.yml` and in
`validate/own-required`:

- **add `walg-version`** — the pinned WAL-G release tag, `v3.0.8`.
- **remove `airflow-admin-email`** — it no longer reaches a rendered file.

A **third** key was added during implementation that this section did not
anticipate: **`caddy-image`**. See below.

## What implementation found — revised

Seven things did not survive contact with the code. Recorded here because each
one contradicts something stated above, and a plan that quietly disagrees with
the codebase is worse than no plan.

### `caddy-image` was added, and Caddy is a container

The plan assumed Caddy without saying where it comes from. It is a **container in
the same compose file**, pinned by a new `caddy-image` key — which contradicts
"Configuration changes this revision requires" naming only two edits.

Two reasons. Pinning: an apt repository would make the one process facing the
internet the only unpinned thing on a machine where `airflow-image`,
`postgres-version` and `walg-version` are all pinned deliberately. And
addressing: on the compose network Caddy reaches `airflow-apiserver:8080` by
name, which means **the api-server publishes no port at all** — better than the
plan's "binds 127.0.0.1:8080", because a published port is a second door anything
with a shell on the box could walk through.

### `golden.sh` cannot assert the `:once/compute-params` contract

"Mitigations" says the script should assert the rendered A record carries the
machine's address and not `192.168.0.1`. **It cannot, and this was verified by
deleting the key and watching every variant still pass.**

A build has no OpenTofu state, so both sides fall back to a params map — and this
package's `fallback-compute-params` is a copy of ONCE's. They are equal by
construction, so the rendered bytes are identical whether or not the key is
published. The no-infra variant does not help: ONCE's fallback reads
`no-infra-compute-ip` from the same desired state.

It is covered instead by two tests in `tools_test` that drive a create with a
stubbed tofu — `compute-step-publishes-the-key-ONCEs-dns-step-reads` and
`ONCEs-dns-step-still-reads-that-key` — and both were confirmed to fail when the
key is dropped. `golden.sh` keeps the weaker shape check and a comment saying not
to move it back.

The general lesson, which applies to the whole reuse surface: **a golden proves
the output did not change, and most of what a pin bump would break is not
reachable from a build at all.**

### The SMTP verification records cannot be asserted either

Same reason. `smtp.tf.json` renders as `{ }` on a build because the records come
from a real apply. What the script asserts instead is the coupling that actually
breaks: that the zone derived from `airflow-host` still reaches ONCE's SMTP
template, which is `utils/once-applications` — the adapter — still fitting.

### The authorized-keys reconciler is Python, not Babashka

"The DAG repo and the ForceCommand" says the reconciler "transplants too" and
that "only the `managed-marker` constant changes". The algorithm did transplant
unchanged, but the language did not: it is a Python port.

ONCE installs Babashka because its deploy path needs it for other things. This
package does not, and putting a whole extra runtime on an Airflow server for one
forty-line script is exactly what "Rejected → Reusing ONCE's `ansible` stage
wholesale" declined to do. Ubuntu already has Python 3.

### Airflow 3 needs two more secrets, generated on the machine

Not anticipated anywhere above. `AIRFLOW__API__SECRET_KEY` and
`AIRFLOW__API_AUTH__JWT_SECRET` both default to a value Airflow generates per
process, which is fine for one process and wrong for five: the api-server signs
task-identity JWTs that the scheduler presents back, so a per-container secret
means every task fails authentication.

They are generated on the machine with `openssl rand` and kept, rather than added
to `colors.yml`, because **neither encrypts anything persisted** — a new machine
may safely have new ones. That is precisely what is not true of the Fernet key,
and the contrast is now stated in the playbook where the two sit together.

### The admin hash is `htpasswd`, not `caddy hash-password`

"Authentication moved to the edge" says the hash is computed with
`caddy hash-password`. It is computed with `htpasswd -bBC 14`, because bcrypt is
salted: hashing unconditionally on every converge produces a different value
every time, rewrites `caddy.env`, and bounces the only thing serving the site.

`htpasswd` can also **verify**, so the stored hash is reused whenever the
password has not changed and replaced the moment it has. Caddy accepts the `$2y$`
prefix htpasswd writes — it is bcrypt, the same as the `$2a$` Caddy's own command
produces.

### A base backup is taken during the create

"Backups fail in two directions" describes the schedule but not the gap before
its first run. A machine created at 09:00 with a 02:00 schedule would hold no
restorable backup for seventeen hours, shipping WAL into an archive with no base
to replay it onto — and `walg-check` would fail for its whole first day.

The playbook now takes one base backup during the create, guarded on the archive
being empty so it runs once per project rather than on every converge.

### The preflight sweep found two wrong values in `colors.yml`

Neither is a design change; both are the file's own warnings coming true, and
both would have failed during a real apply rather than at build.

- **`digitalocean-vpc-uuid` named `default-ams3` against a `fra1` region.** The
  key's own comment says a VPC is region-scoped and to confirm with
  `doctl vpcs list`. It is now `default-fra1`.
- **`digitalocean-size: s-2vcpu-8gb` does not exist.** DigitalOcean retired the
  plain slug; the 2 vCPU / 8 GB shared droplet is now sold only as CPU-specific
  variants. It is now `s-2vcpu-8gb-amd` — 100 GB disk, $42/mo, the closest match
  to the stated intent and the cheapest 8 GB option.

The lesson worth keeping: **validation cannot catch either of these.** Both are
well-formed strings that only a provider can adjudicate, which is what makes a
read-only sweep against the live account worth running before a first create
rather than discovering them half way through one.

### What only a real create found

Five creates were run against live infrastructure. Every one of them failed on
something no test, golden, syntax check or preflight could have caught, and the
pattern across them is the finding worth keeping: **each bug was invisible to
every static check, and each was masked by the one before it.**

| # | Failed at | Cause |
|---|---|---|
| 1 | task 34 of the playbook | Selmer HTML-escaped a credential expression — `lookup(&#39;env&#39;,…)` |
| 2 | task 29 | `chown` to the `deploy` user, created 300 lines later in the play |
| 3 | — | succeeded; Airflow live |
| 4 | github stage | seeding refused with a misleading 404 |
| 5 | github stage | same, after a fix built on a wrong diagnosis |

The first two are ordinary bugs with an unordinary property: the playbook
rendered, parsed and `--syntax-check`ed identically in both the broken and fixed
states. Only running it could tell them apart. Both now have tests that bite —
one refusing any HTML entity in rendered output, one asserting the order of two
tasks — and each was verified by reintroducing the bug rather than trusted for
being green.

The second was hidden behind the first: create 1 never reached the task that
create 2 failed on. **A failure does not mean there is only one failure**, and a
create that dies at task 34 has said nothing about tasks 35 onward.

### The 404 that was not a 404

Creates 4 and 5 are the ones to learn from, because the diagnosis was wrong
twice and the second wrong fix was built on the evidence of the first.

Seeding the DAG repository failed with `404 Not Found`. That looked like a race
against GitHub initialising a new repository, and the timing evidence seemed to
agree: a manual write to an empty repository succeeded where the tool's had
failed, and the repository was twenty minutes old at the time. A retry was
added. It failed. The retry was widened to a minute, measured against a
repository that accepted a write seventy-one seconds after creation. That failed
too.

**None of it was a race.** Writing under `.github/workflows/` requires the
`workflow` OAuth scope, and GitHub reports its absence as **404 rather than
403** — the same status as a repository that genuinely is not there. The
"evidence" for a race was an artefact of which paths were being probed: every
successful probe wrote a non-workflow path, every failure wrote the workflow
file. Same token, same repository, seconds apart:

```
dags/hello_world.py                  accepted
.github/workflows/deploy-dags.yml    404
```

Three lessons, in the order they cost time:

1. **The evidence was already in hand and went unread.** The very first push of
   this repository failed with `refusing to allow an OAuth App to create or
   update workflow .github/workflows/cicd.yml without workflow scope`. The same
   permission, the same file prefix, an hour earlier.
2. **A probe that differs from the failing call proves nothing.** The manual
   writes that "disproved" the permission theory used a different path, which
   was the only variable that mattered.
3. **A retry is not a diagnosis.** Two retry budgets were tuned against a
   failure that could never succeed. Retrying should follow understanding why
   something is transient, not substitute for it.

The package now recognises the pattern and reports the scope, the variable, and
why the status code lies. `COLORS_PAR_GITHUB_TOKEN` needs `repo` **and**
`workflow`, which is stated in `colors.yml` and in the configuration reference.

### Smaller corrections

- The WAL-G asset is `wal-g-pg-<24.04>-<amd64|aarch64>.tar.gz`, not
  `-<ubuntu-version>-<arch>` in the shapes one would guess: the version carries no
  `ubuntu-` prefix and x86 is `amd64` where Ansible reports `x86_64`. Verified
  against the real v3.0.8 release, including the `sha256sum -c`-format sidecar.
- `rrsync` is at `/usr/bin/rrsync` in Ubuntu's rsync package. `/usr/local/bin/rrsync`
  is a symlink the playbook makes, so the authorized_keys line — rendered on a
  workstation that cannot know the machine's package layout — names a stable path.
- `ansible.builtin.apt_repository` is deprecated and removed in ansible-core
  2.25; the PGDG repository uses `deb822_repository`.
- `archive_timeout = 300` was added. Without it an idle Airflow can leave the
  last committed transaction in an unarchived segment indefinitely, which makes
  the recovery point unbounded while the backups look healthy.
