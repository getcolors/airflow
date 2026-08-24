# CLAUDE.md

This file describes the `airflow` codebase for AI assistants. Read it before
making changes.

## What this is

`airflow` provisions and operates one **Apache Airflow server**: a single VPS
running Airflow under Docker with LocalExecutor, a host Postgres for the
metadata database, continuous archiving to R2 with WAL-G, TLS and authentication
through Caddy, and a public hostname behind Cloudflare. DAGs are pushed to it
from a GitHub Actions workflow over rsync, using a disposable deploy key.

It is the third Package Skill on the Colors SDK, after ONCE and walter. It has
three interchangeable implementations: `green/` (Clojure), `red/` (TypeScript),
and `blue/` (Python), with byte-identical output enforced by `scripts/parity.sh`.

`plans/0001-airflow-v1.md` records why the design is what it is, including the
alternatives that were rejected and why. It is history, not specification — read
the code before acting on anything in it.

The first consumer is `../airflow-digitalocean/`, whose `colors.yml` is a
complete and heavily commented statement of the configuration surface.

## Tech stack

- Clojure 1.12.5, plus Babashka for the launcher
- `io.github.getcolors/green` — the workflow engine
- `io.github.getcolors/once` — **a package dependency, not a library one**; see
  "The reuse surface" below
- OpenTofu, Ansible, and the `gh` CLI

## Commands

```bash
cd green && bb green build
cd green && bb test && bb golden
cd red && bun install && bun test && bun run typecheck
cd blue && uv sync && uv run pytest -q
./scripts/parity.sh
./scripts/launcher.sh
cd green && bb pin
```

`cd green && bb green build -f other.yml` overrides the `colors.yml` found by walking up.

## The reuse surface — read this before touching anything

Walter consumes **two** things from ONCE and its `CLAUDE.md` is emphatic about
keeping it that way. This package consumes **six**:

1. `io.github.getcolors.once.validate/providers` — the provider registry, as data
2. The compute templates, by classpath keyword
3. `once.tools/tofu-smtp-step` — the function, not just the template
4. `once.tools/tofu-dns-step` — likewise
5. `once.tools/tofu-smtp-post-step` — likewise
6. `once.tools/tool-dir` — likewise, so the backend advice writes
   `backend.tf.json` into exactly the directory the delegated step will run in

Items 3–6 are a **code-level API**, not data. Nothing upstream promises any of
it: ONCE's `utils/contract` versions the *launcher* handshake, not a library
API, and ONCE's own rules treat its internals as free to move as long as three
colours move together. Walter deliberately refused to reuse ONCE's
`ansible-local` for a much smaller version of this exposure.

This was a deliberate trade — the DNS and SMTP stages are most of ONCE's value.
The consequence is that `scripts/golden.sh` is not a nice-to-have. It is the only
thing standing between an ONCE refactor and a silently broken Airflow deploy.

**Bump the ONCE pin deliberately and rarely.** Run `bb golden` immediately after
and read the diff rather than accepting it.

### Two consequences that are load-bearing

**The delegated stage directories cannot be renamed.** Each ONCE step hard-codes
its own — `(tool-dir opts "tofu-dns")` is *inside* `tofu-dns-step`, not a
parameter. So the state keys are:

```text
<profile>/airflow-compute.tfstate     this package's own stage
<profile>/tofu-dns.tfstate            ONCE's name
<profile>/tofu-smtp.tfstate           ONCE's name
<profile>/tofu-smtp-post.tfstate      ONCE's name
```

For those three, **`profile` alone separates this project from a once-colors**,
where walter has two independent separations. That is the price of the reuse, and
it is why `COLORS_PAR_PROFILE` is refused rather than merely discouraged.

**`tofu-dns-step` reads `:once/compute-params`** to find the address it points
DNS at. ONCE's own compute step sets that key; this package's compute step is its
own, so it must publish it too. Dropping it would not fail — it would fall
through to ONCE's `fallback-compute-params` and point every A record at
192.168.0.1, a create that succeeds and resolves nowhere.

**That contract cannot be caught by `golden.sh`, and it was tried.** On a build
both sides fall back to a params map, and this package's fallback is a copy of
ONCE's — so the rendered bytes are identical whether or not the key is published.
It is covered instead by `compute-step-publishes-the-key-ONCEs-dns-step-reads`
and `ONCEs-dns-step-still-reads-that-key` in `tools_test`, which drive a create
with a stubbed tofu. Do not move it back into the golden script; the comment
there explains why.

### The adapter

`airflow-host` is a flat key. The `once: applications:` map is assembled at the
call boundary, in `utils/once-applications` — one line — so an upstream rename is
a one-line fix here rather than a breaking change to every consumer's
`colors.yml`.

### Records inherit ONCE's namespace

`add-fqn-suffix ::app-dns` uses the *calling* namespace, so this package's DNS
records get addresses like
`cloudflare_dns_record.io_github_getcolors_once_tools_app_dns_<host>` in
**airflow's** state. Cosmetic, but serialised into state — changing it later is a
`tofu state mv`, not an edit. Accepted rather than forked.

## Architecture

### The DAG

```text
create / build   start ─ compute ─ smtp ─ dns ─ smtp-post ─┬─ ansible-local
                                                           └─ ansible-remote ─ github

delete           start ─ github ─ ansible-cleanup ─ smtp-post ─ dns ─┬─ smtp
                                                                    └─ compute
```

This is ONCE's shape, not walter's, and the SMTP ordering is why: the Resend
sending domain must exist before its verification records can be rendered into
DNS, and DNS must be live before verification runs.

Compute and SMTP run in **series** where ONCE forks them. ONCE's `joined-params`
reads the compute output out of `:green/branches` at the DNS join; running in
series means it reads it off `opts` instead, which is the same function's second
fallback. One less concurrency edge for no lost time.

`github` runs last on create — **after `ansible-remote`, not beside it** — and
first on delete. A withdrawn credential against a live host is a loud,
recoverable broken deploy; a live credential against a destroyed host is silent.

The edge from `ansible-remote` is load-bearing and was drawn as a fork here and
in the plan until it broke a real deploy. Seeding pushes a commit, the commit
triggers the deploy workflow, and that workflow rsyncs to the server with the
key this create just issued — so run in parallel, the matching public key is not
on the box yet and CI fails with `Permission denied (publickey)`. Both documents
said the right thing in prose and drew the wrong thing in ASCII; the ASCII is
what got implemented.

`delete` never deletes the DAG repository. Destroying compute is recoverable
through WAL-G; destroying the repository is not.

### Stages

| Step | Directory | Does |
|---|---|---|
| `:airflow/compute` | `airflow-compute` | ONCE's provider template + this package's `firewall.tf` on DigitalOcean |
| `:airflow/smtp` | `tofu-smtp` | delegated to ONCE |
| `:airflow/dns` | `tofu-dns` | delegated to ONCE |
| `:airflow/smtp-post` | `tofu-smtp-post` | delegated to ONCE |
| `:airflow/ansible-local` | `airflow-ansible-local` | the managed `~/.ssh/config` block |
| `:airflow/ansible-remote` | `airflow-ansible-remote` | Docker, Postgres, WAL-G, Airflow, Caddy, the deploy account |
| `:airflow/github` | `airflow-github` | renders the seed, then publishes the deploy key |

`airflow-github` renders but never runs a tool: the seeded workflow and DAG are
artifacts `golden.sh` can diff, and `github.clj` pushes them by reading them back.

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

## Code conventions

- **Namespaces**: `io.github.getcolors.airflow.*` — `utils` (contract, alias, the
  ONCE adapter), `validate` (rules over ONCE's registry), `tools` (the steps and
  every template spec), `github` (the repository and its key), `workflow` (the
  graph). Adding a sixth needs a genuinely new concern.
- **`tools` requires `github`, never the reverse.** `tools` builds the
  authorized_keys line from `github/public-keys`; `github` renders nothing. The
  seed is composed in `workflow`, which requires both.
- **Keys**: plain kebab-case keywords for desired state (they match template
  variable names); namespaced for engine state (`:green/…`, `:airflow/…`,
  and `:once/compute-params`, which is ONCE's).
- **Steps** take `opts` and return `opts`, reporting failure through
  `:green/exit` / `:green/err` rather than throwing.
- **Anything that shells out gets a runner arity.** `github.clj`'s functions take
  an injectable runner in their second arity, so the tests cover the argv and the
  decisions without starting a process. Preserve that split.
- **The launcher holds no logic.** Validation, the graph and the steps live where
  the tests reach them; a copied payload is the one place code cannot be tested.
  `scripts/launcher.sh` enforces this.
- **No secret is ever rendered.** Ansible reads them with `lookup('env', …)` at
  play time and writes 0600 files on the host. `golden.sh` and `tools_test` both
  assert the rendered playbook still contains lookups rather than values.

## Testing

`bb test` is the unit suite; `bb golden` is the regression net for the reuse
surface; `./scripts/launcher.sh` covers the copied payload in environments this
checkout is not.

The three are not interchangeable. A golden proves the output did not change; the
unit tests prove the couplings a build cannot exercise — which is most of what
would break on a pin bump, since a build never runs a create.

## Documentation

`index.html` is this repository's landing page and carries two analytics tags:
GA4 measurement ID `G-4VKP1WY4QJ`, whose explicit `page_title` must exactly
equal the decoded HTML `<title>` and stay distinct and stable so one Analytics
property can separate repositories, and the self-hosted Rybbit snippet
`<script src="https://rybbit.getcolors.ai/api/script.js" data-site-id="9fb9c41a6d49" defer></script>`,
which shares one site ID across every page because `getcolors.github.io/<repo>/`
paths already encode the repository. Never add one tag without the other.

## Git

Work on the current branch. Do not commit or push unless explicitly asked.

`airflow-sha` in the launcher is managed by `bb pin` — **never hand-edit it, and
never invent a SHA.** `pin` reads the HEAD of the checkout surrounding it and
refuses a dirty or unpushed tree, so the sequence for a change consumers need is:
commit, push, `bb pin`, commit the stamp, push again. The stamp names the commit
*before* it, which is correct — it points at the library code, and the stamp
commit only rewrites the payload that fetches it.

Consumers hold a **copy** of the payload, not a symlink, so re-copy it into every
project after a repin or they keep running the old pin:

```sh
cp skills/package-airflow-green/green ../airflow-digitalocean/green
cp skills/package-airflow-red/red ../airflow-digitalocean/red
cp skills/package-airflow-blue/blue ../airflow-digitalocean/blue
```
