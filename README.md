# airflow

One Apache Airflow server, converged from a desired-state file.

A VPS running Airflow under Docker with LocalExecutor, a host Postgres for the
metadata database archiving continuously to object storage with WAL-G, TLS and
authentication through Caddy, and a public hostname behind Cloudflare. DAGs are
pushed to it from a GitHub Actions workflow over rsync, using a disposable deploy
key confined by an `rrsync` ForceCommand.

It is a **Package Skill** on the [Colors](https://github.com/getcolors) SDK — an
agent-installable CLI — and the third after ONCE and walter. Like walter it is
green only.

## Install

```sh
npx skills add getcolors/airflow
```

That writes `.agents/skills/package-airflow-green/` into your project and records
the source and content hash in `skills-lock.json`. Copy the launcher to your
project root:

```sh
cp .agents/skills/package-airflow-green/green ./green
```

The root launcher is a **copy**, not a symlink, so re-copy it after every
`npx skills update -p` or the project keeps running the old pin.

## Use

```sh
./green build                # render .colors/<profile>/ only; contacts nothing
./green create --dry-run     # print the graph; touches nothing
./green create               # provision, configure, publish the deploy key
./green delete               # revoke the key, then destroy — the repo is kept
```

`build` and `--dry-run` render from desired state alone and need no credentials.
Only a real `create` or `delete` touches a provider.

There is no `stop`, `start` or `describe`. An Airflow scheduler runs
continuously, so a box that cannot be parked costs nothing that was not already
being paid.

## Configure

Everything lives in one `colors.yml`, found by walking up from the working
directory, holding **non-secret values only**. Credentials arrive at run time
through `COLORS_PAR_*` environment variables named after the key they fill.

See
[`skills/package-airflow-green/references/configuration.md`](skills/package-airflow-green/references/configuration.md)
for every key, and `../airflow-digitalocean/colors.yml` for a complete worked
example.

Three things are worth knowing before the first create:

- **`COLORS_PAR_AIRFLOW_FERNET_KEY` is part of the backup.** It encrypts every
  stored Airflow connection, so restoring with a different one leaves them
  undecryptable — a broken restore that looks like a successful one until a DAG
  uses a connection. Store it somewhere that survives the machine.
- **Never export `COLORS_PAR_PROFILE`.** The package refuses to run when it is
  set. `profile` is what separates this project's OpenTofu state from another
  Colors project's in a shared bucket, and the overlay happens before any step
  runs.
- **Use a Cloudflare zone no other Colors project manages.** The reused DNS
  template manages zone-level settings, not just the A record, and two OpenTofu
  states co-owning those is invisible until the first delete.

## The login

`https://<airflow-host>` answers with a browser password prompt. That prompt is
**Caddy's**, not Airflow's: Airflow 3's `SimpleAuthManager` will not take a
password from configuration, so authentication sits at the edge and the
api-server publishes no port at all.

The consequence is a real limit rather than a detail — there is one login for one
operator and no user model behind it. A second person means Cloudflare Access or
FAB.

## Restoring

There is deliberately no `restore` verb: a command whose purpose is overwriting a
live database, one typo from `create` in the same CLI, is a hazard that outweighs
the convenience. The procedure is in
[`SKILL.md`](skills/package-airflow-green/SKILL.md).

## Development

```sh
bb green build              # render, from this checkout
bb test                     # the unit suite, under babashka
bb golden                   # every provider variant vs committed output
bb golden:accept            # regenerate after an intended change
./scripts/launcher.sh       # the launcher, outside this checkout
bb pin                      # stamp the launcher (maintainers, after a push)
```

`bb golden` is the regression net for this package's dependency on ONCE, which is
the widest in the stack — six things, three of them functions nothing upstream
promises. Never accept a golden to make it pass without reading why it moved.

See [CLAUDE.md](CLAUDE.md) for the architecture and the invariants, and
[plans/0001-airflow-v1.md](plans/0001-airflow-v1.md) for why the design is what
it is.

## Licence

MIT.
