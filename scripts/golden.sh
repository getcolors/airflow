#!/usr/bin/env bash
set -euo pipefail

# This package is a single colour, so there is no parity harness — and ONCE's
# parity.sh was its golden-file regression net as much as its parity check. This
# is that net: render every provider variant and diff against committed output.
#
# It carries more weight here than it does in walter, and the difference is the
# whole reason to read this file before changing anything. Walter consumes two
# things from ONCE, both of them *resources*. This package consumes six, and
# three of them are step FUNCTIONS — tofu-smtp-step, tofu-dns-step and
# tofu-smtp-post-step — plus tool-dir, which decides where their backend
# configuration has to be written. Nothing upstream promises any of it: ONCE's
# contract number versions the *launcher* handshake, not a library API, and
# ONCE's own rules treat its internals as free to move as long as three colours
# move together.
#
# So this is not a nice-to-have. It is the only thing standing between an ONCE
# refactor and a silently broken Airflow deploy.
#
#   ./scripts/golden.sh            check
#   ./scripts/golden.sh --accept   regenerate after an intended change
#
# Goldens are rendered against the pins in deps.edn, not against a sibling
# checkout — bb.edn only local-roots airflow itself. Setting ONCE_LIB_ROOT while
# running this compares the working tree against the pinned goldens, which is a
# useful thing to do on purpose and a confusing one to do by accident.
#
# Never accept a golden to make it pass without reading why it moved.

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
state="$root/test/fixtures/colors.yml"
goldens="$root/test/resources/golden"
profile=airflow-fixture
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

accept=0
[ "${1:-}" = "--accept" ] && accept=1

if [ -n "${ONCE_LIB_ROOT:-}" ]; then
  echo "note: ONCE_LIB_ROOT=$ONCE_LIB_ROOT — comparing a working tree against pinned goldens"
fi

fail() {
  echo "golden: FAIL — $*" >&2
  exit 1
}

build_variant() {
  local variant=$1
  shift
  (
    cd "$root"
    env COLORS_PAR_WORKDIR="$tmp/$variant" "$@" bb airflow build -f "$state" >/dev/null
  )
  if [ "$accept" = 1 ]; then
    rm -rf "${goldens:?}/$variant"
    mkdir -p "$goldens/$variant"
    cp -r "$tmp/$variant/." "$goldens/$variant/"
    echo "  accepted — $variant"
  else
    if [ ! -d "$goldens/$variant" ]; then
      fail "no committed golden for $variant; run ./scripts/golden.sh --accept"
    fi
    diff -qr "$goldens/$variant" "$tmp/$variant"
    echo "  ok — $variant"
  fi
}

# Every compute provider ONCE's registry offers, because this package fills all
# four provider slots and any of them can be selected by a consumer.
build_variant digitalocean
build_variant hcloud COLORS_PAR_PROVIDER_COMPUTE=hcloud
build_variant oci COLORS_PAR_PROVIDER_COMPUTE=oci
build_variant yandex COLORS_PAR_PROVIDER_COMPUTE=yandex
build_variant no-infra COLORS_PAR_PROVIDER_COMPUTE=no-infra

# The firewall is this package's own HCL rather than ONCE's, and it is the one
# file whose presence is conditional on a key rather than on a provider.
build_variant digitalocean-no-firewall COLORS_PAR_DIGITALOCEAN_FIREWALL=false

# The other side of the three delegated stages: ONCE ships a no-infra template
# for DNS and for SMTP, and a consumer pointing at an existing relay and an
# unmanaged zone renders those instead.
build_variant no-infra-services \
  COLORS_PAR_PROVIDER_DNS=no-infra \
  COLORS_PAR_PROVIDER_SMTP=no-infra

# Both sides of the Caddyfile's one conditional.
build_variant acme COLORS_PAR_CADDY_ACME_EMAIL=ops@fixture.example

build_variant s3 COLORS_PAR_PROVIDER_BACKEND=s3
build_variant r2 COLORS_PAR_PROVIDER_BACKEND=r2

if [ "$accept" = 1 ]; then
  echo "goldens regenerated"
else
  echo "every provider variant matches its committed golden"
fi

# ==========================================================================
# The assertions that are not diffs.
#
# A golden proves the output did not change. These prove the output is still
# *connected to the things it depends on* — every one of them is a coupling that
# lives in ONCE, or in an undocumented key between ONCE and this package, and
# that a diff alone would not notice moving.

do_compute="$tmp/digitalocean/$profile/airflow-compute"

# ---------------------------------------------------------------------------
# The resource address this package's firewall.tf depends on.
#
# firewall.tf says `digitalocean_droplet.node1.id`, and that resource is
# declared in ONCE's template, not this one. A rename upstream would otherwise
# surface as an opaque `tofu validate` failure during a real apply — against
# live infrastructure, half way through a create.

grep -q 'resource "digitalocean_droplet" "node1"' "$do_compute/main.tf" ||
  fail "ONCE's DigitalOcean template no longer declares
  resource \"digitalocean_droplet\" \"node1\"
which this package's firewall.tf references. Update the pin deliberately."
echo "  ok — ONCE still declares digitalocean_droplet.node1"

grep -q 'digitalocean_droplet.node1.id' "$do_compute/firewall.tf" ||
  fail "firewall.tf no longer attaches to digitalocean_droplet.node1"
echo "  ok — the firewall attaches to it"

# ---------------------------------------------------------------------------
# The stage names, which are the OpenTofu state keys.
#
# One of them is this package's choice and three are ONCE's. All four are
# checked, because a change to any of them moves where this project's state
# lives — and for the three delegated ones, `profile` alone is what separates
# this project from a once-colors in the same bucket.

[ -d "$do_compute" ] ||
  fail "the compute stage is no longer named airflow-compute"
echo "  ok — compute stage is airflow-compute"

for tool in tofu-dns tofu-smtp tofu-smtp-post; do
  [ -d "$tmp/digitalocean/$profile/$tool" ] ||
    fail "the delegated stage $tool no longer renders under that name.
ONCE's step functions compute their own directory, so this moved upstream — and
with it this project's state key. Read the diff before accepting anything."
done
echo "  ok — the three delegated stages still render as ONCE names them"

# The backend advice has to write into exactly the directory the delegated step
# will run in. Two copies of that resolution agreeing today is not the same as
# agreeing after a pin bump, which is why tool-dir is routed through ONCE's.
for tool in tofu-dns tofu-smtp tofu-smtp-post; do
  [ -f "$tmp/r2/$profile/$tool/backend.tf.json" ] ||
    fail "no backend.tf.json in the delegated stage $tool.
The backend advice and ONCE's own tool-dir have stopped agreeing on where that
stage runs, so it would silently use local state instead of the R2 bucket."
  grep -q "\"key\" : \"$profile/$tool.tfstate\"" "$tmp/r2/$profile/$tool/backend.tf.json" ||
    fail "$tool's remote state key is not $profile/$tool.tfstate"
done
echo "  ok — every delegated stage gets a backend keyed by profile and stage"

# ---------------------------------------------------------------------------
# The A record.
#
# NOTE, because it was tried and does not work: this CANNOT assert the
# `:once/compute-params` contract, and an earlier version of this script
# claimed to. ONCE's tofu-dns-step reads that key to find the address it points
# DNS at, and this package's compute step must publish it — but a build has no
# OpenTofu state, so both sides fall back to a params map, and this package's
# fallback is a copy of ONCE's. The two are equal by construction, so a build
# renders the same bytes whether the key is published or not. Deleting the key
# and running this script proves it: every variant still passes.
#
# That contract is covered by `compute-step-publishes-the-key-ONCEs-dns-step-reads`
# and `ONCEs-dns-step-still-reads-that-key` in tools_test, which drive a create
# with a stubbed tofu and can therefore tell the two apart. Do not move it back
# here.
#
# What a build CAN prove is that the record is still the right shape, and it is
# checked against the no-infra variant because that is the one whose address
# comes from desired state rather than from a placeholder.

apps="$tmp/no-infra/$profile/tofu-dns/apps.tf.json"

grep -q '"content" : "198.51.100.10"' "$apps" ||
  fail "the rendered A record does not carry the address desired state names.
This is the shape check, not the :once/compute-params contract — see tools_test
for that one — but it still catches a DNS render that stopped reading the
compute params at all."
echo "  ok — the A record carries the address desired state names"

grep -q '"name" : "airflow.fixture.example"' "$apps" ||
  fail "the A record is no longer named after airflow-host"
grep -q '"proxied" : true' "$apps" ||
  fail "the A record is no longer proxied, which breaks the TLS story:
the zone is set to ssl = strict, and an unproxied record bypasses the whole
Cloudflare path this package's certificate handling assumes."
echo "  ok — it is the proxied record for airflow-host"

# ---------------------------------------------------------------------------
# The SMTP stages, and the adapter that feeds them.
#
# Deliberately NOT an assertion about rendered verification records: those come
# from a real apply, so on a build smtp.tf.json is empty by design and a check
# for their content would either be vacuous or fail forever. What a build CAN
# prove is the coupling that actually breaks — that `utils/once-applications`
# still puts airflow-host somewhere ONCE derives the sending domain from.

smtp="$tmp/digitalocean/$profile/tofu-smtp/main.tf"

grep -q 'resource "resend_domain" "domains"' "$smtp" ||
  fail "ONCE's SMTP template no longer declares resend_domain.domains"
grep -q 'toset(\["fixture.example"\])' "$smtp" ||
  fail "the zone derived from airflow-host did not reach ONCE's SMTP template.
utils/once-applications is the whole adapter between this package's flat
airflow-host key and the :once :applications shape ONCE's steps read — if the
zone list is empty, that adapter has stopped matching what ONCE expects."
grep -q 'notifications' "$smtp" ||
  fail "the sending domain is no longer notifications.<zone>"
echo "  ok — the sending domain is derived from airflow-host"

grep -q 'resource "resend_domain_verification" "domains"' \
  "$tmp/digitalocean/$profile/tofu-smtp-post/main.tf" ||
  fail "ONCE's smtp-post template no longer verifies the sending domain"
echo "  ok — smtp-post still verifies it"

# The zone settings are why this package must not share a Cloudflare zone with
# another Colors project. Two OpenTofu states co-owning them is invisible while
# both apply identical values and destructive the first time either is deleted.
grep -q 'cloudflare_zone_setting' "$tmp/digitalocean/$profile/tofu-dns/main.tf" ||
  fail "ONCE's DNS template no longer manages zone settings.
That is good news, but colors.yml documents the opposite at airflow-host and
tells the operator to keep this project on a zone of its own. Update that
comment in the same commit as the pin bump."
echo "  ok — ONCE still manages zone settings, as colors.yml warns"

# ---------------------------------------------------------------------------
# The firewall renders only where its resource address exists.

for variant in hcloud oci yandex no-infra digitalocean-no-firewall; do
  if [ -f "$tmp/$variant/$profile/airflow-compute/firewall.tf" ]; then
    fail "$variant rendered firewall.tf, which names digitalocean_droplet.node1 —
a resource that provider's template never declares."
  fi
done
echo "  ok — the firewall renders for DigitalOcean only, and only when asked for"

# ---------------------------------------------------------------------------
# The deploy key line, which is the security crux.
#
# rrsync -wo confines a leaked key to writing under dags-dest. A change that
# dropped -wo, or the directory, or `restrict`, would still render, still
# install, and still deploy — and would hand CI a general-purpose shell account.

keys="$tmp/digitalocean/$profile/airflow-ansible-remote/deploy_keys"

grep -q 'command="/usr/local/bin/rrsync -wo /srv/airflow/dags"' "$keys" ||
  fail "the deploy key's ForceCommand is no longer rrsync -wo <dags-dest>.
That single line is what stops a leaked deploy key doing anything but writing
DAGs. Read the diff very carefully."
grep -q '^restrict,' "$keys" ||
  fail "the deploy key line no longer starts with `restrict`, which is what
denies pty, agent and port forwarding."
echo "  ok — the deploy key is confined by rrsync -wo and restrict"

# ---------------------------------------------------------------------------
# The seed, which is pushed by reading these very files back.

seed="$tmp/digitalocean/$profile/airflow-github/seed"
[ -f "$seed/.github/workflows/deploy-dags.yml" ] ||
  fail "the seeded workflow no longer renders"
[ -f "$seed/dags/hello_world.py" ] ||
  fail "the seeded hello-world DAG no longer renders"
grep -q 'environment: airflow-fixture' "$seed/.github/workflows/deploy-dags.yml" ||
  fail "the seeded workflow no longer names the profile as its Actions
environment, so it would find none of the credentials github.clj publishes."
echo "  ok — the seed renders, and names the profile's Actions environment"

# ---------------------------------------------------------------------------
# No secret reached the work directory.
#
# The last line of defence for the one way this package departs from walter: it
# puts credentials on a server. They travel in the process environment and are
# written by Ansible into 0600 files on the machine, never through .colors/.
# This asserts the *rendered* form is still a lookup expression rather than a
# value — the failure it guards against is someone "simplifying" a playbook by
# interpolating the credential at render time.

remote="$tmp/digitalocean/$profile/airflow-ansible-remote/main.yml"
for par in COLORS_PAR_POSTGRES_PASSWORD COLORS_PAR_AIRFLOW_FERNET_KEY \
  COLORS_PAR_AIRFLOW_ADMIN_PASSWORD COLORS_PAR_WALG_R2_ACCESS_KEY_ID \
  COLORS_PAR_WALG_R2_SECRET_ACCESS_KEY COLORS_PAR_RESEND_PASSWORD; do
  grep -q "lookup('env','\?\s*$par'\|lookup('env', '$par'" "$remote" ||
    fail "$par is no longer resolved by an Ansible lookup in the playbook.
Credentials must be read from the environment at play time, never rendered."
done
echo "  ok — every credential is still an Ansible lookup, not a value"

echo "golden: every variant and every assertion passed"
