#!/usr/bin/env bash
set -euo pipefail

# The launcher is the one file here that is copied out and run somewhere else,
# so its interesting behaviour happens in environments this checkout does not
# contain: no bb.edn beside it, no airflow on the classpath, an unstamped pin.
# `bb test` cannot reach any of that — it runs inside the checkout, where bb.edn
# local-roots airflow to the working tree, which is the one path on which none of
# the resolution logic runs.
#
# This launcher copies walter's, which copies ONCE's. Copying the untestable
# half without its harness would be the wrong half. Every failure this catches is
# silent: the launcher still starts and still renders, it just resolves the wrong
# thing.

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
launcher="$root/skills/package-airflow-green/airflow"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

checks=0
fail() {
  echo "launcher: FAIL — $*" >&2
  exit 1
}
ok() {
  checks=$((checks + 1))
  echo "  ok — $*"
}

[ -f "$launcher" ] || fail "no launcher at $launcher"

# --------------------------------------------------------------------------
# It holds no logic of its own.
#
# ONCE's rule, and walter's: validation, the graph and the steps live in the
# library where tests reach them. A launcher that grows a step is a step nobody
# can test.

grep -q 'io.github.getcolors.airflow.workflow/workflow' "$launcher" ||
  fail "the launcher no longer dispatches to the library workflow"
ok "dispatches to the library workflow"

for forbidden in 'defn.*-step' 'tofu/' 'ansible/' 'gh secret'; do
  if grep -qE "$forbidden" "$launcher"; then
    fail "the launcher contains logic that belongs in the library: /$forbidden/"
  fi
done
ok "carries no step, tofu, ansible or github logic"

# --------------------------------------------------------------------------
# Copied out of the checkout, with nothing to resolve.
#
# This is the state a user's project is in before `bb pin` has ever run, and the
# state a stranger's project is in if the pin is lost. It must say what to do
# rather than fail obscurely.

copy="$tmp/bare"
mkdir -p "$copy"
cp "$launcher" "$copy/airflow"
chmod +x "$copy/airflow"

pin=$(grep -oE '\(def \^:private airflow-sha (nil|"[0-9a-f]{40}")\)' "$launcher" || true)
[ -n "$pin" ] || fail "could not read the launcher's own pin declaration"
ok "declares an airflow-sha pin site"

if echo "$pin" | grep -q 'nil'; then
  # Unpinned: the launcher must refuse and name the override, not guess.
  out=$( (cd "$copy" && ./airflow build 2>&1) || true )
  echo "$out" | grep -q 'AIRFLOW_LIB_ROOT' ||
    fail "an unpinned launcher must name AIRFLOW_LIB_ROOT; got: $out"
  ok "an unpinned launcher explains itself instead of failing obscurely"
else
  ok "launcher is pinned to a real commit"
fi

# --------------------------------------------------------------------------
# AIRFLOW_LIB_ROOT overrides whatever is pinned.
#
# This is how a copied payload is pointed at a working tree, and how the check
# above is escaped in a project that has not been able to pin yet.
#
# Every provider is no-infra here on purpose: this exercises the launcher, not
# the templates, and scripts/golden.sh is what covers the real ones.

cat >"$copy/colors.yml" <<'EOF'
profile: launcher-check
workdir: .colors
provider-compute: no-infra
provider-dns: no-infra
provider-smtp: no-infra
provider-backend: local
compute-prevent-destroy: true
no-infra-compute-ip: 198.51.100.10
no-infra-compute-user: root
no-infra-compute-sudoer: root
no-infra-compute-uid: 1000
no-infra-smtp-server: smtp.example.com
no-infra-smtp-port: 587
no-infra-smtp-username: launcher-check
airflow-host: airflow.example.com
airflow-image: apache/airflow:3.1.3
airflow-admin-username: admin
airflow-smtp-from: airflow@notifications.example.com
caddy-image: caddy:2.11.4
dags-repo: example/dags
dags-dest: /srv/airflow/dags
dags-branch: main
postgres-version: 16
walg-version: v3.0.8
walg-r2-bucket: launcher-check
walg-r2-endpoint: https://example.r2.cloudflarestorage.com
walg-r2-region: auto
walg-full-backup-oncalendar: "*-*-* 02:00:00"
walg-retain-full: 7
walg-max-backup-age-hours: 30
alerts-email: ops@example.com
EOF

out=$( (cd "$copy" && AIRFLOW_LIB_ROOT="$root" ./airflow build 2>&1) ) ||
  fail "AIRFLOW_LIB_ROOT did not resolve the working tree: $out"
[ -f "$copy/.colors/launcher-check/airflow-compute/main.tf" ] ||
  fail "the override resolved but rendered nothing"
ok "AIRFLOW_LIB_ROOT resolves a working tree from a copied payload"

# --------------------------------------------------------------------------
# Desired state is found by walking up.
#
# A user runs this from wherever they happen to be in their project, not from
# its root. Only the launcher does this walk; nothing in the library can.

mkdir -p "$copy/deep/nested"
out=$( (cd "$copy/deep/nested" && AIRFLOW_LIB_ROOT="$root" ./../../airflow build 2>&1) ) ||
  fail "running from a subdirectory failed: $out"
[ -f "$copy/.colors/launcher-check/airflow-compute/main.tf" ] ||
  fail "a subdirectory run rendered somewhere other than beside colors.yml"
ok "finds colors.yml by walking up, and renders beside it"

# --------------------------------------------------------------------------
# COLORS_PAR_PROFILE is refused.
#
# The guard that matters most in this package, and the one place the launcher's
# behaviour is worth asserting end to end rather than in a unit test. Three of
# the four OpenTofu stages carry ONCE's stage names, so the profile is the ONLY
# thing separating this project's state from a once-colors' in a shared bucket.

out=$( (cd "$copy" && AIRFLOW_LIB_ROOT="$root" COLORS_PAR_PROFILE=someone-elses ./airflow build 2>&1) || true )
echo "$out" | grep -q 'COLORS_PAR_PROFILE' ||
  fail "COLORS_PAR_PROFILE must be refused by name; got: $out"
if [ -d "$copy/.colors/someone-elses" ]; then
  fail "COLORS_PAR_PROFILE was refused but a work directory was still rendered for it"
fi
ok "COLORS_PAR_PROFILE is refused rather than honoured"

# --------------------------------------------------------------------------
# The contract handshake.

grep -q 'launcher-contract' "$launcher" || fail "the contract handshake is gone"
lc=$(grep -oE '^\s+[0-9]+\)' <<<"$(grep -A6 'def \^:private launcher-contract' "$launcher")" | grep -oE '[0-9]+' | head -1)
libc=$(grep -oE '^\s+[0-9]+\)' <<<"$(grep -A8 'def contract' "$root/src/clj/io/github/getcolors/airflow/utils.clj")" | grep -oE '[0-9]+' | head -1)
[ -n "$lc" ] && [ -n "$libc" ] || fail "could not read both contract numbers"
[ "$lc" -le "$libc" ] ||
  fail "launcher requires contract $lc but the library provides $libc"
ok "launcher contract $lc is satisfied by library contract $libc"

# --------------------------------------------------------------------------
# Unknown verbs.

out=$( (cd "$copy" && AIRFLOW_LIB_ROOT="$root" ./airflow frobnicate 2>&1) || true )
echo "$out" | grep -q 'Usage:' || fail "an unknown verb should print usage; got: $out"
ok "an unknown verb prints usage"

for verb in build create delete; do
  grep -q "\"$verb\"" "$launcher" || fail "the launcher no longer accepts $verb"
done
ok "every verb the workflow implements is dispatchable"

# stop and start are walter's, and are deliberately absent here — `stoppable` is
# empty for every provider this package supports. A launcher that accepted them
# would dispatch into a graph that has no such branch.
for verb in stop start; do
  out=$( (cd "$copy" && AIRFLOW_LIB_ROOT="$root" ./airflow "$verb" 2>&1) || true )
  echo "$out" | grep -q 'Usage:' ||
    fail "$verb should not be dispatchable; got: $out"
done
ok "the power verbs are not dispatchable"

echo "launcher: $checks checks passed"
