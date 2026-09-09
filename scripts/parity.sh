#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
state="$root/test/fixtures/colors.yml"
build_variant() {
  local variant=$1; shift
  local fixture="$state"
  if [ "$variant" = managed ]; then
    fixture="$tmp/managed-colors.yml"
    sed '/^digitalocean-ssh-keys:/d' "$state" > "$fixture"
  fi
  (cd "$root/green" && env COLORS_PAR_WORKDIR="$tmp/$variant/green" "$@" bb green build -f "$fixture" >/dev/null)
  (cd "$root/red" && env COLORS_PAR_WORKDIR="$tmp/$variant/red" "$@" ./red build -f "$fixture" >/dev/null)
  (cd "$root/blue" && env COLORS_PAR_WORKDIR="$tmp/$variant/blue" "$@" uv run python -m package_airflow_blue build -f "$fixture" >/dev/null)
  diff -qr "$tmp/$variant/green/airflow-fixture" "$tmp/$variant/red/airflow-fixture"
  diff -qr "$tmp/$variant/green/airflow-fixture" "$tmp/$variant/blue/airflow-fixture"
}
build_variant digitalocean
build_variant azure COLORS_PAR_PROVIDER_COMPUTE=azure
build_variant aws COLORS_PAR_PROVIDER_COMPUTE=aws
build_variant google COLORS_PAR_PROVIDER_COMPUTE=google
build_variant hcloud COLORS_PAR_PROVIDER_COMPUTE=hcloud
build_variant oci COLORS_PAR_PROVIDER_COMPUTE=oci
build_variant yandex COLORS_PAR_PROVIDER_COMPUTE=yandex
build_variant vultr COLORS_PAR_PROVIDER_COMPUTE=vultr
build_variant managed
build_variant no-infra-services COLORS_PAR_PROVIDER_DNS=no-infra COLORS_PAR_PROVIDER_SMTP=no-infra
build_variant acme COLORS_PAR_CADDY_ACME_EMAIL=ops@fixture.example
build_variant s3 COLORS_PAR_PROVIDER_BACKEND=s3
build_variant r2 COLORS_PAR_PROVIDER_BACKEND=r2
diff -qr "$root/green/src/resources/io/github/getcolors/airflow" "$root/red/resources"
diff -qr "$root/green/src/resources/io/github/getcolors/airflow" "$root/blue/src/package_airflow_blue/resources"
echo "green, red, and blue Airflow artifacts are byte-identical"
