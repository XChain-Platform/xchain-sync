#!/usr/bin/env bash
#*********************************************************************
#
# Copyright © 2025-2026 Dankest, LLC
# Based on XChain Platform by Dankest, LLC - https://dankest.llc
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of XChain Platform. Licensed under the GNU Affero
# General Public License v3.0 or later; see LICENSE.md. A commercial
# license (without AGPL source-disclosure terms) is available -
# contact legal@dankest.llc.
#
#*********************************************************************

#
# bin/ci-full.sh: run EVERY tier this repo's GitHub CI runs, in one process.
#
# .github/workflows/ci.yml fans this repo out as four jobs (ci, e2e,
# drift-guards, coverage). The pre-push venue gate used to run only
# `npm run ci`, so a push could gate green locally and then go red on GitHub
# on a job the gate never ran (2026-08-15: exactly that, on three repos at
# once). This script IS the local twin of the workflow: every job's run
# steps, transcribed, in job order. When ci.yml gains or changes a job,
# change this script in the same commit.
#
# Layout: siblings resolve at ../<repo>, which is both the platform monorepo
# layout and the venue gate's work/ layout (.ci-siblings ships them there). A
# sibling a GitHub job checks out is REQUIRED here: missing means fail loud,
# never skip, because GitHub will run the step this gate would be skipping.
#
# The e2e job spins up its OWN throwaway MariaDB pair (source-db + replica-db,
# test/e2e/docker-compose.e2e.yml) rather than a venue-provided database,
# matching the two GitHub Actions service containers exactly (same image,
# ports, and fixture credentials). It needs Docker; a docker-less venue fails
# this script loud rather than silently skip the tier GitHub actually runs.
#
# SKIPPED-BY-DESIGN: none. Every real test/build step ci.yml runs is
# transcribed below (checkout/setup-node/npm-ci/cache steps are GitHub-only
# bookkeeping and need no transcription).
#
# All tiers run even after one fails (GitHub reports every red job, so this
# reports every red tier); the exit code is red if any tier was.
#
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
SELF="$(pwd)"
SIB="$(cd .. && pwd)"

FAILED=""
# >>> ci-tier (generated block; re-run the tier wirer to update) >>>
# Tier classes. A push grades the FAST tier only: the unit job, the pin and
# drift guards, and the structure and hygiene checks the hook runs before it
# dispatches. The tiers named below (coverage re-runs, perf scenarios) are
# skipped when the gate sets CI_TIER=fast, and each skip is recorded so the
# closing verdict can never claim a green it did not earn. Nothing stops
# being graded: a scheduled sweep re-runs this same script with CI_TIER=full
# on every repo every three hours and before any release or deploy, and a
# red there is tracked down and fixed first. CI_TIER is unset for a hand
# run, so a bare `npm run ci:full` still runs every tier as it always did.
CI_TIER_FULL_ONLY=(
  "coverage ratchet (coverage:check)"
)
DEFERRED=""
ci_tier_deferred() {
  [ "${CI_TIER:-full}" = "fast" ] || return 1
  local t
  for t in ${CI_TIER_FULL_ONLY[@]+"${CI_TIER_FULL_ONLY[@]}"}; do
    if [ "$t" = "$1" ]; then
      DEFERRED="$DEFERRED [$1]"
      echo; echo "ci:full ===== $1 DEFERRED (CI_TIER=fast, runs in the full sweep) ====="
      return 0
    fi
  done
  return 1
}
# <<< ci-tier <<<
# >>> ci-tier timer (generated block; re-run the tier wirer to update) >>>
run_tier() {
  ci_tier_deferred "$1" && return 0  # ci-tier guard (generated)
  local name="$1"; shift
  local __ci_tier_t0=$SECONDS
  echo; echo "ci:full ===== $name ====="
  if "$@"; then
    echo "ci:full ----- $name PASS ($(( SECONDS - __ci_tier_t0 ))s)"
  else
    FAILED="$FAILED [$name]"
    echo "ci:full ----- $name FAIL ($(( SECONDS - __ci_tier_t0 ))s)"
  fi
}
# <<< ci-tier timer <<<
need_sib() {
  local s
  for s in "$@"; do
    if [ ! -d "$SIB/$s" ]; then
      echo "ci:full: MISSING SIBLING $SIB/$s" >&2
      echo "ci:full: GitHub CI checks this sibling out and runs steps against it," >&2
      echo "ci:full: so skipping here would gate green on a subset. Declare it in" >&2
      echo "ci:full: .ci-siblings (venue) or clone it beside this repo (hand run)." >&2
      exit 1
    fi
  done
}

export XCHAIN_INDEXER_SQL_PATH="${XCHAIN_INDEXER_SQL_PATH:-$SIB/xchain-indexer/src/sql}"
export XCHAIN_DECODER_SQL_PATH="${XCHAIN_DECODER_SQL_PATH:-$SIB/xchain-decoder/src/sql}"

need_sib xchain-indexer xchain-decoder xchain-hub

# The e2e job (below) needs Docker for its two service containers; guard once,
# up front, so a docker-less venue fails loud instead of every DB-backed tier
# failing separately with a confusing connection-refused error.
docker info >/dev/null 2>&1 || {
  echo "ci:full: VENUE LACKS DOCKER for e2e job (source-db/replica-db service" >&2
  echo "ci:full: containers, e2e tier, integration tier); pin a docker venue" >&2
  echo "ci:full: with CI_VENUES=..." >&2
  exit 1
}

# --- job: ci (XChain-Platform/.github ci-reusable.yml -> npm run ci) -------
run_tier "ci" env XCHAIN_REQUIRE_SIBLINGS=1 npm run ci

# --- job: e2e ----------------------------------------------------------------
# GitHub stands up source-db (:23306) and replica-db (:23307) as service
# containers before any step runs; test/e2e/docker-compose.e2e.yml is the
# same pair (same image, ports, MARIADB_USER/PASSWORD), and every e2e/testDb
# helper already defaults to those ports and credentials, so no env override
# is needed once the stack is up.
E2E_COMPOSE="test/e2e/docker-compose.e2e.yml"
E2E_DB_PORT_RESOLVED="$(node bin/fixture-ports.js port E2E_DB_PORT)" || exit 1
e2e_compose_down() {
  node bin/fixture-ports.js compose "$E2E_COMPOSE" down -v >/dev/null 2>&1
}
trap e2e_compose_down EXIT
run_tier "e2e: bring up service containers (source-db, replica-db)" \
  node bin/fixture-ports.js compose "$E2E_COMPOSE" up -d --wait

# Cross-repo consensus drift guards (rollback-coverage and friends) live in
# the unit tier but the shared `ci` job never checks out a sibling, so they
# silently skip there. Run them HERE, where xchain-indexer and xchain-decoder
# ARE checked out, with XCHAIN_REQUIRE_SIBLINGS=1 so a missing sibling
# hard-fails instead of green-by-skip. Pure source comparisons (no DB).
run_tier "e2e: cross-repo consensus drift guards" \
  env XCHAIN_REQUIRE_SIBLINGS=1 \
  npx mocha --timeout 10000 \
    test/unit/rollback_coverage.test.js \
    test/unit/blockhash_conformance_twin.test.js \
    test/unit/protocol_address_roles_twin.test.js \
    test/unit/stakes_validator_set_parity.test.js \
    test/unit/generated_columns.test.js

run_tier "e2e: e2e tier (test:e2e:ci)" npm run test:e2e:ci

# Independent of the e2e tier above (own DBs, own schema seed); run even if
# the e2e tier failed, so a flake there can't mask the integration result.
# Reuses source-db (:23306) with the admin credentials, not the e2e
# xchain-node user, matching the workflow step exactly.
run_tier "e2e: integration tier (green suites, test:integration:ci)" \
  env TEST_DB_HOST=127.0.0.1 TEST_DB_PORT="$E2E_DB_PORT_RESOLVED" TEST_DB_USER=root TEST_DB_PASS=test \
  npm run test:integration:ci

run_tier "e2e: tear down service containers" e2e_compose_down
trap - EXIT

# --- job: drift-guards -------------------------------------------------------
# Run FROM the parent so sync-coins.sh sees the canonical + vendored pair the
# way the workflow lays them out (hub checkout beside this repo's checkout).
sync_coins_check() { (cd "$SIB" && "xchain-hub/bin/sync-coins.sh" --check --only "$(basename "$SELF")"); }
run_tier "drift-guards: coin-registry byte-identity" sync_coins_check
run_tier "drift-guards: coin consensus-pin conformance" node -e '
  const coins = require("./src/coins");
  for (const net of ["testnet", "regtest"]) {
    const res = coins.verifyConsensusPin(net);
    if (res && res.skipped) throw new Error("consensus pin unexpectedly unarmed for " + net);
  }
  console.log("consensus pin conformance OK (testnet, regtest)");
'

# --- identity pin (this gate only; no ci.yml job runs it) --------------------
# bin/pins/identity.json holds the armed-map fingerprint, the per-file hashes
# behind it, and the sha256 of each vendored coin file. Nothing else reads it,
# so this tier re-hashes the tree against it and fails on any moved, renamed,
# missing or unreadable carrier instead of letting the pin go stale.
run_tier "identity pin (armed map, vendored coins)" node bin/pin-identity.js --compare bin/pins/identity.json

# --- job: coverage -----------------------------------------------------------
run_tier "coverage ratchet (coverage:check)" env XCHAIN_REQUIRE_SIBLINGS=1 npm run coverage:check

echo
# >>> ci-tier summary (generated) >>>
echo "ci:full: tier class ${CI_TIER:-full}"
if [ -n "${DEFERRED:-}" ]; then
  echo "ci:full: DEFERRED to the full sweep:$DEFERRED"
fi
# <<< ci-tier summary <<<
if [ -n "$FAILED" ]; then
  echo "ci:full: RED tiers:$FAILED"
  exit 1
fi
# >>> ci-tier verdict (generated) >>>
if [ "${CI_TIER:-full}" = "fast" ]; then
  echo "ci:full: all FAST tiers green; the DEFERRED tiers above were NOT graded here"
else
  echo "ci:full: all tiers green (same set GitHub CI runs)"
fi
# <<< ci-tier verdict <<<
