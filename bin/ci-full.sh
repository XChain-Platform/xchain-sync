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
run_tier() {
  local name="$1"; shift
  echo; echo "ci:full ===== $name ====="
  if "$@"; then
    echo "ci:full ----- $name PASS"
  else
    FAILED="$FAILED [$name]"
    echo "ci:full ----- $name FAIL"
  fi
}
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
run_tier "ci" npm run ci

# --- job: e2e ----------------------------------------------------------------
# GitHub stands up source-db (:23306) and replica-db (:23307) as service
# containers before any step runs; test/e2e/docker-compose.e2e.yml is the
# same pair (same image, ports, MARIADB_USER/PASSWORD), and every e2e/testDb
# helper already defaults to those ports and credentials, so no env override
# is needed once the stack is up.
E2E_COMPOSE="test/e2e/docker-compose.e2e.yml"
e2e_compose_down() {
  docker compose -f "$E2E_COMPOSE" down -v >/dev/null 2>&1
}
trap e2e_compose_down EXIT
run_tier "e2e: bring up service containers (source-db, replica-db)" \
  docker compose -f "$E2E_COMPOSE" up -d --wait

# Cross-repo consensus drift guards (rollback-coverage and friends) live in
# the unit tier but the shared `ci` job never checks out a sibling, so they
# silently skip there. Run them HERE, where xchain-indexer and xchain-decoder
# ARE checked out, with XCHAIN_REQUIRE_SIBLINGS=1 so a missing sibling
# hard-fails instead of green-by-skip. Pure source comparisons (no DB).
run_tier "e2e: cross-repo consensus drift guards" \
  env XCHAIN_REQUIRE_SIBLINGS=1 \
  npx mocha --timeout 10000 \
    test/unit/rollback-coverage.test.js \
    test/unit/blockhash-conformance-twin.test.js \
    test/unit/protocolAddressRoles.twin.test.js \
    test/unit/stakesValidatorSetParity.test.js \
    test/unit/generatedColumns.test.js

run_tier "e2e: e2e tier (test:e2e:ci)" npm run test:e2e:ci

# Independent of the e2e tier above (own DBs, own schema seed); run even if
# the e2e tier failed, so a flake there can't mask the integration result.
# Reuses source-db (:23306) with the admin credentials, not the e2e
# xchain-node user, matching the workflow step exactly.
run_tier "e2e: integration tier (green suites, test:integration:ci)" \
  env TEST_DB_HOST=127.0.0.1 TEST_DB_PORT=23306 TEST_DB_USER=root TEST_DB_PASS=test \
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

# --- job: coverage -----------------------------------------------------------
run_tier "coverage ratchet (coverage:check)" npm run coverage:check

echo
if [ -n "$FAILED" ]; then
  echo "ci:full: RED tiers:$FAILED"
  exit 1
fi
echo "ci:full: all tiers green (same set GitHub CI runs)"
