#!/usr/bin/env bash
#********************************************************************
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
#********************************************************************
#
# Run the DB-backed test tiers against a throwaway MariaDB.
#
# WHY THIS EXISTS. The unit tier runs anywhere; the integration and e2e tiers need a
# database, two sibling schemas and five environment variables, and none of that was
# written down. That is the difference between "run it by hand" and "never run again",
# and it matters more than usual right now: while GitHub Actions is not executing,
# these tiers run in NO automation, so a human typing one command is the only thing
# standing between a regression and a release.
#
# It brings up its own MariaDB (throwaway, tmpfs-backed, gone on exit), resolves the
# sibling schemas, exports what the harness expects, runs the tier, and tears down.
# The root password is the same documented fixture value the repo's own
# test/e2e/docker-compose.e2e.yml uses: this database holds nothing but generated
# fixtures and does not outlive the run, so there is no credential here to steward.
#
# USAGE
#   bin/run-db-tiers.sh                       # integration tier (default)
#   bin/run-db-tiers.sh integration e2e       # both, in order
#   bin/run-db-tiers.sh unit                  # unit tier WITH siblings required
#   bin/run-db-tiers.sh --keep integration    # leave the database up afterwards
#   bin/run-db-tiers.sh -- test/integration/replication-insert-shape.test.js
#                                             # one file, straight to mocha
#
# ENVIRONMENT (all optional)
#   DB_PORT                  host port for the throwaway MariaDB (default 23316)
#   XCHAIN_INDEXER_SQL_PATH  overrides the sibling lookup
#   XCHAIN_DECODER_SQL_PATH  overrides the sibling lookup
#
#********************************************************************

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

CONTAINER="xchain-sync-dbtier"
DB_PORT="${DB_PORT:-23316}"
DB_PASS="test"                 # fixture value, see the header
IMAGE="mariadb:11.4"
KEEP=0
TIERS=()
MOCHA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keep) KEEP=1; shift ;;
        --) shift; MOCHA_ARGS=("$@"); break ;;
        # Print the usage block itself, located by its heading rather than by line
        # numbers, which drift the moment the header comment is edited.
        -h|--help) sed -n '/^# USAGE/,/^#\*\{10,\}$/p' "${BASH_SOURCE[0]}"; exit 0 ;;
        unit|integration|e2e) TIERS+=("$1"); shift ;;
        *) echo "run-db-tiers: unknown argument '$1' (want unit|integration|e2e, --keep, --, -h)" >&2
           exit 64 ;;
    esac
done
if [[ ${#TIERS[@]} -eq 0 && ${#MOCHA_ARGS[@]} -eq 0 ]]; then TIERS=(integration); fi

command -v docker >/dev/null 2>&1 || {
    echo "run-db-tiers: docker is required (it hosts the throwaway MariaDB)." >&2; exit 69; }

# Sibling schemas. The integration and e2e harnesses seed the CANONICAL indexer and
# decoder DDL rather than a copy, so a missing sibling is a real limitation and not a
# detail to paper over. When both are present XCHAIN_REQUIRE_SIBLINGS is set, which is
# what stops the cross-repo drift guards passing by SKIPPING: several of them are
# no-ops without a sibling and would otherwise report green having checked nothing.
INDEXER_SQL="${XCHAIN_INDEXER_SQL_PATH:-$REPO_ROOT/../xchain-indexer/src/sql}"
DECODER_SQL="${XCHAIN_DECODER_SQL_PATH:-$REPO_ROOT/../xchain-decoder/src/sql}"
SIBLINGS_OK=1
[[ -d "$INDEXER_SQL" ]] || { echo "run-db-tiers: no indexer schema at $INDEXER_SQL" >&2; SIBLINGS_OK=0; }
[[ -d "$DECODER_SQL" ]] || { echo "run-db-tiers: no decoder schema at $DECODER_SQL" >&2; SIBLINGS_OK=0; }
if [[ $SIBLINGS_OK -eq 0 ]]; then
    echo "run-db-tiers: continuing, but sibling-dependent guards will SKIP rather than run." >&2
fi

teardown(){
    local status=$?
    if [[ $KEEP -eq 1 ]]; then
        echo "run-db-tiers: leaving $CONTAINER up on port $DB_PORT (--keep)."
    else
        docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    fi
    exit $status
}
trap teardown EXIT

docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
echo "run-db-tiers: starting $IMAGE as $CONTAINER on 127.0.0.1:$DB_PORT (tmpfs)"
docker run -d --name "$CONTAINER" \
    -e MARIADB_ROOT_PASSWORD="$DB_PASS" \
    -p "127.0.0.1:$DB_PORT:3306" \
    --tmpfs /var/lib/mysql \
    "$IMAGE" >/dev/null

# Poll rather than sleep a fixed amount: a cold image pull makes a fixed wait either
# flaky or slow, and the container ships the healthcheck that answers this exactly.
echo -n "run-db-tiers: waiting for the database"
for _ in $(seq 1 60); do
    if docker exec "$CONTAINER" healthcheck.sh --connect --innodb_initialized >/dev/null 2>&1; then
        echo " ready"; break
    fi
    echo -n "."; sleep 2
done
docker exec "$CONTAINER" healthcheck.sh --connect --innodb_initialized >/dev/null 2>&1 || {
    echo; echo "run-db-tiers: the database never came up; last container logs:" >&2
    docker logs --tail 20 "$CONTAINER" >&2; exit 75; }

export TEST_DB_HOST=127.0.0.1
export TEST_DB_PORT="$DB_PORT"
export TEST_DB_USER=root
export TEST_DB_PASS="$DB_PASS"
export XCHAIN_INDEXER_SQL_PATH="$INDEXER_SQL"
export XCHAIN_DECODER_SQL_PATH="$DECODER_SQL"
[[ $SIBLINGS_OK -eq 1 ]] && export XCHAIN_REQUIRE_SIBLINGS=1

rc=0
if [[ ${#MOCHA_ARGS[@]} -gt 0 ]]; then
    echo "run-db-tiers: mocha ${MOCHA_ARGS[*]}"
    npx mocha --timeout 300000 --exit "${MOCHA_ARGS[@]}" || rc=$?
else
    for tier in "${TIERS[@]}"; do
        echo "run-db-tiers: === $tier tier ==="
        case "$tier" in
            unit)        npm test || rc=$? ;;
            integration) npm run test:integration:ci || rc=$? ;;
            e2e)         npm run test:e2e:ci || rc=$? ;;
        esac
        # Keep going through a failing tier so one red tier cannot hide the state of the
        # next, and report the first failure's status at the end.
        [[ $rc -ne 0 ]] && echo "run-db-tiers: $tier tier FAILED (rc=$rc), continuing" >&2
    done
fi

echo "run-db-tiers: done (rc=$rc)"
exit $rc
