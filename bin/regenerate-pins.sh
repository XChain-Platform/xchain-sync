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
# Regenerate this repository's generated pins from the tree: the pins the
# platform's generated-pins list names for it. On an unchanged tree it writes
# nothing.
#
#   bin/regenerate-pins.sh                  every pin below
#   bin/regenerate-pins.sh identity         bin/pins/identity.json
#   bin/regenerate-pins.sh carrier-logic    bin/pins/carrier-logic.json (checked, never rewritten)
#
# THE SUITE-TITLE PIN IS NOT HERE. bin/pins/at1-suite-titles.json is a frozen
# base in this repository, graded through the rename, name and split maps
# beside it (the `compare` line of bin/pins/suite-title-splits.json), so a
# fresh `--out` is a different file by design and would discard that record.
#
# THE CARRIER LOGIC PIN has no regenerate, by design: an entry moves only
# through `bin/lib/carrier_logic_pin.js --write --id <id> --reason "<text>"`,
# which records the re-pin, and its unit test refuses an entry that moved
# without that record. So this script checks it and fails if it does not hold.
# It runs before the identity pin, which reads the carrier hashes.

set -u
cd "$(dirname "$0")/.." || exit 2

carrier_logic() { node bin/lib/carrier_logic_pin.js >/dev/null; }

identity() { node bin/pin-identity.js --out bin/pins/identity.json >/dev/null; }

[ "$#" -gt 0 ] || set -- carrier-logic identity
rc=0
for pin in "$@"; do
  case "$pin" in
    carrier-logic) carrier_logic ;;
    identity)      identity ;;
    *) echo "regenerate-pins: unknown pin: $pin" >&2; exit 2 ;;
  esac
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "regenerate-pins: $pin failed (exit $status)" >&2
    rc=1
  fi
done
exit "$rc"
