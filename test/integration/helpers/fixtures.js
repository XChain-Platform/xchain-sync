// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

// This used to be a stale FORK of the e2e fixtures: it fabricated the
// consensus hashes (so VERIFY_RECOMPUTE could never run on fixture data),
// rebuilt balances with DOUBLE-promoting SQL the applier no longer uses, and
// had missed the ensureIndexAction dedup fix. The integration tier uses only
// a subset of the e2e fixture API (seedBlocks / deleteBlocksFrom), so it now
// re-exports the single maintained implementation — which commits REAL
// computed block hashes (hash-consistent data) and rebuilds balances with the
// shared src/balance-helpers.js SQL.
module.exports = require('../../e2e/helpers/fixtures');
