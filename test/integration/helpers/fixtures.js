// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Re-exports the single maintained e2e fixture implementation rather than keeping a
// copy of it. That implementation commits REAL computed block hashes, so hash-
// consistent fixture data lets VERIFY_RECOMPUTE run, and it rebuilds balances with the
// same shared SQL the applier uses. The integration tier needs only seedBlocks and
// deleteBlocksFrom from it, and a second copy is exactly what drifts: fabricated
// consensus hashes, stale balance SQL and a missed dedup fix are what one looks like.
module.exports = require('../../e2e/helpers/fixtures');
