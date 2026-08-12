// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// This module used to fork the e2e fixtures and drifted out of sync with them
// (fabricated consensus hashes, stale balance SQL, a missed dedup fix). It now
// re-exports the single maintained e2e implementation instead, so the
// integration tier can't drift from it again.
module.exports = require('../../e2e/helpers/fixtures');
