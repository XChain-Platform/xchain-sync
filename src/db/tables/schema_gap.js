// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Classify a driver error as a schema gap on an older source: errno 1146 (missing
// table) or 1054 (missing column). Every other error, one with no errno included,
// is a transient or operational fault the caller must re-throw.
function isSchemaGapError(e){
    return !!(e && (e.errno === 1146 || e.errno === 1054));
}

module.exports = { isSchemaGapError };
