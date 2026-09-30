/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * Armed-map fingerprint v2 for the running process.
 *
 * Since W3 this is the armed-map identity. It hashes the resolved registry
 * rows through the canonical serialisation, so it moves when a height or
 * constant does and stays put under a carrier reformat or move.
 *
 * On failure the value is the literal UNREADABLE, never a hex string: a build
 * that cannot read its own armed map must not publish something a fleet sweep
 * could match against a healthy peer.
 *
 ********************************************************************/

'use strict';

const { collectRows } = require('./manifest');
const { fingerprint } = require('./canonical');

// Memoised per process like v1: the rows are fixed at load, and /health is
// probed often enough that recomputing on every request would be waste.
let cached = null;

/**
 * @returns {{hex: string, rows: Object<string, string>, count: number}|{hex: 'UNREADABLE', reason: string}}
 */
function computeArmedMapFingerprint() {
    if (cached) return cached;
    const collected = collectRows();
    if (!collected.ok) {
        cached = { hex: 'UNREADABLE', reason: collected.reason };
        return cached;
    }
    try {
        const fp = fingerprint(collected.rows);
        cached = { hex: fp.hex, rows: fp.rows, count: fp.count };
    } catch (e) {
        // A key outside the grammar or a duplicate key is a manifest defect;
        // it poisons the value the same way an unreadable carrier does.
        cached = { hex: 'UNREADABLE', reason: e.message };
    }
    return cached;
}

module.exports = {
    computeArmedMapFingerprint,
    computeArmedMapFingerprintV2: computeArmedMapFingerprint,
};
