// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// applyProtocolHaltFreshness overlays the active-halt flag on a replica
// health row and forces the row stale when a halt is active or unreadable.

const assert = require('assert');
const { applyProtocolHaltFreshness } = require('../../../src/api');

function baseRow(){
    return { replica_stale: false, lag_blocks: 3 };
}

describe('applyProtocolHaltFreshness', () => {
    it('sets replica_halted null for a null db', async () => {
        const row = baseRow();
        const out = await applyProtocolHaltFreshness(row, null, 'mysql');
        assert.strictEqual(out, row);
        assert.deepStrictEqual(row, { replica_stale: false, lag_blocks: 3, replica_halted: null });
    });

    it('sets replica_halted null when db lacks getActiveHalt', async () => {
        const row = baseRow();
        const out = await applyProtocolHaltFreshness(row, {}, 'mysql');
        assert.strictEqual(out, row);
        assert.deepStrictEqual(row, { replica_stale: false, lag_blocks: 3, replica_halted: null });
    });

    it('leaves staleness and lag alone when no halt is active', async () => {
        const row = baseRow();
        const out = await applyProtocolHaltFreshness(row, { getActiveHalt: async () => false }, 'mysql');
        assert.strictEqual(out, row);
        assert.deepStrictEqual(row, { replica_stale: false, lag_blocks: 3, replica_halted: false });
    });

    it('marks the row stale with null lag when a halt is active', async () => {
        const row = baseRow();
        const out = await applyProtocolHaltFreshness(row, { getActiveHalt: async () => true }, 'mysql');
        assert.strictEqual(out, row);
        assert.deepStrictEqual(row, { replica_stale: true, lag_blocks: null, replica_halted: true });
    });

    it('calls getActiveHalt once with the dbType', async () => {
        const calls = [];
        const db = { getActiveHalt: async (...args) => { calls.push(args); return false; } };
        const row = baseRow();
        const out = await applyProtocolHaltFreshness(row, db, 'sqlite');
        assert.strictEqual(out, row);
        assert.deepStrictEqual(calls, [['sqlite']]);
    });

    it('marks the row stale with unknown halt when the lookup rejects', async () => {
        const row = baseRow();
        const db = { getActiveHalt: async () => { throw new Error('boom'); } };
        const out = await applyProtocolHaltFreshness(row, db, 'mysql');
        assert.strictEqual(out, row);
        assert.deepStrictEqual(row, { replica_stale: true, lag_blocks: null, replica_halted: null });
    });
});
