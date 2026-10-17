// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// A restart, or the next trigger after a failed verification, resumes only after
// every committed block above the last verified block recomputes: a block that halts
// an uninterrupted run still halts after a crash, a recompute error or a lost halt write.
const assert = require('assert');
const H = require('../../helpers/offline_replica');

describe('ClientSync: resume re-verifies every unverified block @regression', function(){
    beforeEach(function(){ H.captureLogs(); });
    afterEach(function(){ H.sinon.restore(); });

    async function drive(sync, origin, from, to){
        for(let h = from; h <= to; h++) await sync.handleEvent(await origin.mint(h, H.stdSpec(h)), 0);
    }

    it('kill after a bad block commits, before its recompute, restart: still halts', async function(){
        let { origin, store, sync, applier } = await H.seed3();
        await sync.incrementalCatchUp(4);
        await applier.applyBlock(await H.tamperedNine(origin));
        let r2 = await H.restart(store);
        if(r2.startup === 'resumed') await drive(r2.sync, origin, 10, 12);
        let hs = H.haltSummary(r2.sync, store);
        assert.strictEqual(hs.halted, true, 'resumed past the bad block to ' + r2.sync.lastAppliedBlock);
        assert.strictEqual(hs.blockIndex, 9);
        assert.strictEqual(await store.getLastBlock(), 9);
    });

    it('transient recompute error on a bad block, then the next live block: still halts', async function(){
        let { origin, store, sync } = await H.seed3();
        await sync.incrementalCatchUp(4); sync.running = true;
        store.faults.strictThrow = 1;
        await sync.handleEvent(await H.tamperedNine(origin), 0);
        await drive(sync, origin, 10, 12);
        let hs = H.haltSummary(sync, store);
        assert.strictEqual(hs.halted, true, 'the unverified bad block was skipped; tip ' + sync.lastAppliedBlock);
        assert.strictEqual(hs.blockIndex, 9);
    });

    it('transient recompute error on a bad block, restart: still halts', async function(){
        let { origin, store, sync } = await H.seed3();
        await sync.incrementalCatchUp(4); sync.running = true;
        store.faults.strictThrow = 1;
        await sync.handleEvent(await H.tamperedNine(origin), 0);
        let r2 = await H.restart(store);
        if(r2.startup === 'resumed') await drive(r2.sync, origin, 10, 12);
        let hs = H.haltSummary(r2.sync, store);
        assert.strictEqual(hs.halted, true, 'restart resumed past the bad block to ' + r2.sync.lastAppliedBlock);
        assert.strictEqual(hs.blockIndex, 9);
    });

    it('halt persistence fails on a bad block, restart: still halts', async function(){
        let { origin, store, sync } = await H.seed3();
        await sync.incrementalCatchUp(4); sync.running = true;
        store.faults.recordHaltThrow = true;
        await sync.handleEvent(await H.tamperedNine(origin), 0);
        assert.strictEqual(sync.isHalted(), true);
        let r2 = await H.restart(store);
        if(r2.startup === 'resumed') await drive(r2.sync, origin, 10, 12);
        let hs = H.haltSummary(r2.sync, store);
        assert.strictEqual(hs.halted, true, 'halt lost; restart resumed to ' + r2.sync.lastAppliedBlock);
        assert.deepStrictEqual(hs.durable, [{ block: 9, reason: 'local-recompute-divergence' }]);
    });
});
