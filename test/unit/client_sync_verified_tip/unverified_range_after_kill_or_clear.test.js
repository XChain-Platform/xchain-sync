// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// A reorg that crossed a disconnect, with the process killed after the catch-up
// range committed and before it was verified, must not leave orphaned blocks under
// the new fork; and clearing a halt never lets the replica follow on unverified rows.
const assert = require('assert');
const H = require('../../helpers/offline_replica');

describe('ClientSync: unverified ranges after a kill or a halt clear @regression', function(){
    beforeEach(function(){ H.captureLogs(); });
    afterEach(function(){ H.sinon.restore(); });

    async function drive(sync, origin, from, to, salt){
        for(let h = from; h <= to; h++) await sync.handleEvent(await origin.mint(h, H.stdSpec(h, salt)), 0);
    }

    it('disconnected reorg, kill before the range is verified, restart: halts or converges', async function(){
        let { origin, store, sync } = await H.seed3();
        await sync.incrementalCatchUp(4);                       // replica at 8 on the old fork
        origin.rollbackTo(6);
        for(let h = 6; h <= 10; h++) await origin.mint(h, H.stdSpec(h, 1000));
        sync.verifyIncrementalCatchUpRange = async () => { throw new Error('killed after commit'); };
        await sync.incrementalCatchUp(9);
        let r2 = await H.restart(store);
        if(r2.startup === 'resumed') await drive(r2.sync, origin, 11, 13, 1000);
        let diff = H.originDiff(origin, store);
        assert.ok(r2.sync.isHalted() || diff.length === 0,
            'followed the new fork on top of orphaned blocks: ' + JSON.stringify(diff.map(d => d.table)));
    });

    it('clearHalt then the next new block: halts again or converges', async function(){
        let { origin, sync, store } = await H.hotfixed();
        await sync.handleEvent(await origin.mint(11, H.stdSpec(11)), 0);
        assert.strictEqual(sync.isHalted(), true);
        await sync.clearHalt();
        await drive(sync, origin, 12, 14, 0);
        let diff = H.originDiff(origin, store);
        assert.ok(sync.isHalted() || diff.length === 0,
            'after clearHalt the replica followed to ' + sync.lastAppliedBlock + ' on pre-hotfix rows');
    });

    it('clearHalt then restart: halts again or converges', async function(){
        let { origin, sync, store } = await H.hotfixed();
        await sync.handleEvent(await origin.mint(11, H.stdSpec(11)), 0);
        await sync.clearHalt();
        let r2 = await H.restart(store);
        if(r2.startup === 'resumed') await drive(r2.sync, origin, 12, 14, 0);
        let diff = H.originDiff(origin, store);
        assert.ok(r2.sync.isHalted() || diff.length === 0,
            'clear plus restart resumed to ' + r2.sync.lastAppliedBlock + ' on pre-hotfix rows');
    });
});
