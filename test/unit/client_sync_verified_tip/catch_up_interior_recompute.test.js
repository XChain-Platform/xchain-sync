// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Catch-up recomputes every block of an applied range, not only its join and terminal:
// a hashed difference in a block strictly inside the range halts the replica.
const assert = require('assert');
const H = require('../../helpers/offline_replica');

describe('ClientSync: catch-up verifies every block of the range @regression', function(){
    beforeEach(function(){ H.captureLogs(); });
    afterEach(function(){ H.sinon.restore(); });

    for(const kind of ['changed_row', 'missing_row', 'extra_row', 'hashed_tx_index']){
        it('catch-up ' + kind + ' at an interior block halts', async function(){
            let { origin, sync, store } = await H.seeded(5);
            for(let h = 6; h <= 10; h++) await origin.mint(h, H.stdSpec(h));
            origin.fault = H.snapTamper(kind, 8);
            await sync.handleEvent({ type: 'status', block_height: 10 }, 0);
            let hs = H.haltSummary(sync, store);
            assert.strictEqual(hs.halted, true, 'interior block 8 was accepted: ' +
                JSON.stringify(H.originDiff(origin, store).map(d => d.table)));
            assert.strictEqual(hs.blockIndex, 8);
            assert.strictEqual(hs.reason, 'local-recompute-divergence');
            assert.deepStrictEqual(hs.durable, [{ block: 8, reason: 'local-recompute-divergence' }]);
            // A later live block cannot carry the replica past the halt.
            await sync.handleEvent(await origin.mint(11, H.stdSpec(11)), 0);
            assert.strictEqual(sync.isHalted(), true);
            assert.strictEqual(await store.getLastBlock(), 10);
        });
    }

    it('a clean range verifies and leaves the replica identical to the origin', async function(){
        let { origin, sync, store } = await H.seeded(5);
        for(let h = 6; h <= 10; h++) await origin.mint(h, H.stdSpec(h));
        await sync.handleEvent({ type: 'status', block_height: 10 }, 0);
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.lastAppliedBlock, 10);
        assert.deepStrictEqual(H.originDiff(origin, store), []);
    });
});
