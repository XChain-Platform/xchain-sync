// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

// CONSENSUS regression: getBlockLeafRows must canonicalize protocol special
// addresses in the ledger rows it feeds to block_merkle_root, byte-for-byte with
// BlockHasher and the indexer's getBlockHashes. The indexer commits block_merkle_root
// over canonicalized ledger rows (its getBlockLeafRows reuses the getBlockHashes
// stash); if the follower's getBlockLeafRows queried raw addresses, the recomputed
// merkle root would diverge on the first special-address block and the follower would
// halt. Canonicalization is ledger-only (credits/debits/escrows); contract rows stay
// raw, matching the source.

const assert  = require('assert');
const sinon   = require('sinon');
const Database = require('../../src/db');
const { ROLE_BY_ADDRESS } = require('../../src/protocolAddressRoles');

// Pick one concrete special address per ledger role from the frozen map.
function addrForRole(role){
    const hit = Object.keys(ROLE_BY_ADDRESS).find(a => ROLE_BY_ADDRESS[a] === role);
    assert.ok(hit, `no special address found for role ${role}`);
    return hit;
}

describe('getBlockLeafRows special-address canonicalization (consensus)', function () {

    let db;
    const PLAIN = 'mq7tVfobimRUPxPNnyd5mKn11SVmTiLxtu';
    const DONATE1 = addrForRole('DONATE1');
    const BURN    = addrForRole('BURN');
    const REWARD  = addrForRole('REWARD');

    beforeEach(function () {
        db = new Database('localhost', 3306, 'idx', 'u', 'p', { isNull: x => x == null }, 'indexer');
        // Return one row per ledger table (special address) plus a contract row whose
        // source address is ALSO a special address, to prove contract rows are left raw.
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/FROM credits/.test(sql))  return [{ action_index: 1, address: DONATE1, tick: 'XCHAIN', amount: '1' }];
            if (/FROM debits/.test(sql))   return [{ action_index: 1, address: BURN,    tick: 'XCHAIN', amount: '1' }];
            if (/FROM escrows/.test(sql))  return [{ action_index: 1, address: REWARD,  tick: 'XCHAIN', amount: '1' }];
            if (/FROM contracts/.test(sql)) return [{ action_index: 1, source_address: DONATE1, code_hash: 'h', status: 'valid' }];
            return [];
        });
    });

    afterEach(function () { sinon.restore(); });

    it('canonicalizes credit/debit/escrow special addresses to their role token', async function () {
        const rows = await db.getBlockLeafRows(100);
        assert.strictEqual(rows.ledger.credits[0].address, 'DONATE1');
        assert.strictEqual(rows.ledger.debits[0].address,  'BURN');
        assert.strictEqual(rows.ledger.escrows[0].address, 'REWARD');
    });

    it('leaves contract source addresses raw (ledger-only canonicalization, mirrors the source)', async function () {
        const rows = await db.getBlockLeafRows(100);
        assert.strictEqual(rows.contracts.contracts[0].source_address, DONATE1);
    });

    it('passes a non-special ledger address through unchanged', async function () {
        db.doQuery.restore();
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/FROM credits/.test(sql)) return [{ action_index: 1, address: PLAIN, tick: 'XCHAIN', amount: '1' }];
            return [];
        });
        const rows = await db.getBlockLeafRows(100);
        assert.strictEqual(rows.ledger.credits[0].address, PLAIN);
    });
});
