'use strict';

// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const specs = require('../../../../src/server/updated_rows/table_specs.js');

function assertNonEmptyNames(tables){
    assert.ok(tables.every((table) => typeof table === 'string' && table.length > 0));
}

function assertTableList(tables){
    assertNonEmptyNames(tables);
    assert.strictEqual(new Set(tables).size, tables.length);
}

// Registers the import-seam checks inside the calling describe block.
function registerAttestSeamTests(){
    describe('attest constants import seam', function () {
        const srcRoot = path.join(__dirname, '../../../../src');
        const read = (rel) => fs.readFileSync(path.join(srcRoot, rel), 'utf8');

        it('keeps the defining module free of require calls', function () {
            assert.ok(!/\brequire\s*\(/.test(read('server/updated_rows/table_specs.js')),
                'client/rollback.js imports the ATTEST constants from this module as a dependency-free leaf');
        });

        it('has the client rollback import the attest constants from the defining module', function () {
            const source = read('client/rollback.js');
            const match = source.match(/const\s*\{([^}]*ATTEST_BATCH_HEAD_VERSION[^}]*)\}\s*=\s*require\(([^)]*)\)/);
            assert.ok(match, 'rollback.js destructures the ATTEST batch constants from a require');
            assert.strictEqual(match[2].replace(/['"]/g, ''), '../server/updated_rows/table_specs.js');
            for(const name of ['ATTEST_BATCH_HEAD_VERSION', 'ATTEST_BATCH_CONTINUATION_VERSION',
                'ATTEST_BATCH_COMPLETION_STAMP'])
                assert.ok(match[1].includes(name), `${name} comes from the same require`);
        });
    });
}

describe('updated row table specifications', function () {
    it('pins the direct table lists and keeps every name non-empty and unique', function () {
        const expected = {
            DEACTIVATION_TABLES: ['stakes', 'delegations', 'contract_stakes', 'contract_delegations'],
            ROTATION_TABLES: ['contract_stakes', 'contract_unstakes'],
            REQUEST_STATUS_TABLES: ['attests', 'xcalls'],
            POLL_FINALIZE_TABLES: ['polls'],
            COOLDOWN_STATUS_TABLES: ['unstakes', 'contract_unstakes']
        };

        for(const [name, tables] of Object.entries(expected)){
            assert.deepStrictEqual(specs[name], tables);
            assertTableList(specs[name]);
        }
    });

    it('pins slash specs and exposes every key used to build the debit join', function () {
        assert.deepStrictEqual(specs.SLASH_SPECS, [
            { table: 'contract_stakes', debits: 'contract_slash_debits', target: 'contract_stakes' },
            { table: 'contract_unstakes', debits: 'contract_slash_debits', target: 'contract_unstakes' },
            { table: 'stakes', debits: 'capability_slash_debits', target: 'stakes' },
            { table: 'unstakes', debits: 'capability_slash_debits', target: 'unstakes' }
        ]);
        assertTableList(specs.SLASH_SPECS.map((spec) => spec.table));
        for(const spec of specs.SLASH_SPECS){
            assert.deepStrictEqual(Object.keys(spec).sort(), ['debits', 'table', 'target']);
            assertNonEmptyNames([spec.table, spec.debits, spec.target]);
        }
    });

    it('pins bet specs and exposes every key used to build the stamp predicate', function () {
        assert.deepStrictEqual(specs.BET_STATUS_SPECS, [
            { table: 'bet_feeds', stamps: ['closed_block', 'terminal_block'] },
            { table: 'bets', stamps: ['settled_block'] }
        ]);
        assertTableList(specs.BET_STATUS_SPECS.map((spec) => spec.table));
        for(const spec of specs.BET_STATUS_SPECS){
            assert.deepStrictEqual(Object.keys(spec).sort(), ['stamps', 'table']);
            assertTableList(spec.stamps);
        }
    });

    it('pins the attest batch versions and completion marker', function () {
        assert.strictEqual(specs.ATTEST_BATCH_HEAD_VERSION, 5);
        assert.strictEqual(specs.ATTEST_BATCH_CONTINUATION_VERSION, 6);
        assert.ok(Number.isInteger(specs.ATTEST_BATCH_HEAD_VERSION));
        assert.ok(Number.isInteger(specs.ATTEST_BATCH_CONTINUATION_VERSION));
        assert.strictEqual(specs.ATTEST_BATCH_COMPLETION_STAMP, ' (stamped on batch completion)');
    });

    registerAttestSeamTests();
});
