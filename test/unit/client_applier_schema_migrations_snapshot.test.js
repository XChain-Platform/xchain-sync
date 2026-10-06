// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const ClientApplier = require('../../src/client/applier');
const Utility = require('../../src/util');
const { SCHEMA_VERSION } = require('../../src/schema/version');
const { withDbMixins } = require('../helpers/db_mixins.js');

let applier, db;

function touchedLedger(){
    let sqlTouched = db.doQuery.getCalls().map(c => c.args[0])
        .filter(q => typeof q === 'string' && /schema_migrations/.test(q));
    let helperTouched = [db.deleteAllRows, db.deleteRowsByKeyValues]
        .filter(s => s && s.getCalls)
        .flatMap(s => s.getCalls().filter(c => c.args[0] === 'schema_migrations'));
    return sqlTouched.concat(helperTouched);
}

describe('ClientApplier schema_migrations classification', function(){
    beforeEach(function(){
        db = withDbMixins({
            doQuery: sinon.stub().resolves([]),
            beginTransaction: sinon.stub().resolves(),
            commitTransaction: sinon.stub().resolves(),
            rollbackTransaction: sinon.stub().resolves()
        });
        sinon.spy(db, 'deleteAllRows');
        sinon.spy(db, 'deleteRowsByKeyValues');
        applier = new ClientApplier(db, new Utility());
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    it('full snapshot leaves the replica schema_migrations rows untouched', async function(){
        db.doQuery.withArgs(sinon.match(/information_schema\.tables/)).resolves([
            { table_name: 'blocks' },
            { table_name: 'schema_migrations' }
        ]);
        await applier.applyFullSnapshot({
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 10,
            tables: {
                blocks: [{ block_index: 1 }],
                schema_migrations: [{ version: '001_source_only', applied_at: 1 }]
            }
        });
        assert.deepStrictEqual(touchedLedger(), []);
        assert.strictEqual(db.commitTransaction.calledOnce, true);
        assert.strictEqual(db.rollbackTransaction.called, false);
    });

    it('full snapshot leaves the ledger alone when only the local enumeration lists it', async function(){
        db.doQuery.withArgs(sinon.match(/information_schema\.tables/)).resolves([
            { table_name: 'blocks' },
            { table_name: 'schema_migrations' }
        ]);
        await applier.applyFullSnapshot({
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 10,
            tables: { blocks: [{ block_index: 1 }] }
        });
        assert.deepStrictEqual(touchedLedger(), []);
    });

    it('incremental snapshot skips schema_migrations without a swallowed missing-column error', async function(){
        let ignore = sinon.spy(applier, 'ignoreSchemaGap');
        await applier.applyIncrementalSnapshot({
            schema_version: SCHEMA_VERSION.indexer,
            since_block: 5,
            tables: { schema_migrations: [{ version: '001_source_only', applied_at: 1 }] }
        });
        assert.deepStrictEqual(touchedLedger(), []);
        assert.strictEqual(ignore.called, false);
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });
});
