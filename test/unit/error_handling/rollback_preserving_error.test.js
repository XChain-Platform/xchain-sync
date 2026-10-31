'use strict';

const assert = require('assert');
const sinon = require('sinon');
const ClientApplier = require('../../../src/client/applier');
const Utility = require('../../../src/util');
const { rollbackPreservingError } = require('../../../src/client/rollback_preserving_error');

describe('rollbackPreservingError', function(){
    afterEach(function(){ sinon.restore(); });

    it('resolves after a rollback failure', async function(){
        const db = { rollbackTransaction: sinon.stub().rejects(new Error('connection lost')) };
        await assert.doesNotReject(() => rollbackPreservingError(db, new Error('apply failed'), 'Apply'));
        assert.strictEqual(db.rollbackTransaction.calledOnce, true);
    });

    it('keeps the original classified error on the block apply path', async function(){
        const original = Object.assign(new Error('duplicate row'), {
            errno: 1062,
            upsertTable: 'markets'
        });
        const db = {
            dbType: 'indexer',
            beginTransaction: sinon.stub().resolves(),
            rollbackTransaction: sinon.stub().rejects(new Error('connection lost'))
        };
        const applier = new ClientApplier(db, new Utility());
        sinon.stub(applier, 'insertBlockRows').rejects(original);
        await assert.rejects(
            () => applier.applyBlockTransaction({ block_index: 12, data: {} }),
            error => error === original && error.errno === 1062 && error.upsertTable === 'markets'
        );
        assert.strictEqual(db.rollbackTransaction.calledOnce, true);
    });
});
