/*********************************************************************
 * Documents the known attest batch-head state-hash gap.
 * Expected to be rewritten when the flag-day class is added.
 ********************************************************************/
'use strict';

const assert = require('assert');
const { buildStateHashData } = require('../../src/consensus/state_hash');

describe('state_hash attest batch-head stamp gap @regression', function(){

    it('covers only the legacy v0 attest request-status class', async function(){
        const queries = [];
        const db = {
            getStatusId: async () => 1,
            doQuery: async (sql, params) => {
                queries.push(sql);
                return [];
            }
        };

        const preimage = await buildStateHashData(db, 1000000, {
            activationDelay: 10,
            network: 'regtest',
            coin: 'BTC',
            gasTick: 'GAS'
        });
        const attestQueries = queries.filter(sql => sql.includes('attests'));
        const batchPredicates = /version\s*=\s*5|version\s+IN|batch_chunk_index|request_id/i;

        assert.ok(attestQueries.length > 0, 'the v0 attest request-status class still runs');
        assert.ok(attestQueries.every(sql => sql.includes('version = 0')),
            'every attest query remains restricted to version 0');
        assert.ok(!attestQueries.some(sql => batchPredicates.test(sql)),
            'no attest query covers a batch-head predicate');
        assert.ok(!Object.keys(preimage).some(key => /batch/i.test(key)),
            'the top-level preimage has no batch class');
        assert.ok(!Object.keys(preimage.request_status).some(key => /batch/i.test(key)),
            'the request-status preimage has no batch class');
    });
});
