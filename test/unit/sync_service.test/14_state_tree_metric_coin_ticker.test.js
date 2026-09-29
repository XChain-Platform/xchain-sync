/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 * SyncService.startStateTreeMetric: retained roots read by the TICKER
 *
 * The metric loop iterates the hub-discovered configs, whose `coin` is the
 * full lowercase name ('bitcoin'), while every state_tree_roots writer binds
 * the ticker ('BTC'). Forwarding the full name matched no root, so the walk
 * reached nothing and every node was reported orphaned.
 ********************************************************************/

'use strict';

const assert = require('assert');
const sinon  = require('sinon');
const SyncService     = require('../../../src/sync_service');
const stateCommitment = require('../../../src/state_commitment');

const ROOT = 'ab'.repeat(32);

// One node and one retained root; the root row matches only when bound to the ticker.
function tickerOnlyPool(rootArgs){
    const query = async (sql, args) => {
        if(/COUNT\(\*\) AS c FROM state_tree_nodes/.test(sql)) return [{ c: 1 }];
        if(/FROM state_tree_roots/.test(sql)){ rootArgs.push(args[0]); return args[0] === 'BTC' ? [{ r: ROOT }] : []; }
        if(/FROM state_tree_nodes WHERE node_hash IN/.test(sql))
            return args.filter(h => h === ROOT).map(h => ({ node_hash: h, left_hash: null, right_hash: null }));
        return [];
    };
    return { getConnection: async () => ({ query, release: async () => {} }) };
}

describe('SyncService state-tree orphan metric passes the coin ticker @regression', function(){
    let clock, savedInterval;

    beforeEach(function(){
        savedInterval = process.env.STATE_TREE_METRIC_INTERVAL_MS;
        process.env.STATE_TREE_METRIC_INTERVAL_MS = '1000';
        clock = sinon.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    });
    afterEach(function(){
        clock.restore();
        sinon.restore();
        if(savedInterval === undefined) delete process.env.STATE_TREE_METRIC_INTERVAL_MS;
        else process.env.STATE_TREE_METRIC_INTERVAL_MS = savedInterval;
    });

    it('reads the ticker-keyed roots, so a fully reachable store reports zero orphans', async function(){
        const rootArgs = [];
        const svc = new SyncService({});
        svc.databases.set('bitcoin:mainnet:indexer', {
            db: { pool: tickerOnlyPool(rootArgs) }, config: { coin: 'bitcoin', network: 'mainnet' }, dbType: 'indexer'
        });
        const spy = sinon.spy(stateCommitment, 'reportOrphanStats');

        svc.startStateTreeMetric();
        await clock.tickAsync(1000);
        clearInterval(svc._stateTreeMetricTimer);

        assert.deepStrictEqual(rootArgs, ['BTC'], 'state_tree_roots must be read with the ticker');
        assert.strictEqual(spy.callCount, 1, 'the metric pass did not run');
        const stats = await spy.returnValues[0];
        assert.strictEqual(stats.orphanCount, 0);
        assert.strictEqual(stats.reachableNodes, 1);
    });
});
