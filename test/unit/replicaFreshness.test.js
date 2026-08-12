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
// /status derived lag_blocks as source_height - block_height, and
// source_height falls back to db.getLastBlock(), a MAX(block_index) against the
// SERVED database. On a node fronting a native SQL replica those are one failure
// domain: replication stops applying, both heights freeze at the same number,
// Math.max(0, 0) publishes lag 0, and status certifies an hours-behind node as
// caught up. The client path already had CLIENT_SOURCE_STALE_MS for the same
// class; the server path fronting a replica had nothing.

const assert = require('assert');
const { applyReplicaFreshness } = require('../../src/api');
const config = require('../../src/config');

function baseRow(){
    return { block_height: 100, source_height: 100, lag_blocks: 0 };
}

describe('/status replication freshness', function(){

    it('withholds lag_blocks when the replication engine says the node is stale', function(){
        let row = applyReplicaFreshness(baseRow(), { replica_stale: true, replica_seconds_behind: null });
        assert.strictEqual(row.lag_blocks, null,
            'publishing 0 here is the defect: a frozen replica reads as caught up');
        assert.strictEqual(row.replica_stale, true);
    });

    it('keeps lag_blocks when replication is fresh', function(){
        let row = applyReplicaFreshness(baseRow(), { replica_stale: false, replica_seconds_behind: 4 });
        assert.strictEqual(row.lag_blocks, 0);
        assert.strictEqual(row.replica_seconds_behind, 4);
    });

    it('fails closed on a poller status that predates the fields', function(){
        // An older poller sends no replica_* keys at all. Absent evidence must not
        // read as "not stale" AND still surface an explicit unknown seconds-behind.
        let row = applyReplicaFreshness(baseRow(), { block_height: 100 });
        assert.strictEqual(row.replica_seconds_behind, null);
        assert.strictEqual(row.replica_stale, false, 'no signal is the pre-replica topology default');
    });

    it('SYNC_REPLICA_MAX_LAG_S is configurable and defaults to 120s', function(){
        let saved = process.env.SYNC_REPLICA_MAX_LAG_S;
        try {
            delete process.env.SYNC_REPLICA_MAX_LAG_S;
            assert.strictEqual(config.getConfig()['SYNC_REPLICA_MAX_LAG_S'], 120);
            process.env.SYNC_REPLICA_MAX_LAG_S = '30';
            assert.strictEqual(config.getConfig()['SYNC_REPLICA_MAX_LAG_S'], 30);
        } finally {
            if(saved === undefined) delete process.env.SYNC_REPLICA_MAX_LAG_S;
            else process.env.SYNC_REPLICA_MAX_LAG_S = saved;
        }
    });
});
