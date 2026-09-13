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
// /health is the route ModuleService points the sync container's Docker probe at,
// and its verdict was built from two server-side signals only: the database
// circuit state and ServerPoller.pollErrorCount. SyncService.getPoller returns
// null in client mode, so that count is a constant 0 on every replica, and a
// durable halt leaves the database perfectly healthy with its circuit closed. A
// client held on a halt after a reboot therefore answered 200 'healthy' while
// applying no blocks at all, for as long as nobody read /status by hand.

const assert = require('assert');
const { buildHealthEntry, healthEntryDegraded } = require('../../src/api');

function mockService(clientState){
    return {
        getPoller: () => null,                       // client mode: never a poller
        getClientSyncState: () => clientState
    };
}

const HEALTHY_DB = { circuitState: 'closed' };

describe('/health halt verdict', function(){

    it('degrades on a halted client and names the halt', function(){
        let entry = buildHealthEntry(
            mockService({ halted: true, haltInfo: { blockIndex: 812345, reason: 'state-hash-divergence' } }),
            'client', HEALTHY_DB, 'bitcoin', 'mainnet', 'indexer');

        assert.strictEqual(entry.halted, true);
        assert.strictEqual(entry.halt_reason, 'state-hash-divergence');
        assert.strictEqual(entry.halt_block, 812345);
        assert.strictEqual(healthEntryDegraded(entry), true,
            'a replica that has stopped replicating must not read healthy');
    });

    it('stays healthy for a running client on a healthy database', function(){
        // Negative control for the test above: the halt check must not turn every
        // client into a permanent 503, or the probe stops carrying any signal.
        let entry = buildHealthEntry(
            mockService({ halted: false, haltInfo: null }),
            'client', HEALTHY_DB, 'bitcoin', 'mainnet', 'indexer');

        assert.strictEqual(entry.halted, false);
        assert.strictEqual(entry.halt_reason, null);
        assert.strictEqual(healthEntryDegraded(entry), false);
    });

    it('still degrades on an open circuit, halt or no halt', function(){
        let entry = buildHealthEntry(
            mockService({ halted: false, haltInfo: null }),
            'client', { circuitState: 'open' }, 'bitcoin', 'mainnet', 'indexer');

        assert.strictEqual(healthEntryDegraded(entry), true, 'the pre-existing signal is untouched');
    });

    it('leaves the server-mode row shape alone (no halt fields, poller count preserved)', function(){
        let svc = {
            getPoller: () => ({ pollErrorCount: 3 }),
            getClientSyncState: () => { throw new Error('server mode must not consult client state'); }
        };

        let entry = buildHealthEntry(svc, 'server', HEALTHY_DB, 'bitcoin', 'mainnet', 'indexer');

        assert.strictEqual(entry.poll_error_count, 3);
        assert.strictEqual('halted' in entry, false, 'a server publishes no client halt fields');
        assert.strictEqual(healthEntryDegraded(entry), true, 'a failing poll streak still degrades');
    });
});
