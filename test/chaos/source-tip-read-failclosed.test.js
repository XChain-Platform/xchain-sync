'use strict';

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
 * Chaos Engineering: source cursor read fails CLOSED (CE-SRC-01 lens)
 *
 * CE-SRC-01 asserts the sync server SURVIVES a source-DB outage and that the
 * outage is FELT (counted poll failures). The compose-backed half of that
 * experiment runs against the e2e harness database, whose doQuery throws on
 * every error, so it cannot see how production's src/db.js answers the same
 * fault: there, a non-transactional query error is logged and collapsed into
 * [], which ServerPoller._poll would read as "no blocks yet" and skip - an
 * unreachable source and an idle chain producing the identical, silent result.
 *
 * These cases drive _poll against a database stub that reproduces db.js's
 * fail-soft/fail-closed contract exactly, so the poller's own choice of read is
 * what is under test. No Toxiproxy, no MariaDB, no compose stack: it lives
 * beside the experiment whose claim it completes.
 */

const { expect } = require('chai');
const ServerPoller = require('../../src/ServerPoller');

// Mirrors src/db.js: outside a transaction doQuery swallows a query error and
// returns [] unless the caller passed { rethrow: true }; getLastBlock turns an
// empty result into null. `queryFails` is the source-DB outage.
function makeSourceDb(opts){
    let settings = opts || {};
    return {
        dbType: 'indexer',
        queries: [],
        async doQuery(query, args, conn, queryOpts){
            this.queries.push({ query, opts: queryOpts });
            if(settings.queryFails){
                if(queryOpts && queryOpts.rethrow)
                    throw new Error('connect ECONNREFUSED 127.0.0.1:3306');
                return [];
            }
            return settings.rows || [];
        },
        async getLastBlock(conn, queryOpts){
            let rows = await this.doQuery('SELECT MAX(block_index) AS block_index FROM blocks', null, conn, queryOpts);
            if(rows.length > 0 && rows[0].block_index !== null) return Number(rows[0].block_index);
            return null;
        }
    };
}

function makePoller(db){
    let broadcaster = {
        broadcastCalls: 0,
        broadcast(){ this.broadcastCalls++; },
        updateStatus(){},
        getSubscriberCount(){ return 0; }
    };
    let util = {
        sleep: () => Promise.resolve(),
        logError: () => {},
        throwError: (m) => { throw new Error(m); }
    };
    let poller = new ServerPoller(
        'bitcoin', 'mainnet', db, broadcaster, null,
        { BLOCK_POLL_INTERVAL: 10 }, util
    );
    return { poller, broadcaster };
}

describe('Chaos: source cursor read fails closed', function () {

    it('a source-DB outage surfaces out of _poll instead of reading as an idle chain', async function () {
        let db = makeSourceDb({ queryFails: true });
        let { poller } = makePoller(db);
        poller.lastPolledBlock = 20;

        let error = null;
        try { await poller._poll(); } catch (e) { error = e; }

        expect(error, 'the outage must propagate so the poll loop can count it').to.be.an('Error');
        expect(error.message).to.contain('ECONNREFUSED');
        // The cursor is untouched: a failed poll must not move the broadcast
        // position, and the next cycle retries from the same height.
        expect(poller.lastPolledBlock).to.equal(20);
    });

    it('an outage on a poller with no cursor yet does not seed lastPolledBlock from a swallowed error', async function () {
        let db = makeSourceDb({ queryFails: true });
        let { poller } = makePoller(db);
        poller.lastPolledBlock = null;

        let error = null;
        try { await poller._poll(); } catch (e) { error = e; }

        expect(error, 'a first poll against a dead source must fail, not adopt a cursor').to.be.an('Error');
        expect(poller.lastPolledBlock).to.equal(null);
    });

    it('a genuinely empty source is still a quiet no-op, not an error', async function () {
        // rows: [] with no fault is what an indexer that has not written block 1 yet
        // returns. The fail-closed read must not turn that into a poll failure.
        let db = makeSourceDb({ queryFails: false, rows: [] });
        let { poller, broadcaster } = makePoller(db);
        poller.lastPolledBlock = null;

        await poller._poll();

        expect(poller.lastPolledBlock).to.equal(null);
        expect(broadcaster.broadcastCalls).to.equal(0);
    });
});
