/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 **********************************************************************
 * test/unit/db.replicaStatus.test.js
 *
 * getReplicaStatus on a MULTI-SOURCE replica, the shape the serving tier runs.
 *
 * `SHOW REPLICA STATUS` reports only the UNNAMED connection, so a replica whose
 * connections are all named answers it with zero rows and is indistinguishable
 * from a primary, which certifies an unmeasured server as fresh. These tests pin
 * the multi-source reading, the worst-case reduction across connections, the
 * named-connection path, both fail-closed cases and the fallback chain.
 *
 * The ServerPoller tests stub getReplicaStatus out entirely, so this file is the
 * half that can see the statement selection at all.
 */

'use strict';

const assert   = require('assert');
const sinon    = require('sinon');
const Database = require('../../src/db');

function makeUtil() {
    return {
        isNull:     (v) => v === null || v === undefined,
        throwError: (m) => { throw new Error(m); },
        sleep:      sinon.stub().resolves(),
        logError:   sinon.stub()
    };
}

// A Database whose doQueryStrict answers per statement. `answers` maps a
// statement to rows, or to an Error to simulate a server that lacks it.
function dbWith(answers) {
    const db = new Database('localhost', 3306, 'replica_db', 'u', 'p', makeUtil(), 'indexer');
    db._asked = [];
    sinon.stub(db, 'doQueryStrict').callsFake((sql) => {
        db._asked.push(sql);
        const a = answers[sql];
        if (a === undefined) return Promise.reject(new Error('ER_PARSE_ERROR: unknown statement ' + sql));
        if (a instanceof Error) return Promise.reject(a);
        return Promise.resolve(a);
    });
    return db;
}

const conn = (name, io, sql, behind) => ({
    Connection_name: name, Slave_IO_Running: io, Slave_SQL_Running: sql, Seconds_Behind_Master: behind
});

afterEach(function () { sinon.restore(); delete process.env.SYNC_REPLICA_CONNECTION; });

describe('getReplicaStatus on a multi-source replica @regression @tier1', function () {

    it('SEES a two-connection replica that SHOW REPLICA STATUS reports as empty', async function () {
        // The regression itself. Before the fix this returned isReplica:false and
        // the caller published replica_stale:false on an unmeasured box.
        const db = dbWith({
            'SHOW ALL SLAVES STATUS': [conn('coindaddy', 'Yes', 'Yes', 0), conn('xchain', 'Yes', 'Yes', 3)],
            'SHOW REPLICA STATUS':    [],
            'SHOW SLAVE STATUS':      []
        });
        const r = await db.getReplicaStatus();
        assert.strictEqual(r.isReplica, true, 'a multi-source replica must not read as a primary');
        assert.strictEqual(r.running, true);
        assert.strictEqual(r.secondsBehind, 3);
        assert.strictEqual(db._asked[0], 'SHOW ALL SLAVES STATUS', 'must ask the multi-source statement FIRST');
    });

    it('reduces lag worst-case across connections', async function () {
        const db = dbWith({ 'SHOW ALL SLAVES STATUS': [
            conn('coindaddy', 'Yes', 'Yes', 41), conn('xchain', 'Yes', 'Yes', 7)
        ] });
        const r = await db.getReplicaStatus();
        assert.strictEqual(r.secondsBehind, 41, 'served data is only as fresh as the laggiest stream');
    });

    it('one stopped SQL thread makes the whole box not-running', async function () {
        const db = dbWith({ 'SHOW ALL SLAVES STATUS': [
            conn('coindaddy', 'Yes', 'Yes', 0), conn('xchain', 'Yes', 'No', 0)
        ] });
        const r = await db.getReplicaStatus();
        assert.strictEqual(r.isReplica, true);
        assert.strictEqual(r.running, false, 'a halted stream cannot certify freshness');
    });

    it('a NULL Seconds_Behind on any connection is unbounded lag, never zero', async function () {
        const db = dbWith({ 'SHOW ALL SLAVES STATUS': [
            conn('coindaddy', 'Yes', 'Yes', 0), conn('xchain', 'Yes', 'Yes', null)
        ] });
        const r = await db.getReplicaStatus();
        assert.strictEqual(r.secondsBehind, null);
    });

    it('zero connections is a genuine primary and stays fresh', async function () {
        // A server fed by the sync protocol rather than native replication
        // legitimately has no connections at all.
        const db = dbWith({ 'SHOW ALL SLAVES STATUS': [] });
        const r = await db.getReplicaStatus();
        assert.strictEqual(r.isReplica, false);
        assert.strictEqual(r.secondsBehind, null);
    });

    it('SYNC_REPLICA_CONNECTION measures only the named stream', async function () {
        process.env.SYNC_REPLICA_CONNECTION = 'xchain';
        const db = dbWith({ 'SHOW ALL SLAVES STATUS': [
            conn('coindaddy', 'Yes', 'Yes', 999), conn('xchain', 'Yes', 'Yes', 4)
        ] });
        const r = await db.getReplicaStatus();
        assert.strictEqual(r.secondsBehind, 4, 'an unrelated lagging stream must not drag the reading');
        assert.strictEqual(r.running, true);
    });

    it('a named-but-absent connection FAILS CLOSED rather than measuring another', async function () {
        process.env.SYNC_REPLICA_CONNECTION = 'xchain';
        const db = dbWith({ 'SHOW ALL SLAVES STATUS': [conn('coindaddy', 'Yes', 'Yes', 0)] });
        const r = await db.getReplicaStatus();
        assert.strictEqual(r.isReplica, null, 'an assertion that no longer matches the server is unknown, not fresh');
        assert.strictEqual(r.secondsBehind, null);
    });

    it('falls back to SHOW REPLICA STATUS on a server without ALL SLAVES', async function () {
        const db = dbWith({
            'SHOW ALL SLAVES STATUS': new Error('ER_PARSE_ERROR'),
            'SHOW REPLICA STATUS':    [{ Replica_IO_Running: 'Yes', Replica_SQL_Running: 'Yes', Seconds_Behind_Source: 11 }]
        });
        const r = await db.getReplicaStatus();
        assert.strictEqual(r.isReplica, true);
        assert.strictEqual(r.secondsBehind, 11);
    });

    it('an unreadable status is UNKNOWN, and the warning names SLAVE MONITOR', async function () {
        const denied = new Error('Access denied; you need (at least one of) the SUPER, SLAVE MONITOR privilege(s)');
        const db = dbWith({
            'SHOW ALL SLAVES STATUS': denied,
            'SHOW REPLICA STATUS':    denied,
            'SHOW SLAVE STATUS':      denied
        });
        const r = await db.getReplicaStatus();
        assert.strictEqual(r.isReplica, null);
        assert.strictEqual(r.running, null);
        assert.strictEqual(r.secondsBehind, null);
        const msg = db.util.logError.getCall(0).args[0];
        assert.ok(/SLAVE MONITOR/.test(msg),
            'MariaDB REPLICATION CLIENT aliases BINLOG MONITOR and does not cover slave status; ' +
            'the message must name the privilege that actually works');
    });
});
