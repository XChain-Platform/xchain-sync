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
// Drift guard for the `dateStrings: true` pool setting in src/db.js. Replicated
// DATETIME/TIMESTAMP columns round-trip source -> JSON wire -> replica and enter
// BlockHasher.contentDigest; only because the driver hands them back as MariaDB-format
// STRINGS do the re-insert (strict mode rejects ISO 'T...Z') and the parity digest
// (timezone-dependent Date serialization) stay byte-stable. The indexer side is NOT
// a no-op, contrary to the comment above the flag (indexer events.time /
// events.witness_time are DATETIME and `events` rides the snapshot channel, #5609).
// This test freezes the inventory of replicated DATETIME columns on BOTH dbTypes and
// pins the flag, so the next schema or pool-option edit fails here, not in production
// as a parity divergence.

'use strict';

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const lifecycle        = require('../../src/tableLifecycle');
const replicatedTables = require('../../src/replicatedTables');

const SQL_DIRS = {
    indexer: process.env.XCHAIN_INDEXER_SQL_PATH
        || path.resolve(__dirname, '..', '..', '..', 'xchain-indexer', 'src', 'sql'),
    decoder: process.env.XCHAIN_DECODER_SQL_PATH
        || path.resolve(__dirname, '..', '..', '..', 'xchain-decoder', 'src', 'sql')
};
const SIBLING_REQUIRED = process.env.XCHAIN_REQUIRE_SIBLINGS === '1';

// FROZEN: every wire-replicated DATETIME/TIMESTAMP/DATE column, per dbType. A new
// entry here means a new column that depends on dateStrings staying global; add it
// here deliberately (and keep the db.js comment naming the inventory current).
const EXPECTED = {
    indexer: { events: ['time', 'witness_time'] },
    decoder: { events: ['time'] }
};

// Tables xchain-sync actually carries over the wire for each dbType. The indexer set
// is the registry's stream:* classes plus the snapshot-channel tables (indexer `events`
// is replication: 'snapshot', full-dumped by SnapshotBuilder, and is NOT in the stream
// topology getReplicatedTables derives from). local / hub-mirror / follower-derived
// rows never ship (SnapshotBuilder OPERATOR_LOCAL_TABLES), so their timestamps are out.
function wireTables(dbType){
    if(dbType === 'decoder') return new Set(replicatedTables.getReplicatedTables('decoder'));
    return new Set(lifecycle.tablesWhere(t => t.owner === 'indexer'
        && (/^stream:/.test(t.replication) || t.replication === 'snapshot')));
}

// Every `<col> DATETIME|TIMESTAMP|DATE` declaration in a CREATE TABLE, keyed by table.
function deriveFromDdl(sqlDir){
    const found = {};
    for(const file of fs.readdirSync(sqlDir).filter(f => f.endsWith('.sql'))){
        const text = fs.readFileSync(path.join(sqlDir, file), 'utf8');
        // Strip comments first: the DDL discusses DATETIME round-trips in prose.
        const body = text.replace(/^\s*--.*$/gm, '').replace(/--[^\n]*/g, '');
        const table = (body.match(/CREATE TABLE(?:\s+IF NOT EXISTS)?\s+`?(\w+)`?/i) || [])[1];
        if(!table) continue;
        for(const m of body.matchAll(/^[ \t]*`?(\w+)`?\s+(?:DATETIME|TIMESTAMP|DATE)\b/gim)){
            (found[table] = found[table] || []).push(m[1]);
        }
    }
    for(const t of Object.keys(found)) found[t].sort();
    return found;
}

describe('replicated DATETIME columns depend on db.js dateStrings:true @regression', function(){

    it('db.js sets dateStrings: true on the shared pool (never scoped per dbType)', function(){
        const src = fs.readFileSync(path.resolve(__dirname, '..', '..', 'src', 'db.js'), 'utf8');
        assert.ok(/dateStrings:\s*true/.test(src), 'db.js must set dateStrings: true on connectionPoolParams');
        assert.ok(!/dateStrings:\s*(?:false|this\.dbType|\()/.test(src), 'dateStrings must stay an unconditional true');
    });

    for(const dbType of Object.keys(EXPECTED)){
        it(`${dbType}: the wire-replicated DATETIME/TIMESTAMP inventory matches the frozen list`, function(){
            const dir = SQL_DIRS[dbType];
            if(!fs.existsSync(dir)){
                if(SIBLING_REQUIRED) throw new Error('sibling DDL missing at ' + dir);
                this.skip(); return;
            }
            const all  = deriveFromDdl(dir);
            const wire = wireTables(dbType);
            const got  = {};
            for(const t of Object.keys(all).sort()) if(wire.has(t)) got[t] = all[t];
            const exp = {};
            for(const t of Object.keys(EXPECTED[dbType]).sort()) exp[t] = EXPECTED[dbType][t].slice().sort();
            assert.deepStrictEqual(got, exp,
                dbType + ': replicated DATETIME/TIMESTAMP columns drifted from the frozen inventory; ' +
                'every such column rides the wire as a MariaDB-format string ONLY because db.js dateStrings is global. ' +
                'Update EXPECTED and the db.js comment deliberately.');
        });
    }
});
