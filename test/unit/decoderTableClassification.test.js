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
// Schema-exhaustiveness guard for the DECODER table universe.
//
// The other two table universes on this seam are already structurally proven
// against their schema directory: the indexer topology is GENERATED from
// src/tableLifecycle.js and rollback-coverage.test.js reads
// xchain-indexer/src/sql, and the sync-owned set is checked by
// syncTableClassification.test.js against xchain-sync/src/sql. The decoder
// topology is a hand-written literal (replicatedTables.js TOPOLOGY.decoder)
// and nothing read the decoder schema directory to prove it exhaustive.
//
// Every decoder channel trusts that literal: the per-block stream
// (ServerPoller), the /status completeness count (getReplicatedTables), reorg
// rollback (ClientRollback decoderBlockTables / decoderTxScopedTables) and the
// content-parity plan. So a table added by a decoder migration and never added
// to the literal is never streamed, never counted, never rolled back and never
// parity-checked, with nothing turning red. That is the merkle_reorgs failure
// class the sync-owned guard was written to close (see the header of
// syncTableClassification.test.js).
//
// The by-value pin in replicatedTables.test.js is NOT this guard: it compares
// the literal against another literal, so it makes an EDIT to the literal
// deliberate while staying blind to a schema file nobody ever declared.
//
// Scope note: this enumerates the base `<table>.sql` files, the same unit the
// decoder repo creates its tables from. A table introduced only inside
// src/sql/migrations/ would still escape; none exist today.

'use strict';

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const { getReplicatedTables }  = require('../../src/replicatedTables');
const { OPERATOR_LOCAL_TABLES } = require('../../src/SnapshotBuilder');

// Resolved exactly as the other two decoder-schema readers resolve it
// (generatedColumns.test.js, replicatedDatetimeColumns.test.js), so this guard
// runs in the same CI job and against the same directory they do.
const DECODER_SQL_DIR = process.env.XCHAIN_DECODER_SQL_PATH
    || path.resolve(__dirname, '..', '..', '..', 'xchain-decoder', 'src', 'sql');

// A drift guard must never pass by skipping. Absent sibling + XCHAIN_REQUIRE_SIBLINGS=1
// (the job that checks the sibling out) is a hard failure; a standalone checkout skips.
const SIBLING_REQUIRED = process.env.XCHAIN_REQUIRE_SIBLINGS === '1';
function requireSibling(ctx){
    if(fs.existsSync(DECODER_SQL_DIR)) return true;
    if(SIBLING_REQUIRED)
        throw new Error('decoder schema drift guard cannot run: sibling missing at ' +
            DECODER_SQL_DIR + ' (check out xchain-decoder or set XCHAIN_DECODER_SQL_PATH)');
    ctx.skip();
    return false;
}

// Decoder tables deliberately NOT replicated. Each entry is a decision, not an
// oversight, and the same decision is repeated by hand in SnapshotBuilder
// (OPERATOR_LOCAL_TABLES, and the function-local decoderSkip inside
// streamIncrementalSnapshot), which is why the last case below cross-checks it.
//
// mempool_transactions: node-local, non-deterministic observation state; its own
// schema comment forbids sharing raw values across nodes.
const DECODER_EXCLUDED = new Set(['mempool_transactions']);

const ADD_INSTRUCTIONS =
    '\n\nEither add it to TOPOLOGY.decoder in src/replicatedTables.js (and update the ' +
    'by-value pin in test/unit/replicatedTables.test.js), or add it to DECODER_EXCLUDED ' +
    'here with a written reason. Either way check SnapshotBuilder OPERATOR_LOCAL_TABLES / ' +
    'decoderSkip and ClientRollback decoderBlockTables / decoderTxScopedTables.';

function decoderSqlTables(){
    return fs.readdirSync(DECODER_SQL_DIR)
        .filter(f => f.endsWith('.sql'))
        .map(f => path.basename(f, '.sql'))
        .sort();
}

describe('decoder table classification (schema exhaustiveness) @regression', function(){

    it('classifies every xchain-decoder/src/sql table as replicated or excluded', function(){
        if(!requireSibling(this)) return;
        const sqlTables = decoderSqlTables();

        // Without this a broken readdir (wrong path, renamed extension) would make
        // the loop below iterate nothing and pass vacuously.
        assert.ok(sqlTables.length >= 8,
            'decoder schema enumeration looks broken at ' + DECODER_SQL_DIR +
            ': found ' + sqlTables.join(',') );

        const declared = new Set(getReplicatedTables('decoder'));
        const unclassified = sqlTables.filter(t => !declared.has(t) && !DECODER_EXCLUDED.has(t));

        assert.deepStrictEqual(unclassified, [],
            unclassified.length
                ? 'decoder table(s) in ' + DECODER_SQL_DIR + ' classified NOWHERE: ' +
                  unclassified.join(', ') + '. They are never streamed, never counted for ' +
                  '/status completeness, never rolled back on reorg and never parity-checked.' +
                  ADD_INSTRUCTIONS
                : undefined);
    });

    it('declares no decoder table that the schema no longer defines', function(){
        if(!requireSibling(this)) return;
        const sqlTables = new Set(decoderSqlTables());
        const orphans = getReplicatedTables('decoder').filter(t => !sqlTables.has(t)).sort();

        assert.deepStrictEqual(orphans, [],
            orphans.length
                ? 'TOPOLOGY.decoder names table(s) with no ' + DECODER_SQL_DIR +
                  '/<table>.sql: ' + orphans.join(', ') + '. A rename or removal on the ' +
                  'decoder side leaves the stream and the /status count asking for a table ' +
                  'no replica has.'
                : undefined);
    });

    it('keeps every excluded decoder table clear-protected on full-snapshot apply', function(){
        // No sibling needed: both sets are local. An excluded table that is NOT in
        // OPERATOR_LOCAL_TABLES rides the full snapshot (ClientApplier's clear loop
        // deletes every BASE table outside that set), which is the leak the
        // mempool_transactions entry was added to close.
        for(const table of DECODER_EXCLUDED)
            assert.ok(OPERATOR_LOCAL_TABLES.has(table),
                table + ' is excluded from decoder replication but is not in ' +
                'SnapshotBuilder.OPERATOR_LOCAL_TABLES, so the source\'s rows would ' +
                'still ride a full snapshot onto every bootstrapping replica');
    });

    it('never lists a decoder table as both replicated and excluded', function(){
        const declared = new Set(getReplicatedTables('decoder'));
        for(const table of DECODER_EXCLUDED)
            assert.ok(!declared.has(table),
                table + ' is in both TOPOLOGY.decoder and DECODER_EXCLUDED - pick one');
    });
});
