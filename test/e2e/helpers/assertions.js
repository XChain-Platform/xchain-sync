// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const assert = require('assert');
const crypto = require('crypto');
const { getReplicatedTables } = require('../../../src/replicatedTables');
const BlockHasher = require('../../../src/BlockHasher');
const testDbModule = require('./testDb');

// Columns excluded from the byte comparison: local-machine artifacts that are
// NOT replicated data (each side writes its own value by design).
//   - sync_meta id/logged_at: AUTO_INCREMENT + local write timestamp.
//   - balances/contract_balances id: the follower REBUILDS these aggregates
//     (DELETE + re-INSERT), so its AUTO_INCREMENT ids legitimately differ
//     from the source's; the (address|contract, tick, amount) content is the
//     replicated contract. (A full-snapshot bootstrap copies source ids
//     verbatim, so id equality WOULD hold right after bootstrap — comparing
//     it would make the oracle pass or fail depending on which path
//     populated the table, which is exactly the kind of path-dependence the
//     oracle exists to reject.)
const COLUMN_EXCLUSIONS = {
    sync_meta: ['id', 'logged_at'],
    balances: ['id'],
    contract_balances: ['id'],
};

// Tables compared with SUBSET semantics (every replica row must exist
// byte-identical on the source, but the replica may hold FEWER rows):
//   - sync_meta: the source's TransparencyLog records a block's row shortly
//     AFTER the block itself becomes visible, so a catch-up snapshot built in
//     that window misses the newest rows and nothing re-delivers them (the
//     live stream only carries sync_meta with NEW blocks). Production treats
//     the shortfall as a count-based health signal, not an error
//     (ClientSync._verifyTableCounts). A row the replica has that the source
//     does NOT, or one that differs byte-wise, is still a hard failure.
//   - index_* dedup tables: the indexer's rollback NEVER deletes index rows
//     (they're in neither blockTables nor dataTables — append-only dedup), so
//     after a reorg the SOURCE retains residue rows created by orphaned
//     blocks. Block payloads deliver index rows BY REFERENCED ID
//     (ServerPoller._buildBlockPayload), so a replica that never received the
//     orphan blocks legitimately never receives their residue — and that is
//     safe: any later block that dedups onto a residue row re-delivers it by
//     reference (ClientApplier INSERT IGNOREs it). Content of every
//     REFERENCED row is still verified — the per-block recompute joins
//     through these tables, and the strict data tables pin the ids.
const SUBSET_TABLES = new Set([
    'sync_meta',
    'index_actions', 'index_addresses', 'index_coins', 'index_fiats',
    'index_memos', 'index_mime_types', 'index_pubkeys', 'index_statuses',
    'index_tickers', 'index_transactions',
]);

// Derived aggregates the follower REBUILDS rather than receives. They are not
// in the per-block replicated set, but a complete replica must still hold
// content-identical rows (the rebuild SQL is required to render amounts
// exactly the way the source indexer writes them — that contract broke in
// production twice: DOUBLE-promotion corruption and trailing-zero format
// drift).
const DERIVED_AGGREGATES = { indexer: ['balances', 'contract_balances'], decoder: [] };

// Canonicalize one row for comparison: stable key order, Buffers as hex,
// BigInt as string. DATETIME/TIMESTAMP already arrive as strings
// (dateStrings: true) and ints as numbers (bigIntAsNumber: true).
function canonicalRow(row, excludedColumns) {
    let out = {};
    for (let key of Object.keys(row).sort()) {
        if (excludedColumns && excludedColumns.includes(key)) continue;
        let v = row[key];
        if (Buffer.isBuffer(v)) v = '0x' + v.toString('hex');
        else if (typeof v === 'bigint') v = v.toString();
        out[key] = v;
    }
    return JSON.stringify(out);
}

// Order-insensitive content digest of a whole table (sorted canonical rows).
async function tableContent(db, table, excludedColumns) {
    let rows = await db.doQuery('SELECT * FROM `' + table + '`');
    let canon = rows.map(r => canonicalRow(r, excludedColumns)).sort();
    let h = crypto.createHash('sha256');
    for (let s of canon) h.update(s + '\n');
    return { digest: h.digest('hex'), canon };
}

/**
 * THE parity oracle: the replica must be row-for-row, byte-for-byte identical
 * to the source on every replicated table (plus the derived aggregates), and —
 * for indexer DBs — the consensus-hash triple recomputed from the REPLICA's
 * raw rows must equal the hashes the SOURCE committed for every block
 * (behavioral VERIFY_RECOMPUTE: data → hash conformance).
 *
 * This replaces a row-count-only check that silently skipped tables missing
 * from the replica. Production bugs that row counts cannot see and this can:
 * DOUBLE-promotion amount corruption, minimal-decimal format drift, a replica
 * missing a whole table (schema wedge), and any apply that lands the right
 * NUMBER of rows with the wrong content.
 *
 * opts:
 *   dbType            'indexer' (default) | 'decoder'
 *   tables            explicit table list (defaults to the replicated set + aggregates)
 *   verifyRecompute   false to skip the per-block hash recompute (default true; indexer only)
 *   maxDiffRows       sample size per direction in the failure report (default 3)
 */
async function assertReplicaByteIdentical(sourceDb, replicaDb, opts = {}) {
    let dbType = opts.dbType || 'indexer';
    let tables = opts.tables ||
        getReplicatedTables(dbType).concat(DERIVED_AGGREGATES[dbType] || []);
    let maxDiffRows = opts.maxDiffRows || 3;

    // A replicated table missing from the replica is a FAILURE (it was a
    // silent skip before — exactly how a schema wedge hides).
    let replicaTables = new Set(await testDbModule.getTables(replicaDb));
    let missing = tables.filter(t => !replicaTables.has(t));
    assert.deepStrictEqual(missing, [],
        'Replica is missing replicated tables: ' + missing.join(', '));

    let diffs = [];
    for (let table of tables) {
        let excluded = COLUMN_EXCLUSIONS[table];
        let src = await tableContent(sourceDb, table, excluded);
        let rep = await tableContent(replicaDb, table, excluded);
        if (src.digest === rep.digest) continue;
        let repSet = new Set(rep.canon);
        let srcSet = new Set(src.canon);
        let onlyInReplica = rep.canon.filter(r => !srcSet.has(r));
        // Subset tables: lagging rows are tolerated; foreign/divergent rows are not.
        if (SUBSET_TABLES.has(table) && onlyInReplica.length === 0) continue;
        diffs.push({
            table,
            sourceRows: src.canon.length,
            replicaRows: rep.canon.length,
            onlyInSource: src.canon.filter(r => !repSet.has(r)).slice(0, maxDiffRows),
            onlyInReplica: onlyInReplica.slice(0, maxDiffRows),
        });
    }
    if (diffs.length > 0) {
        assert.fail('Replica is NOT byte-identical to source in ' +
            diffs.map(d => d.table).join(', ') + ':\n' + JSON.stringify(diffs, null, 2));
    }

    // Behavioral VERIFY_RECOMPUTE: recompute every block's consensus hashes
    // from the replica's raw rows; they must equal what the source committed.
    if (dbType === 'indexer' && opts.verifyRecompute !== false) {
        let hasher = new BlockHasher(replicaDb, testDbModule.util);
        let blocks = await replicaDb.doQuery('SELECT block_index FROM blocks ORDER BY block_index ASC');
        for (let row of blocks) {
            let blockIndex = Number(row.block_index);
            let committed = await sourceDb.getBlockHashRow(blockIndex);
            assert.ok(committed, 'Source has no committed hash row for block ' + blockIndex);
            let computed = await hasher.computeBlockHashes(blockIndex);
            for (let field of ['ledger_hash', 'actions_hash', 'contract_hash']) {
                assert.strictEqual(computed[field], committed[field],
                    'Replica-recomputed ' + field + ' diverges from the source-committed hash at block ' +
                    blockIndex + ' (replica data does not reproduce the committed consensus hash)');
            }
        }
    }
}

// Back-compat wrapper: every existing call site (the chaos suites) now gets
// the full byte-identity + recompute oracle instead of a row-count check.
// The testDb parameter is retained for signature compatibility but unused.
async function assertReplicaMatchesSource(sourceDb, replicaDb, testDb, tables) {
    await assertReplicaByteIdentical(sourceDb, replicaDb, tables ? { tables } : {});
}

// Assert that a block exists in a database (blocks + transactions rows present)
async function assertBlockExists(db, block_index) {
    let blockRows = await db.getBlockScopedRows('blocks', block_index);
    assert.ok(blockRows.length > 0, 'Block ' + block_index + ' should exist in blocks table');

    let txRows = await db.getTransactions(block_index);
    assert.ok(txRows.length > 0, 'Block ' + block_index + ' should have transactions');
}

// Assert that a block does NOT exist in a database
async function assertBlockNotExists(db, block_index) {
    let blockRows = await db.getBlockScopedRows('blocks', block_index);
    assert.strictEqual(blockRows.length, 0, 'Block ' + block_index + ' should not exist in blocks table');

    let txRows = await db.getTransactions(block_index);
    assert.strictEqual(txRows.length, 0, 'Block ' + block_index + ' should have no transactions');
}

// Assert that balances table is consistent with credits/debits
async function assertBalancesConsistent(db) {
    // Compute expected balances from credits/debits
    let computed = await db.doQuery(`
        SELECT address_id, tick_id,
            CAST(COALESCE(SUM(CASE WHEN t.type = 'credit' THEN CAST(t.amount AS DECIMAL(65,0)) ELSE -CAST(t.amount AS DECIMAL(65,0)) END), 0) AS CHAR) as expected_amount
        FROM (
            SELECT address_id, tick_id, amount, 'credit' as type FROM credits
            UNION ALL
            SELECT address_id, tick_id, amount, 'debit' as type FROM debits
        ) t
        GROUP BY address_id, tick_id
        HAVING SUM(CASE WHEN t.type = 'credit' THEN CAST(t.amount AS DECIMAL(65,0)) ELSE -CAST(t.amount AS DECIMAL(65,0)) END) != 0
    `);

    let actual = await db.doQuery("SELECT address_id, tick_id, amount FROM balances ORDER BY address_id, tick_id");

    assert.strictEqual(actual.length, computed.length,
        'Balances count mismatch: actual=' + actual.length + ' expected=' + computed.length);

    // Build lookup map for comparison
    let expectedMap = {};
    for (let row of computed) {
        expectedMap[row.address_id + ':' + row.tick_id] = row.expected_amount;
    }

    for (let row of actual) {
        let key = row.address_id + ':' + row.tick_id;
        assert.ok(expectedMap[key] !== undefined, 'Unexpected balance entry: ' + key);
        assert.strictEqual(row.amount, expectedMap[key],
            'Balance mismatch for ' + key + ': actual=' + row.amount + ' expected=' + expectedMap[key]);
    }
}

// Assert block hashes match between source and replica
async function assertHashesMatch(sourceDb, replicaDb, block_index) {
    let sourceHash = await sourceDb.getBlockHashRow(block_index);
    let replicaHash = await replicaDb.getBlockHashRow(block_index);

    assert.ok(sourceHash, 'Source should have hash for block ' + block_index);
    assert.ok(replicaHash, 'Replica should have hash for block ' + block_index);

    assert.strictEqual(replicaHash.ledger_hash, sourceHash.ledger_hash,
        'Ledger hash mismatch at block ' + block_index);
    assert.strictEqual(replicaHash.actions_hash, sourceHash.actions_hash,
        'Actions hash mismatch at block ' + block_index);
    assert.strictEqual(replicaHash.contract_hash, sourceHash.contract_hash,
        'Contract hash mismatch at block ' + block_index);
}

module.exports = {
    assertReplicaByteIdentical,
    assertReplicaMatchesSource,
    assertBlockExists,
    assertBlockNotExists,
    assertBalancesConsistent,
    assertHashesMatch
};
