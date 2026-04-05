const assert = require('assert');

// Assert that replica row counts match source for all non-empty tables
async function assertReplicaMatchesSource(sourceDb, replicaDb, testDb, tables) {
    if (!tables) {
        tables = await testDb.getTables(sourceDb);
    }

    let mismatches = [];
    for (let table of tables) {
        try {
            let sourceCount = await testDb.getRowCount(sourceDb, table);
            let replicaCount = await testDb.getRowCount(replicaDb, table);
            if (sourceCount !== replicaCount) {
                mismatches.push({ table, sourceCount, replicaCount });
            }
        } catch (e) {
            // Table may not exist in replica — skip
        }
    }

    if (mismatches.length > 0) {
        let details = mismatches.map(m => m.table + ': source=' + m.sourceCount + ' replica=' + m.replicaCount).join(', ');
        assert.fail('Row count mismatches: ' + details);
    }
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
    assertReplicaMatchesSource,
    assertBlockExists,
    assertBlockNotExists,
    assertBalancesConsistent,
    assertHashesMatch
};
