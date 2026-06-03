const crypto = require('crypto');

function blockHash(blockIndex, label) {
    return crypto.createHash('sha256').update(label + ':' + blockIndex).digest('hex');
}

// Get-or-create an index_actions row, mirroring the real indexer's db.createAction.
// index_actions carries only a NON-unique index on `action`, so a blind
// `INSERT IGNORE` does NOT dedup — it inserts one duplicate row per call. The real
// indexer SELECTs first and inserts only when absent, so a given action type is a
// single row referenced by actions.action_id. Without this, the fixtures pile up an
// unreferenced duplicate index_actions row per block (nothing points at their id);
// those rows can only sync via snapshot, so a live-synced replica legitimately ends
// up with fewer index_actions than the source and the integrity check fails.
async function ensureIndexAction(db, action) {
    let rows = await db.doQuery("SELECT id FROM index_actions WHERE action = ? LIMIT 1", [action]);
    if (rows.length === 0)
        await db.doQuery("INSERT INTO index_actions (action) VALUES (?)", [action]);
}

function buildBlock(blockIndex, opts = {}) {
    let txIndex     = opts.txIndex || blockIndex * 10;
    let actionIndex = opts.actionIndex || blockIndex * 100;
    let blockTime   = opts.blockTime || 1700000000 + blockIndex;
    let sourceAddr  = opts.sourceAddr || 'addr_' + blockIndex;
    let tickName    = opts.tickName || 'TOKEN';
    let creditAmt   = opts.creditAmount || '1000';
    let debitAmt    = opts.debitAmount || '0';

    let ledgerHash   = blockHash(blockIndex, 'ledger');
    let actionsHash  = blockHash(blockIndex, 'actions');
    let contractHash = blockHash(blockIndex, 'contract');
    let txHash       = blockHash(blockIndex, 'tx');

    return {
        index_addresses: [{ address: sourceAddr }],
        index_transactions: [
            { hash: ledgerHash },
            { hash: actionsHash },
            { hash: contractHash },
            { hash: txHash }
        ],
        index_tickers: [{ tick: tickName }],
        index_actions: [{ action: 'SEND' }],
        blocks: [{
            block_index: blockIndex,
            block_time: blockTime,
            ledger_hash_id: null,
            actions_hash_id: null,
            contract_hash_id: null
        }],
        transactions: [{
            tx_index: txIndex,
            block_index: blockIndex,
            tx_hash_id: null,
            source_id: null
        }],
        actions: [{
            action_index: actionIndex,
            block_index: blockIndex,
            tx_index: txIndex,
            tx_vout: 0,
            action_id: 1,
            action_format: 0
        }],
        credits: [{
            action_index: actionIndex,
            address_id: null,
            tick_id: null,
            amount: creditAmt
        }],
        debits: debitAmt !== '0' ? [{
            action_index: actionIndex,
            address_id: null,
            tick_id: null,
            amount: debitAmt
        }] : [],
        _meta: {
            blockIndex, txIndex, actionIndex, blockTime,
            sourceAddr, tickName,
            ledgerHash, actionsHash, contractHash, txHash
        }
    };
}

async function seedBlocks(db, startBlock, endBlock, opts = {}) {
    let blocks = [];
    for (let i = startBlock; i <= endBlock; i++) {
        blocks.push(buildBlock(i, {
            txIndex: i * 10,
            actionIndex: i * 100,
            sourceAddr: opts.sourceAddr || 'addr_' + i,
            tickName: opts.tickName || 'TOKEN',
            creditAmount: opts.creditAmount || '1000',
            debitAmount: opts.debitAmount || '0'
        }));
    }

    for (let block of blocks) {
        let meta = block._meta;

        // Idempotent seed: skip a block that already exists. The chaos suites
        // reuse one long-lived source DB across describe blocks and re-seed
        // overlapping ranges; a plain re-INSERT of blocks/transactions would
        // throw a duplicate-key and abort the test. Reorg tests call
        // deleteBlocksFrom() first, so a genuinely-rolled-back block is absent
        // here and is re-inserted with fresh content as intended.
        let existing = await db.doQuery("SELECT 1 FROM blocks WHERE block_index = ? LIMIT 1", [meta.blockIndex]);
        if (existing.length > 0) continue;

        for (let addr of block.index_addresses)
            await db.doQuery("INSERT IGNORE INTO index_addresses (address) VALUES (?)", [addr.address]);
        for (let tx of block.index_transactions)
            await db.doQuery("INSERT IGNORE INTO index_transactions (hash) VALUES (?)", [tx.hash]);
        for (let tick of block.index_tickers)
            await db.doQuery("INSERT IGNORE INTO index_tickers (tick) VALUES (?)", [tick.tick]);
        for (let act of block.index_actions)
            await ensureIndexAction(db, act.action);

        let addrRows = await db.doQuery("SELECT id FROM index_addresses WHERE address = ?", [meta.sourceAddr]);
        let addrId = addrRows[0].id;

        let ledgerRows = await db.doQuery("SELECT id FROM index_transactions WHERE hash = ?", [meta.ledgerHash]);
        let ledgerHashId = ledgerRows[0].id;

        let actionsRows = await db.doQuery("SELECT id FROM index_transactions WHERE hash = ?", [meta.actionsHash]);
        let actionsHashId = actionsRows[0].id;

        let contractRows = await db.doQuery("SELECT id FROM index_transactions WHERE hash = ?", [meta.contractHash]);
        let contractHashId = contractRows[0].id;

        let txHashRows = await db.doQuery("SELECT id FROM index_transactions WHERE hash = ?", [meta.txHash]);
        let txHashId = txHashRows[0].id;

        let tickRows = await db.doQuery("SELECT id FROM index_tickers WHERE tick = ?", [meta.tickName]);
        let tickId = tickRows[0].id;

        await db.doQuery(
            "INSERT INTO blocks (block_index, block_time, ledger_hash_id, actions_hash_id, contract_hash_id) VALUES (?, ?, ?, ?, ?)",
            [meta.blockIndex, meta.blockTime, ledgerHashId, actionsHashId, contractHashId]
        );

        await db.doQuery(
            "INSERT INTO transactions (tx_index, block_index, tx_hash_id, source_id) VALUES (?, ?, ?, ?)",
            [meta.txIndex, meta.blockIndex, txHashId, addrId]
        );

        await db.doQuery(
            "INSERT INTO actions (action_index, block_index, tx_index, tx_vout, action_id, action_format) VALUES (?, ?, ?, ?, ?, ?)",
            [meta.actionIndex, meta.blockIndex, meta.txIndex, 0, 1, 0]
        );

        if (block.credits.length > 0) {
            await db.doQuery(
                "INSERT INTO credits (action_index, address_id, tick_id, amount) VALUES (?, ?, ?, ?)",
                [meta.actionIndex, addrId, tickId, block.credits[0].amount]
            );
        }

        if (block.debits.length > 0) {
            await db.doQuery(
                "INSERT INTO debits (action_index, address_id, tick_id, amount) VALUES (?, ?, ?, ?)",
                [meta.actionIndex, addrId, tickId, block.debits[0].amount]
            );
        }
    }

    // Rebuild balances
    await db.doQuery("DELETE FROM balances");
    await db.doQuery(`INSERT INTO balances (address_id, tick_id, amount)
        SELECT address_id, tick_id,
            CAST(COALESCE(SUM(CASE WHEN t.type = 'credit' THEN t.amount ELSE -t.amount END), 0) AS CHAR)
        FROM (
            SELECT address_id, tick_id, amount, 'credit' as type FROM credits
            UNION ALL
            SELECT address_id, tick_id, amount, 'debit' as type FROM debits
        ) t
        GROUP BY address_id, tick_id
        HAVING SUM(CASE WHEN t.type = 'credit' THEN CAST(t.amount AS DECIMAL(65,0)) ELSE -CAST(t.amount AS DECIMAL(65,0)) END) != 0`);

    return blocks;
}

async function deleteBlocksFrom(db, blockIndex) {
    let actionRows = await db.doQuery(
        "SELECT action_index FROM actions WHERE block_index >= ? ORDER BY action_index ASC LIMIT 1",
        [blockIndex]
    );
    let firstActionIndex = actionRows.length > 0 ? actionRows[0].action_index : null;

    if (firstActionIndex !== null) {
        let actionTables = ['credits', 'debits', 'sends', 'issues', 'tokens', 'destroys', 'actions'];
        for (let table of actionTables) {
            try {
                await db.doQuery("DELETE FROM `" + table + "` WHERE action_index >= ?", [firstActionIndex]);
            } catch (e) {}
        }
    }

    await db.doQuery("DELETE FROM transactions WHERE block_index >= ?", [blockIndex]);
    await db.doQuery("DELETE FROM blocks WHERE block_index >= ?", [blockIndex]);
    try {
        await db.doQuery("DELETE FROM sync_meta WHERE block_index >= ?", [blockIndex]);
    } catch (e) {}

    await db.doQuery("DELETE FROM balances");
    try {
        await db.doQuery(`INSERT INTO balances (address_id, tick_id, amount)
            SELECT address_id, tick_id,
                CAST(COALESCE(SUM(CASE WHEN t.type = 'credit' THEN t.amount ELSE -t.amount END), 0) AS CHAR)
            FROM (
                SELECT address_id, tick_id, amount, 'credit' as type FROM credits
                UNION ALL
                SELECT address_id, tick_id, amount, 'debit' as type FROM debits
            ) t
            GROUP BY address_id, tick_id
            HAVING SUM(CASE WHEN t.type = 'credit' THEN CAST(t.amount AS DECIMAL(65,0)) ELSE -CAST(t.amount AS DECIMAL(65,0)) END) != 0`);
    } catch (e) {}
}

// Seed a large block with many actions for volume testing
async function seedLargeBlock(db, blockIndex, actionCount) {
    let blockTime  = 1700000000 + blockIndex;
    let sourceAddr = 'addr_large_' + blockIndex;
    let tickName   = 'TOKEN';

    let ledgerHash   = blockHash(blockIndex, 'ledger');
    let actionsHash  = blockHash(blockIndex, 'actions');
    let contractHash = blockHash(blockIndex, 'contract');
    let txHash       = blockHash(blockIndex, 'tx');

    await db.doQuery("INSERT IGNORE INTO index_addresses (address) VALUES (?)", [sourceAddr]);
    await db.doQuery("INSERT IGNORE INTO index_transactions (hash) VALUES (?)", [ledgerHash]);
    await db.doQuery("INSERT IGNORE INTO index_transactions (hash) VALUES (?)", [actionsHash]);
    await db.doQuery("INSERT IGNORE INTO index_transactions (hash) VALUES (?)", [contractHash]);
    await db.doQuery("INSERT IGNORE INTO index_transactions (hash) VALUES (?)", [txHash]);
    await db.doQuery("INSERT IGNORE INTO index_tickers (tick) VALUES (?)", [tickName]);
    await ensureIndexAction(db, 'SEND');

    let addrRows = await db.doQuery("SELECT id FROM index_addresses WHERE address = ?", [sourceAddr]);
    let addrId = addrRows[0].id;
    let ledgerRows = await db.doQuery("SELECT id FROM index_transactions WHERE hash = ?", [ledgerHash]);
    let actionsRows = await db.doQuery("SELECT id FROM index_transactions WHERE hash = ?", [actionsHash]);
    let contractRows = await db.doQuery("SELECT id FROM index_transactions WHERE hash = ?", [contractHash]);
    let txHashRows = await db.doQuery("SELECT id FROM index_transactions WHERE hash = ?", [txHash]);
    let tickRows = await db.doQuery("SELECT id FROM index_tickers WHERE tick = ?", [tickName]);
    let tickId = tickRows[0].id;

    await db.doQuery(
        "INSERT INTO blocks (block_index, block_time, ledger_hash_id, actions_hash_id, contract_hash_id) VALUES (?, ?, ?, ?, ?)",
        [blockIndex, blockTime, ledgerRows[0].id, actionsRows[0].id, contractRows[0].id]
    );

    let txIndex = blockIndex * 10;
    await db.doQuery(
        "INSERT INTO transactions (tx_index, block_index, tx_hash_id, source_id) VALUES (?, ?, ?, ?)",
        [txIndex, blockIndex, txHashRows[0].id, addrId]
    );

    // Batch insert actions and credits
    let batchSize = 100;
    for (let i = 0; i < actionCount; i += batchSize) {
        let batch = Math.min(batchSize, actionCount - i);
        let actionValues = [];
        let actionArgs = [];
        let creditValues = [];
        let creditArgs = [];

        for (let j = 0; j < batch; j++) {
            let actionIndex = blockIndex * 100000 + i + j;
            actionValues.push('(?, ?, ?, ?, ?, ?)');
            actionArgs.push(actionIndex, blockIndex, txIndex, 0, 1, 0);
            creditValues.push('(?, ?, ?, ?)');
            creditArgs.push(actionIndex, addrId, tickId, '10');
        }

        await db.doQuery(
            "INSERT INTO actions (action_index, block_index, tx_index, tx_vout, action_id, action_format) VALUES " + actionValues.join(','),
            actionArgs
        );
        await db.doQuery(
            "INSERT INTO credits (action_index, address_id, tick_id, amount) VALUES " + creditValues.join(','),
            creditArgs
        );
    }

    // Rebuild balances
    await db.doQuery("DELETE FROM balances");
    await db.doQuery(`INSERT INTO balances (address_id, tick_id, amount)
        SELECT address_id, tick_id,
            CAST(COALESCE(SUM(CASE WHEN t.type = 'credit' THEN t.amount ELSE -t.amount END), 0) AS CHAR)
        FROM (
            SELECT address_id, tick_id, amount, 'credit' as type FROM credits
            UNION ALL
            SELECT address_id, tick_id, amount, 'debit' as type FROM debits
        ) t
        GROUP BY address_id, tick_id
        HAVING SUM(CASE WHEN t.type = 'credit' THEN CAST(t.amount AS DECIMAL(65,0)) ELSE -CAST(t.amount AS DECIMAL(65,0)) END) != 0`);
}

module.exports = {
    blockHash,
    buildBlock,
    seedBlocks,
    deleteBlocksFrom,
    seedLargeBlock
};
