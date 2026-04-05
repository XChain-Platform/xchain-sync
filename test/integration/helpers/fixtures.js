const crypto = require('crypto');

// Generate a deterministic hash for a given block index and label
function blockHash(blockIndex, label) {
    return crypto.createHash('sha256').update(label + ':' + blockIndex).digest('hex');
}

// Build fixture rows for a single block
// Returns an object of table -> [rows] ready for insertion
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
        // Index tables (INSERT IGNORE safe)
        index_addresses: [
            { address: sourceAddr }
        ],
        index_transactions: [
            { hash: ledgerHash },
            { hash: actionsHash },
            { hash: contractHash },
            { hash: txHash }
        ],
        index_tickers: [
            { tick: tickName }
        ],
        index_actions: [
            { action: 'SEND' }
        ],
        // Core tables
        blocks: [
            {
                block_index: blockIndex,
                block_time: blockTime,
                ledger_hash_id: null,   // filled in after index inserts
                actions_hash_id: null,
                contract_hash_id: null
            }
        ],
        transactions: [
            {
                tx_index: txIndex,
                block_index: blockIndex,
                tx_hash_id: null,       // filled in after index inserts
                source_id: null
            }
        ],
        actions: [
            {
                action_index: actionIndex,
                block_index: blockIndex,
                tx_index: txIndex,
                tx_vout: 0,
                action_id: 1,
                action_format: 0
            }
        ],
        credits: [
            {
                action_index: actionIndex,
                address_id: null,       // filled in after index inserts
                tick_id: null,
                amount: creditAmt
            }
        ],
        debits: debitAmt !== '0' ? [
            {
                action_index: actionIndex,
                address_id: null,
                tick_id: null,
                amount: debitAmt
            }
        ] : [],
        // Metadata
        _meta: {
            blockIndex, txIndex, actionIndex, blockTime,
            sourceAddr, tickName,
            ledgerHash, actionsHash, contractHash, txHash
        }
    };
}

// Seed a range of blocks into a database
// Handles index table lookups to fill in foreign key IDs
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

        // Insert index rows (IGNORE duplicates)
        for (let addr of block.index_addresses) {
            await db.doQuery("INSERT IGNORE INTO index_addresses (address) VALUES (?)", [addr.address]);
        }
        for (let tx of block.index_transactions) {
            await db.doQuery("INSERT IGNORE INTO index_transactions (hash) VALUES (?)", [tx.hash]);
        }
        for (let tick of block.index_tickers) {
            await db.doQuery("INSERT IGNORE INTO index_tickers (tick) VALUES (?)", [tick.tick]);
        }
        for (let act of block.index_actions) {
            await db.doQuery("INSERT IGNORE INTO index_actions (action) VALUES (?)", [act.action]);
        }

        // Look up IDs
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

        // Insert blocks
        await db.doQuery(
            "INSERT INTO blocks (block_index, block_time, ledger_hash_id, actions_hash_id, contract_hash_id) VALUES (?, ?, ?, ?, ?)",
            [meta.blockIndex, meta.blockTime, ledgerHashId, actionsHashId, contractHashId]
        );

        // Insert transactions
        await db.doQuery(
            "INSERT INTO transactions (tx_index, block_index, tx_hash_id, source_id) VALUES (?, ?, ?, ?)",
            [meta.txIndex, meta.blockIndex, txHashId, addrId]
        );

        // Insert actions
        await db.doQuery(
            "INSERT INTO actions (action_index, block_index, tx_index, tx_vout, action_id, action_format) VALUES (?, ?, ?, ?, ?, ?)",
            [meta.actionIndex, meta.blockIndex, meta.txIndex, 0, 1, 0]
        );

        // Insert credits
        if (block.credits.length > 0) {
            await db.doQuery(
                "INSERT INTO credits (action_index, address_id, tick_id, amount) VALUES (?, ?, ?, ?)",
                [meta.actionIndex, addrId, tickId, block.credits[0].amount]
            );
        }

        // Insert debits
        if (block.debits.length > 0) {
            await db.doQuery(
                "INSERT INTO debits (action_index, address_id, tick_id, amount) VALUES (?, ?, ?, ?)",
                [meta.actionIndex, addrId, tickId, block.debits[0].amount]
            );
        }
    }

    // Rebuild balances from credits/debits
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

// Delete all data at or after a given block (simulates source-side reorg)
async function deleteBlocksFrom(db, blockIndex) {
    // Get first action_index at this block
    let actionRows = await db.doQuery(
        "SELECT action_index FROM actions WHERE block_index >= ? ORDER BY action_index ASC LIMIT 1",
        [blockIndex]
    );
    let firstActionIndex = actionRows.length > 0 ? actionRows[0].action_index : null;

    // Delete action-scoped data
    if (firstActionIndex !== null) {
        let actionTables = ['credits', 'debits', 'sends', 'issues', 'tokens', 'destroys', 'actions'];
        for (let table of actionTables) {
            try {
                await db.doQuery("DELETE FROM `" + table + "` WHERE action_index >= ?", [firstActionIndex]);
            } catch (e) { /* table may not have data */ }
        }
    }

    // Delete block-scoped data
    await db.doQuery("DELETE FROM transactions WHERE block_index >= ?", [blockIndex]);
    await db.doQuery("DELETE FROM blocks WHERE block_index >= ?", [blockIndex]);
    try {
        await db.doQuery("DELETE FROM sync_meta WHERE block_index >= ?", [blockIndex]);
    } catch (e) { /* may not exist */ }

    // Rebuild balances
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
    } catch (e) { /* no data */ }
}

module.exports = {
    blockHash,
    buildBlock,
    seedBlocks,
    deleteBlocksFrom
};
