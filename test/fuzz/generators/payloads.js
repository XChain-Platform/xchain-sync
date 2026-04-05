/**
 * fast-check arbitraries for structured objects passed to module-level public APIs.
 *
 * Block payloads, snapshot objects, hub config responses, WebSocket events, hash pairs.
 */

const fc = require('fast-check');
const {
    blockIndex, blockTime, hashString, bigIntLikeValue,
    coinString, networkString, portValue, addressString,
} = require('./values');
const {
    blocksRow, transactionsRow, actionsRow, creditsRow, debitsRow,
    genericDataRow, rowArray,
} = require('./rows');

// Action-scoped table names (subset for payload generation)
const ACTION_TABLES = [
    'sends', 'issues', 'destroys', 'mints', 'tokens', 'credits', 'debits',
    'orders', 'dispensers', 'swaps', 'broadcasts', 'files', 'messages',
    'fees', 'balances', 'stakes', 'contracts', 'deposits', 'withdrawals',
];

/**
 * Internal: build a data object with known table arrays + random action-scoped tables.
 */
function blockDataObject() {
    return fc.record({
        blocks: rowArray(blocksRow(), 0, 3),
        transactions: rowArray(transactionsRow(), 0, 10),
        actions: rowArray(actionsRow(), 0, 10),
        credits: rowArray(creditsRow(), 0, 5),
        debits: rowArray(debitsRow(), 0, 5),
    }).chain(core => {
        // Add 0–5 random action-scoped tables
        return fc.array(
            fc.tuple(
                fc.constantFrom(...ACTION_TABLES),
                rowArray(genericDataRow(), 1, 5)
            ),
            { minLength: 0, maxLength: 5 }
        ).map(extras => {
            let data = { ...core };
            for (let [table, rows] of extras) {
                if (!data[table]) data[table] = rows;
            }
            return data;
        });
    });
}

/** Complete block payload as passed to ClientApplier.applyBlock() */
function blockPayload() {
    return fc.record({
        type: fc.constant('block'),
        chain: coinString(),
        network: networkString(),
        block_index: blockIndex(),
        block_time: blockTime(),
        ledger_hash: hashString(),
        actions_hash: hashString(),
        contract_hash: hashString(),
        data: blockDataObject(),
    });
}

/** Block payload with possibly missing/malformed fields for crash-safety testing */
function partialBlockPayload() {
    return fc.oneof(
        blockPayload(),
        fc.record({ block_index: blockIndex() }),
        fc.record({ data: fc.anything() }),
        fc.record({ block_index: blockIndex(), data: fc.constant({}) }),
        fc.record({
            block_index: fc.oneof(blockIndex(), fc.constantFrom(null, undefined, NaN, 'abc', -1)),
            data: fc.oneof(blockDataObject(), fc.constant(null), fc.constant(undefined)),
        }),
        fc.constant(null),
        fc.constant(undefined),
    );
}

/**
 * Internal: build a tables object for snapshots.
 * 0–15 table keys, each with 0–20 rows.
 */
function snapshotTablesObject() {
    let tableNames = [
        'blocks', 'transactions', 'actions', 'credits', 'debits', 'balances',
        'sends', 'issues', 'tokens', 'orders', 'fees',
        'index_addresses', 'index_transactions', 'index_tickers', 'index_actions',
    ];
    return fc.subarray(tableNames, { minLength: 0, maxLength: 15 })
        .chain(names =>
            fc.tuple(...names.map(() => rowArray(genericDataRow(), 0, 20)))
                .map(rowArrays => {
                    let tables = {};
                    names.forEach((n, i) => { tables[n] = rowArrays[i]; });
                    return tables;
                })
        );
}

/** Full snapshot as passed to ClientApplier.applyFullSnapshot() */
function fullSnapshotPayload() {
    return fc.record({
        block_height: blockIndex(),
        tables: snapshotTablesObject(),
    });
}

/** Incremental snapshot as passed to ClientApplier.applyIncrementalSnapshot() */
function incrementalSnapshotPayload() {
    return fc.record({
        since_block: blockIndex(),
        block_height: blockIndex(),
        tables: snapshotTablesObject(),
    });
}

/** Snapshot with possibly missing/malformed fields */
function partialSnapshotPayload() {
    return fc.oneof(
        fullSnapshotPayload(),
        fc.record({ block_height: blockIndex() }),
        fc.constant(null),
        fc.constant(undefined),
    );
}

/** Hub getallconfigs response — nested coin → network → module → params */
function hubConfigResponse() {
    return fc.array(
        fc.tuple(
            coinString().filter(s => typeof s === 'string' && s.length > 0),
            networkString().filter(s => typeof s === 'string' && s.length > 0),
            fc.boolean(),
            fc.record({
                db_host: fc.oneof(addressString(), fc.constant('localhost'), fc.constant(undefined)),
                db_port: portValue(),
                name: fc.string({ minLength: 0, maxLength: 50 }),
                user: fc.string({ minLength: 0, maxLength: 30 }),
                pass: fc.string({ minLength: 0, maxLength: 30 }),
            }),
        ),
        { minLength: 0, maxLength: 5 }
    ).map(entries => {
        let result = {};
        for (let [coin, network, hasIndexer, params] of entries) {
            if (!result[coin]) result[coin] = {};
            if (!result[coin][network]) result[coin][network] = {};
            if (hasIndexer) {
                result[coin][network]['xchain-indexer'] = {
                    db_host: params.db_host,
                    db_port: params.db_port,
                    name: params.name,
                    user: params.user,
                    pass: params.pass,
                };
            }
        }
        return result;
    });
}

/** Adversarial hub response — anything goes */
function adversarialHubResponse() {
    return fc.oneof(
        hubConfigResponse(),
        fc.constant(null),
        fc.constant(undefined),
        fc.anything(),
        fc.string(),
        fc.array(fc.anything(), { minLength: 0, maxLength: 5 }),
    );
}

/** WebSocket event message — block, reorg, status, or garbage */
function wsEventMessage() {
    return fc.oneof(
        blockPayload(),
        fc.record({
            type: fc.constantFrom('reorg', 'status', 'ping'),
            block_index: blockIndex(),
            chain: coinString(),
            network: networkString(),
        }),
        fc.string(),
        fc.constant(null),
        fc.anything(),
    );
}

/** Hash triple for compareBlockHashes arguments */
function blockHashPair() {
    return fc.record({
        ledger_hash: hashString(),
        actions_hash: hashString(),
        contract_hash: hashString(),
    });
}

/** Payload for verifyChainContinuity */
function continuityPayload() {
    return fc.record({
        block_index: fc.oneof(
            blockIndex(),
            fc.integer({ min: -10, max: 0 }),
            fc.constantFrom(null, undefined, NaN),
        ),
    });
}

module.exports = {
    blockPayload,
    partialBlockPayload,
    fullSnapshotPayload,
    incrementalSnapshotPayload,
    partialSnapshotPayload,
    hubConfigResponse,
    adversarialHubResponse,
    wsEventMessage,
    blockHashPair,
    continuityPayload,
};
