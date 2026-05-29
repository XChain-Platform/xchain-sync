/**
 * test/unit/rollback-coverage.test.js
 *
 * Rollback coverage guard (mirror of xchain-indexer's guard).
 *
 * xchain-sync replicates two DB types (indexer + decoder) to followers. The set
 * of tables it streams is declared in ServerPoller (per dbType). On a source
 * reorg, ClientRollback must do something deliberate with each replicated table,
 * or rows in the orphaned block range survive on the replica and it silently
 * diverges from the source — the exact failure xchain-indexer's rollback guard
 * prevents on the source side.
 *
 * The risk here is sharper than on the source: ClientRollback's table lists are
 * a hand-maintained mirror of xchain-indexer/src/rollback.js (see the header
 * comment there). They have drifted before — `prices` was added to the indexer's
 * rollback set but not here, so reorged price rows lingered on every replica.
 * This test fails when a table ServerPoller replicates is not handled by
 * ClientRollback for that dbType, catching the drift at CI time.
 *
 * To satisfy this test, a newly replicated table must be either:
 *   - rolled back by ClientRollback (dataTables / blockTables for indexer;
 *     decoderBlockTables / decoderTxScopedTables for decoder), or
 *   - recomputed during rollback (RECOMPUTED), or
 *   - handled by bespoke logic (SPECIAL_CASE), or
 *   - listed in ROLLBACK_EXEMPT with a reason, or
 *   - an `index_` / append-only lookup (orphan rows are inert; re-sent INSERT IGNORE).
 *
 * Classify by understanding the table, not by silencing the test.
 */

'use strict';

const assert         = require('assert');
const ClientRollback = require('../../src/ClientRollback');
const ServerPoller   = require('../../src/ServerPoller');
const Utility        = require('../../src/utility');

// ServerPoller's constructor only assigns the per-dbType table lists (no DB or
// network work), so we can read them straight off a stub-backed instance.
function replicatedTables(dbType){
    const sp = new ServerPoller(null, null, { dbType }, null, null, {}, new Utility());
    const universe = new Set([
        ...sp.blockScopedTables,
        ...sp.txScopedTables,
        ...sp.actionScopedTables,
        ...(sp.infraTables || []),
        ...sp.indexTables,
    ]);
    return { sp, universe: [...universe].sort() };
}

const isLookupTable = (t) => t.startsWith('index_') || t === 'pubkeys';

// ── Coverage that lives outside ClientRollback's table arrays ──

// Recomputed from surviving ledger rows during rollback (ClientRollback and
// ClientApplier both rebuild these derived aggregates):
//   - balances           ← credits/debits
//   - contract_balances   ← valid deposits/withdrawals (custody balances).
// Both lack an action_index column, so the source poller can't stream them
// per-block (its action_index JOIN errors and is swallowed); the replica derives
// them from the surviving ledger rows instead.
const RECOMPUTED = ['balances', 'contract_balances'];

// Deleted by bespoke logic in _rollbackIndexer rather than the generic loops.
const SPECIAL_CASE = ['contract_emissions', 'sync_meta'];

// Tables ServerPoller names but that are NOT rolled back, each with a reason.
// Several are intentional; two flag a known smell (see test below).
const ROLLBACK_EXEMPT = {
    events:
        'Append-only operational log; no block_index/action_index cursor to scope ' +
        'a rollback. Never rolled back — matches the source indexer.',
    markets:
        'Derived OHLCV aggregate keyed by (tick1_id, tick2_id) — no action_index column. ' +
        'ServerPoller lists it under actionScopedTables but getActionScopedRows JOINs ' +
        'ON action_index, so the query errors and is swallowed: markets is NOT actually ' +
        'streamed per-block (it only rides along in a full snapshot). The source indexer ' +
        'recomputes it via getMarketInfo (last-trade / 24hr price-high-low-change-volume / ' +
        'bid / ask over orders/order_matches/dispenses), which the thin replica DB has no ' +
        'machinery to reproduce — replicating that math here would re-introduce exactly the ' +
        'indexer-mirror drift this guard exists to catch. The replica instead recovers ' +
        'markets from the next full/incremental snapshot (the existing snapshot ride-along). ' +
        'Deliberately snapshot-refreshed, not block-rolled-back.',
    icons:
        'Token-icon processing state keyed by token_id — no block/action cursor. ' +
        'Full-snapshot ride-along only (SnapshotBuilder), not block-streamed.',
    attestation_validator_stats:
        'Running per-validator aggregate counters — no per-block cursor. Full-snapshot ' +
        'ride-along; reorg rollback deferred (mirrors the source indexer deferral).',
    price_snapshots:
        'Mirrored from the hub price channel (round_number/coin_pair); live convergence ' +
        'is handled by the hub DB sync mirror, not this block stream.',
};

describe('Rollback coverage guard @regression', function(){
    let rollback;

    before(function(){
        rollback = new ClientRollback({ dbType: 'indexer' }, new Utility());
    });

    it('sanity: ServerPoller declares a meaningful indexer table set', function(){
        const { universe } = replicatedTables('indexer');
        assert.ok(universe.length > 50, `expected >50 replicated indexer tables, found ${universe.length}`);
    });

    it('every replicated INDEXER table is handled on reorg', function(){
        const { universe } = replicatedTables('indexer');
        const covered = new Set([
            ...rollback.dataTables,
            ...rollback.blockTables,
            ...RECOMPUTED,
            ...SPECIAL_CASE,
            ...Object.keys(ROLLBACK_EXEMPT),
        ]);
        const uncovered = universe.filter(t => !covered.has(t) && !isLookupTable(t));

        assert.deepStrictEqual(
            uncovered,
            [],
            uncovered.length
                ? `\n\nThese tables are replicated by ServerPoller (indexer) but NOT handled by\n` +
                  `ClientRollback on reorg:\n` +
                  uncovered.map(t => `    - ${t}`).join('\n') +
                  `\n\nRows would survive a rollback and diverge from the source. Add each to\n` +
                  `ClientRollback (dataTables/blockTables), RECOMPUTED, SPECIAL_CASE, or\n` +
                  `ROLLBACK_EXEMPT (with a reason). This is usually drift from xchain-indexer's\n` +
                  `rollback set — check src/rollback.js there.\n`
                : undefined
        );
    });

    it('every replicated DECODER table is handled on reorg', function(){
        const { universe } = replicatedTables('decoder');
        const covered = new Set([
            ...rollback.decoderBlockTables,
            ...rollback.decoderTxScopedTables,
            ...Object.keys(ROLLBACK_EXEMPT),
        ]);
        const uncovered = universe.filter(t => !covered.has(t) && !isLookupTable(t));

        assert.deepStrictEqual(
            uncovered,
            [],
            uncovered.length
                ? `These tables are replicated by ServerPoller (decoder) but not handled by ` +
                  `_rollbackDecoder: ${uncovered.join(', ')}. Add to decoderBlockTables / ` +
                  `decoderTxScopedTables, or ROLLBACK_EXEMPT with a reason.`
                : undefined
        );
    });

    it('prices is rolled back on the replica (regression: this was the drift that motivated the guard)', function(){
        assert.ok(
            rollback.dataTables.includes('prices'),
            'prices is replicated (action-scoped + infra) and rolled back by the source indexer; ' +
            'it must be in ClientRollback.dataTables or reorged price rows linger on every replica.'
        );
    });

    it('balances is recomputed, not blindly deleted by index', function(){
        assert.ok(!rollback.dataTables.includes('balances'));
        assert.ok(!rollback.blockTables.includes('balances'));
    });
});
