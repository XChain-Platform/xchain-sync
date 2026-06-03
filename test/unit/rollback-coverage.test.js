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
// attestation_validator_stats is a snapshot-only aggregate the thin replica
// can't recompute (no capability/governance machinery for missed_count); on
// reorg _rollbackIndexer drops its orphaned-range rows and the next full-snapshot
// ride-along restores correct counts from the source (same model as markets).
// price_snapshots anchors each round to a block via reference_block (not
// block_index/action_index), so it falls outside both generic delete loops and
// is removed by its own bespoke DELETE in _rollbackIndexer — mirroring the
// source indexer. Live convergence is also hub-driven (the hub DB sync mirror
// delivers row:deleted events), but that lags the local reorg; the bespoke
// delete closes the staleness window so the replica never serves finalized rows
// for orphaned rounds while the hub catches up.
const SPECIAL_CASE = ['contract_emissions', 'sync_meta', 'attestation_validator_stats', 'price_snapshots'];

// Tables that are NOT rolled back, each with a reason. All intentional:
// snapshot-refreshed or append-only aggregates the thin replica can't recompute.
const ROLLBACK_EXEMPT = {
    events:
        'Append-only operational log; no block_index/action_index cursor to scope ' +
        'a rollback. Never rolled back — matches the source indexer.',
    markets:
        'Derived OHLCV aggregate keyed by (tick1_id, tick2_id) — no action_index column. ' +
        'ServerPoller deliberately omits it from actionScopedTables (it cannot ride the ' +
        'per-block action_index join): markets is NOT streamed per-block (it only rides ' +
        'along in a full snapshot). The source indexer ' +
        'recomputes it via getMarketInfo (last-trade / 24hr price-high-low-change-volume / ' +
        'bid / ask over orders/order_matches/dispenses), which the thin replica DB has no ' +
        'machinery to reproduce — replicating that math here would re-introduce exactly the ' +
        'indexer-mirror drift this guard exists to catch. The replica instead recovers ' +
        'markets from the next full/incremental snapshot (the existing snapshot ride-along). ' +
        'Deliberately snapshot-refreshed, not block-rolled-back.',
    icons:
        'Token-icon processing state keyed by token_id — no block/action cursor. ' +
        'Full-snapshot ride-along only (SnapshotBuilder), not block-streamed.',
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

    it('attests is rolled back on the replica under its consolidated name, not the phantom split names', function(){
        // The ATTEST tables were consolidated into a single `attests` table
        // (one row per ATTEST action_index: v0 request / v1 response / v2 expire).
        // ClientRollback previously listed the phantom split names
        // `attestation_requests` / `attestation_responses`, which exist in no
        // replica schema, so the DELETEs silently no-op'd and orphaned ATTEST
        // rows from dead-chain blocks survived every reorg (exposed via the
        // explorer's public attestation REST + WS APIs). Guard the consolidated
        // name in, and the phantom split names out.
        assert.ok(
            rollback.dataTables.includes('attests'),
            'attests is the consolidated ATTEST table (replicated action-scoped + rolled back by the ' +
            'source indexer); it must be in ClientRollback.dataTables or reorged ATTEST rows linger on every replica.'
        );
        assert.ok(
            !rollback.dataTables.includes('attestation_requests'),
            'attestation_requests is a phantom split name that exists in no replica schema; ' +
            'its DELETE silently no-ops and never cleans the consolidated attests table.'
        );
        assert.ok(
            !rollback.dataTables.includes('attestation_responses'),
            'attestation_responses is a phantom split name that exists in no replica schema; ' +
            'its DELETE silently no-ops and never cleans the consolidated attests table.'
        );
    });

    it('balances is recomputed, not blindly deleted by index', function(){
        assert.ok(!rollback.dataTables.includes('balances'));
        assert.ok(!rollback.blockTables.includes('balances'));
    });
});
