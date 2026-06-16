/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available —
 * contact legal@dankest.llc.
 *
 **********************************************************************
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
// ClientApplier both rebuild this derived aggregate):
//   - balances           ← credits/debits (includes contract custody, keyed by
//                           the contract's derived address C:<CHAIN>:<action_index>)
// It lacks an action_index column, so the source poller can't stream it
// per-block (its action_index JOIN errors and is swallowed); the replica derives
// it from the surviving ledger rows instead.
const RECOMPUTED = ['balances'];

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

    it('every table the source indexer rolls back is mirrored by ClientRollback (cross-repo drift guard)', function(){
        // The per-dbType guards above only see tables ServerPoller *streams*, so a
        // table the indexer rolls back but sync delivers only via full snapshot
        // (not per-block) never enters their universe and escapes them entirely —
        // exactly how the 2026-06 cross-chain table family drifted in undetected.
        // This guard reads the source's rollback list (xchain-indexer/src/rollback.js)
        // DIRECTLY, so any table added there fails this suite until it is either
        // mirrored into ClientRollback or given a deliberate, reasoned exemption here.
        let IndexerRollback;
        try {
            IndexerRollback = require('../../../xchain-indexer/src/rollback.js');
        } catch(e){
            // Sibling indexer repo not checked out (standalone sync deploy). The
            // cross-repo source of truth is unavailable, so skip rather than error;
            // the per-dbType guards above still run.
            this.skip();
            return;
        }
        // Rollback's constructor only assigns config aliases + the static table
        // arrays (no DB/network work), so a bare stub yields the lists we need.
        const indexer = new IndexerRollback({});
        const indexerRollback = new Set([...indexer.dataTables, ...indexer.blockTables]);

        // Tables the indexer rolls back that sync intentionally does NOT mirror,
        // each with a reason. Keep this list tight — it is the deliberate-decision
        // gate that the cross-chain family lacked.
        const INDEXER_LOCAL = {
            pending_hub_pushes:
                'Indexer-local outbound queue of rows awaiting push to the cross-chain hub. ' +
                'Not replicated to followers (absent from replicatedTables.js) and meaningless ' +
                'on a replica, so there is nothing to roll back here — same rationale as the ' +
                'source keeping icons/price_snapshots out of the streamed set.',
        };

        const covered = new Set([
            ...rollback.dataTables,
            ...rollback.blockTables,
            ...RECOMPUTED,
            ...SPECIAL_CASE,
            ...Object.keys(ROLLBACK_EXEMPT),
            ...Object.keys(INDEXER_LOCAL),
        ]);
        const uncovered = [...indexerRollback]
            .filter(t => !covered.has(t) && !isLookupTable(t))
            .sort();

        assert.deepStrictEqual(
            uncovered,
            [],
            uncovered.length
                ? `\n\nThese tables are rolled back by the source indexer (xchain-indexer/src/\n` +
                  `rollback.js) but are NOT handled by ClientRollback and not exempted:\n` +
                  uncovered.map(t => `    - ${t}`).join('\n') +
                  `\n\nReorged rows would survive on every replica. For each, either add it to\n` +
                  `ClientRollback (dataTables/blockTables) — and to replicatedTables.js if it has\n` +
                  `an action_index column so it also live-streams — or add it to INDEXER_LOCAL /\n` +
                  `ROLLBACK_EXEMPT in this test with a reason. Classify by understanding the\n` +
                  `table, not by silencing the guard.\n`
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

    // Cross-repo drift guard for the escrow re-derive SQL (TP-03 #4017). The
    // tokens.escrow_action_index re-derive must run the SAME logic on the source
    // (xchain-indexer/src/rollback.js) and the replica (xchain-sync/src/ClientRollback.js),
    // or a reorg leaves source and replica with different gate values — a silent consensus
    // divergence. Both files carry the SQL between //<ESCROW-REDERIVE-SQL> markers; this
    // guard extracts the backtick SQL literals from each and asserts they are
    // whitespace-normalised identical. If you edit one, edit the other.
    it('escrow re-derive SQL is identical across xchain-indexer and xchain-sync (cross-repo drift guard)', function(){
        const fs = require('fs');
        function escrowSql(path){
            const src = fs.readFileSync(path, 'utf8');
            const m = src.match(/\/\/<ESCROW-REDERIVE-SQL>([\s\S]*?)\/\/<\/ESCROW-REDERIVE-SQL>/);
            assert.ok(m, `ESCROW-REDERIVE-SQL markers not found in ${path}`);
            // pull every backtick literal, normalise whitespace
            const lits = m[1].match(/`[^`]*`/g) || [];
            assert.ok(lits.length >= 2, `expected >=2 SQL literals in the marked block of ${path}, got ${lits.length}`);
            return lits.map(l => l.replace(/`/g, '').replace(/\s+/g, ' ').trim()).join('\n');
        }
        let indexerPath, syncPath = require('path').resolve(__dirname, '../../src/ClientRollback.js');
        try { indexerPath = require('path').resolve(__dirname, '../../../xchain-indexer/src/rollback.js'); }
        catch(e){ this.skip(); return; }
        if(!require('fs').existsSync(indexerPath)) this.skip();
        assert.strictEqual(escrowSql(syncPath), escrowSql(indexerPath),
            'escrow re-derive SQL drifted between xchain-sync/ClientRollback.js and xchain-indexer/rollback.js — keep them identical');
    });

    // Bespoke-logic parity (not a table-name check): the cooldown-maturity reversal is an
    // in-place reset on SURVIVING credits/unstakes/contract_unstakes rows. Those tables are
    // already in both rollback lists, so the table-membership guard above structurally cannot
    // catch an unmirrored reset (this is exactly how the gap reached HEAD — see #4248/#4249).
    // Assert both rollback.js (source) and ClientRollback.js (replica) carry all four operations
    // the source added in 309fec7. If you add a new in-place reorg reset to one file, mirror it
    // in the other and extend this guard.
    it('cooldown-maturity reversal is mirrored across xchain-indexer and xchain-sync (bespoke-logic drift guard)', function(){
        const fs = require('fs'), pathMod = require('path');
        const syncPath = pathMod.resolve(__dirname, '../../src/ClientRollback.js');
        let indexerPath = pathMod.resolve(__dirname, '../../../xchain-indexer/src/rollback.js');
        if(!fs.existsSync(indexerPath)) this.skip();
        // Whitespace-normalised fragments that uniquely identify each of the four ops.
        const OPS = [
            { name: 'capability refund-credit delete', re: /DELETE c FROM credits c JOIN unstakes u ON u\.action_index = c\.action_index/ },
            { name: 'contract refund-credit delete',   re: /DELETE c FROM credits c JOIN contract_unstakes cu ON cu\.action_index = c\.action_index/ },
            { name: 'unstakes status reset',           re: /UPDATE unstakes SET status_id = \? WHERE status_id = \? AND cooldown_end_block >= \? AND block_index < \?/ },
            { name: 'contract_unstakes status reset',  re: /UPDATE contract_unstakes SET status_id = \? WHERE status_id = \? AND cooldown_end_block >= \? AND block_index < \?/ },
        ];
        for(const [label, p] of [['ClientRollback.js (replica)', syncPath], ['rollback.js (source)', indexerPath]]){
            // Normalise away the two ways the same SQL is spelled: the source uses backtick
            // template literals; the replica concatenates double-quoted strings with `+`. Strip
            // quotes/backticks and the string-concat `+`, then collapse whitespace, so both reduce
            // to the same fragment text.
            const norm = fs.readFileSync(p, 'utf8')
                .replace(/[`"']/g, ' ')
                .replace(/\s+\+\s+/g, ' ')
                .replace(/\s+/g, ' ');
            for(const op of OPS){
                assert.ok(op.re.test(norm), `${label} is missing the cooldown-maturity ${op.name} — source and replica must both reverse it on reorg`);
            }
        }
    });

    // Forward parity (the other half of the cooldown-maturity twin): the reverse delete
    // above removes the refund credit on reorg, but the FORWARD path must stream that same
    // credit to followers in the first place — keyed by maturity block, since its backdated
    // action_index escapes every action-scoped channel (see #4316). The selection lives in
    // cooldownCredits.js and MUST mirror the reverse join keys / cooldown_end_block predicate,
    // or source and follower diverge. If you change one side, change the other and this guard.
    it('forward cooldown-credit selection mirrors the reverse delete keys (bespoke-logic drift guard)', function(){
        const fs = require('fs'), pathMod = require('path');
        const norm = s => s.replace(/[`"']/g, ' ').replace(/\s+\+\s+/g, ' ').replace(/\s+/g, ' ');
        const fwd = norm(fs.readFileSync(pathMod.resolve(__dirname, '../../src/cooldownCredits.js'), 'utf8'));
        const FWD_OPS = [
            { name: 'capability refund select (GAS, by unstake action_index)', re: /SELECT c\.\* FROM credits c JOIN unstakes u ON u\.action_index = c\.action_index AND u\.source_id = c\.address_id/ },
            { name: 'contract refund select (own tick)',                       re: /SELECT c\.\* FROM credits c JOIN contract_unstakes cu ON cu\.action_index = c\.action_index AND cu\.source_id = c\.address_id AND cu\.tick_id = c\.tick_id/ },
            { name: 'capability maturity-block + completed predicate',         re: /WHERE u\.status_id = \? AND u\.cooldown_end_block BETWEEN \? AND \?/ },
            { name: 'contract maturity-block + completed predicate',           re: /WHERE cu\.status_id = \? AND cu\.cooldown_end_block BETWEEN \? AND \?/ },
        ];
        for(const op of FWD_OPS){
            assert.ok(op.re.test(fwd), `cooldownCredits.js is missing the forward ${op.name} — it must mirror ClientRollback's reverse delete keys`);
        }
        // Both forward channels (live per-block + incremental snapshot) must actually invoke it,
        // or one of them silently re-opens the gap for its replication path.
        for(const f of ['../../src/ServerPoller.js', '../../src/SnapshotBuilder.js']){
            const src = fs.readFileSync(pathMod.resolve(__dirname, f), 'utf8');
            assert.ok(/collectMaturedCooldownCredits\s*\(/.test(src),
                `${f} does not call collectMaturedCooldownCredits — its replication channel drops cooldown-maturity refunds`);
        }
    });

    // The maturity event has TWO forward effects that must both be replicated: the refund
    // credit (above, via cooldownCredits.js) AND the in-place status_id flip to 'completed'
    // on the surviving unstake row. The credit rides the credits channel; the status flip
    // must ride the updated_rows channel keyed by cooldown_end_block — the forward twin of
    // ClientRollback's reverse status reset. Without it the follower keeps a stale 'valid'
    // unstake while its balance is already refunded (see #4317).
    it('updated_rows carries the cooldown-maturity status_id flip keyed by cooldown_end_block', function(){
        const { COOLDOWN_STATUS_TABLES } = require('../../src/updatedRows');
        assert.deepStrictEqual(COOLDOWN_STATUS_TABLES, ['unstakes', 'contract_unstakes'],
            'updated_rows must track the cooldown status flip on both unstake tables');
        const fs = require('fs'), pathMod = require('path');
        const src = fs.readFileSync(pathMod.resolve(__dirname, '../../src/updatedRows.js'), 'utf8')
            .replace(/[`"']/g, ' ').replace(/\s+\+\s+/g, ' ').replace(/\s+/g, ' ');
        assert.ok(/WHERE cooldown_end_block BETWEEN \? AND \?/.test(src),
            'updatedRows.js must select the cooldown status flip by cooldown_end_block (the maturity-block key the reverse reset and the forward credit select share)');
    });
});
