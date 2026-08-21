/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
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
 * diverges from the source (the exact failure xchain-indexer's rollback guard
 * prevents on the source side).
 *
 * The risk here is sharper than on the source: ClientRollback's table lists are
 * a hand-maintained mirror of xchain-indexer/src/rollback.js (see the header
 * comment there). They have drifted before: `prices` was added to the indexer's
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
const replicatedTablesMod = require('../../src/replicatedTables');

// The `special` bucket (stream:special) carries replicated-but-not-per-scope-extracted
// tables: sync_meta on indexer, dispensers on decoder. ServerPoller never assigns it to a
// per-scope list, but the /status completeness count DOES include it
// (replicatedTables.getReplicatedTables -> t.special). Fold it into the coverage universe
// so a future stream:special table lacking replica-rollback handling can't pass CI while
// accumulating orphaned rows on every replica after a reorg.
function replicatedTables(dbType){
    const sp = new ServerPoller(null, null, { dbType }, null, null, {}, new Utility());
    const universe = new Set([
        ...sp.blockScopedTables,
        ...sp.txScopedTables,
        ...sp.actionScopedTables,
        ...(sp.infraTables || []),
        ...sp.indexTables,
        ...(replicatedTablesMod.getTopology(dbType).special || []),
    ]);
    return { sp, universe: [...universe].sort() };
}

// stream:special tables deliberately NOT rolled back row-by-row on reorg, with the reason.
// Kept in the test (not the registry) because these tables carry a non-'exempt'
// replicaRollback mode for other paths; the exemption is specifically about the reorg
// row-delete. dispensers: the decoder live-prunes it (soft-expire) and it converges via the
// full snapshot only, so deleting its rows on a reorg would corrupt full-snapshot state with
// no live stream to restore them (ClientRollback.js: decoderTxScopedTables comment).
const SPECIAL_BUCKET_ROLLBACK_EXEMPT = {
    dispensers: 'decoder live-prunes; converges via full snapshot, untouched on reorg',
};

// Append-only / id-keyed dedup lookups whose orphan rows are inert (re-sent INSERT
// IGNORE on forward apply). NOTE: in the INDEXER db, index_addresses / index_tickers are
// NOT inert (a wire ^<id> makes their ids consensus-relevant) and ARE rolled back; that is
// asserted positively below ('index id lookups are rolled back ...'). They stay listed as
// lookups here because in the DECODER db they remain inert append-only mirrors.
// pubkeys is likewise inert in the DECODER db, but in the INDEXER db it is now
// orphan-swept on reorg (a reclaimed address_id re-points the surviving row; see
// SPECIAL_CASE / the parity-sweep guard). It stays in this predicate for the
// decoder-universe coverage check, where it remains inert.
const isLookupTable = (t) => t.startsWith('index_') || t === 'pubkeys';

// Coverage that lives outside ClientRollback's table arrays: the replica-side
// rollback buckets, derived from the table-lifecycle registry twin
// (src/tableLifecycle.js, byte-identical to the xchain-indexer copy; asserted
// below). Per-table rationale lives with each registry entry.
const lifecycleTwin = require('../../src/tableLifecycle');
const { RECOMPUTED, SPECIAL_CASE, ROLLBACK_EXEMPT, INDEXER_LOCAL } = lifecycleTwin.replicaRollbackBuckets();

// Resolve a file inside the sibling xchain-indexer repo. CI checks the sibling out
// and exports XCHAIN_INDEXER_SQL_PATH (=<root>/src/sql); locally it is the monorepo
// sibling three levels up. (The unit tier previously hard-coded only the
// monorepo path, which CI never populates, so every cross-repo guard below skipped.)
function indexerFile(rel){
    const pathMod = require('path');
    const root = process.env.XCHAIN_INDEXER_SQL_PATH
        ? pathMod.resolve(process.env.XCHAIN_INDEXER_SQL_PATH, '..', '..')
        : pathMod.resolve(__dirname, '..', '..', '..', 'xchain-indexer');
    return pathMod.resolve(root, rel);
}
// A consensus drift guard must never silently pass by skipping. When the sibling is
// present we run; when it is absent we HARD-FAIL only where the sibling is REQUIRED,
// i.e. the job that checks it out and sets XCHAIN_REQUIRE_SIBLINGS=1 (the e2e job in
// .github/workflows/ci.yml), so the guard can never green-by-skip there. We do NOT key
// on the generic CI flag: GitHub sets CI=true in the shared unit `ci` job too, which
// does not check out the sibling, and hard-failing there would just be noise. Returns
// false (caller should `return`) when it skipped; throws when required-but-missing.
const SIBLING_REQUIRED = process.env.XCHAIN_REQUIRE_SIBLINGS === '1';
function requireSibling(ctx, absPath){
    if(require('fs').existsSync(absPath)) return true;
    if(SIBLING_REQUIRED)
        throw new Error('consensus drift guard cannot run: sibling missing at ' + absPath +
            ' (check out xchain-indexer or set XCHAIN_INDEXER_SQL_PATH; ci.yml e2e job places it at .xchain-indexer)');
    ctx.skip();
    return false;
}

describe('Rollback coverage guard @regression', function(){
    let rollback;

    before(function(){
        rollback = new ClientRollback({ dbType: 'indexer' }, new Utility());
    });

    it('sanity: ServerPoller declares a meaningful indexer table set', function(){
        const { universe } = replicatedTables('indexer');
        assert.ok(universe.length > 50, `expected >50 replicated indexer tables, found ${universe.length}`);
    });

    it('stream:special bucket tables join the reorg-coverage universe', function(){
        // Regression for the gap where the guard's universe was built from ServerPoller's
        // per-scope lists only, omitting the stream:special bucket that /status counts. A
        // future stream:special table with no replica-rollback handling would then pass CI
        // while orphaning rows on every replica after a reorg.
        const idx = replicatedTables('indexer').universe;
        assert.ok(idx.includes('sync_meta'),
            'indexer stream:special table sync_meta must be inside the reorg-coverage universe');
        const dec = replicatedTables('decoder').universe;
        assert.ok(dec.includes('dispensers'),
            'decoder stream:special table dispensers must be inside the reorg-coverage universe');
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
                  `rollback set (check src/rollback.js there).\n`
                : undefined
        );
    });

    it('every replicated DECODER table is handled on reorg', function(){
        const { universe } = replicatedTables('decoder');
        const covered = new Set([
            ...rollback.decoderBlockTables,
            ...rollback.decoderTxScopedTables,
            ...Object.keys(ROLLBACK_EXEMPT),
            ...Object.keys(SPECIAL_BUCKET_ROLLBACK_EXEMPT),
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
        // (not per-block) never enters their universe and escapes them entirely.
        // This is exactly how the 2026-06 cross-chain table family drifted in undetected.
        // This guard reads the source's rollback list (xchain-indexer/src/rollback.js)
        // DIRECTLY, so any table added there fails this suite until it is either
        // mirrored into ClientRollback or given a deliberate, reasoned exemption here.
        const rbPath = indexerFile('src/rollback.js');
        if(!requireSibling(this, rbPath)) return;
        const IndexerRollback = require(rbPath);
        // Rollback's constructor only assigns config aliases + the static table
        // arrays (no DB/network work), so a bare stub yields the lists we need.
        const indexer = new IndexerRollback({});
        const indexerRollback = new Set([...indexer.dataTables, ...indexer.blockTables]);

        // INDEXER_LOCAL (tables the indexer rolls back that sync intentionally
        // does not mirror because they never exist on a replica) is derived from
        // the registry twin's replicaRollback 'local' entries above; it is the
        // deliberate-decision gate that the cross-chain family lacked.
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
                  `ClientRollback (dataTables/blockTables) and to replicatedTables.js if it has\n` +
                  `an action_index column so it also live-streams, or add it to INDEXER_LOCAL /\n` +
                  `ROLLBACK_EXEMPT in this test with a reason. Classify by understanding the\n` +
                  `table, not by silencing the guard.\n`
                : undefined
        );
    });

    it('index id lookups are rolled back on the replica and mirror the source indexer (^id consensus)', function(){
        // index_addresses / index_tickers ids become consensus-relevant once an
        // address/ticker can be referenced on the wire as ^<id>: a wire ^<id> is stored
        // verbatim and resolved to a canonical string at block-hash time, so a surviving
        // orphaned id forks the replica's checkpoint after a reorg. The replica copies the
        // source's ids verbatim, so it must delete the same orphaned-block id rows.
        assert.deepStrictEqual(
            [...rollback.indexTables].sort(),
            ['index_addresses', 'index_tickers'],
            'ClientRollback.indexTables must roll back index_addresses and index_tickers'
        );
        // Cross-repo drift guard: the source indexer must roll back the same set.
        const rbPath = indexerFile('src/rollback.js');
        if(!requireSibling(this, rbPath)) return;
        const IndexerRollback = require(rbPath);
        const indexer = new IndexerRollback({});
        assert.deepStrictEqual(
            [...(indexer.indexTables || [])].sort(),
            [...rollback.indexTables].sort(),
            'xchain-indexer rollback.indexTables and xchain-sync ClientRollback.indexTables have drifted'
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

    // Cross-repo drift guard for the escrow re-derive SQL. The
    // tokens.escrow_action_index re-derive must run the SAME logic on the source
    // (xchain-indexer/src/rollback.js) and the replica (xchain-sync/src/ClientRollback.js),
    // or a reorg leaves source and replica with different gate values (a silent consensus
    // divergence). Both files carry the SQL between //<ESCROW-REDERIVE-SQL> markers; this
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
        const syncPath = require('path').resolve(__dirname, '../../src/ClientRollback.js');
        const indexerPath = indexerFile('src/rollback.js');
        if(!requireSibling(this, indexerPath)) return;
        assert.strictEqual(escrowSql(syncPath), escrowSql(indexerPath),
            'escrow re-derive SQL drifted between xchain-sync/ClientRollback.js and xchain-indexer/rollback.js; keep them identical');
    });

    // Cross-repo drift guard for the cross-chain mirror reorg delete. On reorg
    // both the source (xchain-indexer/src/rollback.js) and the replica
    // (xchain-sync/src/ClientRollback.js) locally prune the hub-mirrored
    // cross_chain_calls / cross_chain_matches rows for the orphaned range, closing the
    // staleness window before hub-driven convergence (row:deleted). The predicates must
    // also stay byte-identical to xchain-indexer/src/hub_db_sync.js _applyRetraction so
    // the local belt-and-suspenders delete and the hub-driven delete remove exactly the
    // same rows. Both rollback files carry the SQL between //<CROSS-CHAIN-MIRROR-REORG-DELETE>
    // markers; this extracts the backtick literals and asserts whitespace-normalised
    // equality. If you edit one, edit the other.
    it('cross-chain mirror reorg delete SQL is identical across xchain-indexer and xchain-sync (cross-repo drift guard)', function(){
        const fs = require('fs');
        function crossChainSql(path){
            const src = fs.readFileSync(path, 'utf8');
            const m = src.match(/\/\/<CROSS-CHAIN-MIRROR-REORG-DELETE>([\s\S]*?)\/\/<\/CROSS-CHAIN-MIRROR-REORG-DELETE>/);
            assert.ok(m, `CROSS-CHAIN-MIRROR-REORG-DELETE markers not found in ${path}`);
            const lits = m[1].match(/`[^`]*`/g) || [];
            assert.ok(lits.length >= 2, `expected >=2 SQL literals in the marked block of ${path}, got ${lits.length}`);
            return lits.map(l => l.replace(/`/g, '').replace(/\s+/g, ' ').trim()).join('\n');
        }
        const syncPath = require('path').resolve(__dirname, '../../src/ClientRollback.js');
        const indexerPath = indexerFile('src/rollback.js');
        if(!requireSibling(this, indexerPath)) return;
        assert.strictEqual(crossChainSql(syncPath), crossChainSql(indexerPath),
            'cross-chain mirror reorg delete SQL drifted between xchain-sync/ClientRollback.js and xchain-indexer/rollback.js; keep them identical');
    });

    // Cross-repo drift guard for the light-client stakes_root query (SPV spec sec.4.1).
    // The follower rebuilds the BTC stakes_root from db._stakeWeightsSql; it MUST stay
    // byte-identical to xchain-indexer/src/db.js _stakeWeightsSql, or the follower's
    // stakes_root (hence state_root) diverges from the source and the state-commitment
    // check false-halts. Both files carry the method verbatim; this extracts the body
    // and asserts whitespace-normalised equality. If you edit one, edit the other.
    it('_stakeWeightsSql is identical across xchain-indexer and xchain-sync (cross-repo drift guard)', function(){
        const fs = require('fs');
        function stakeSql(p){
            const src = fs.readFileSync(p, 'utf8');
            const m = src.match(/_stakeWeightsSql\(valid_id, blockIndex, minStake\)\{([\s\S]*?)return \{ sql, args \};/);
            assert.ok(m, `_stakeWeightsSql not found in ${p}`);
            return m[1].replace(/\s+/g, ' ').trim();
        }
        const syncPath = require('path').resolve(__dirname, '../../src/db.js');
        const indexerPath = indexerFile('src/db.js');
        if(!requireSibling(this, indexerPath)) return;
        assert.strictEqual(stakeSql(syncPath), stakeSql(indexerPath),
            '_stakeWeightsSql drifted between xchain-sync/src/db.js and xchain-indexer/src/db.js; keep them byte-identical (the stakes_root is consensus-critical)');
    });

    // Cross-repo drift guard for the SWQ source-cap windowed wrapper (SWQ-TRUNC-1
    // liveness half). At/after SWQ_SOURCE_CAP_ACTIVATION this replaces the raw key-row
    // LIMIT and feeds the hashed stakes_root, so it MUST stay byte-identical to the
    // xchain-indexer twin or the follower's stakes_root diverges above the height. The
    // per-repo JS gate wrappers (_stakeWeightsWithCap / _applyStakeWeightCap) differ by
    // design - the indexer reads network/coin from this.config, sync from params - but
    // both call THIS builder + the shared swq_source_cap_activation.js caps, which are
    // the consensus-relevant surface. If you edit one _cappedStakeWeightsSql, edit both.
    it('_cappedStakeWeightsSql is identical across xchain-indexer and xchain-sync (cross-repo drift guard)', function(){
        const fs = require('fs');
        function cappedSql(p){
            const src = fs.readFileSync(p, 'utf8');
            const m = src.match(/_cappedStakeWeightsSql\(inner, maxSources, maxKeys\)\{([\s\S]*?)return \{ sql, args \};/);
            assert.ok(m, `_cappedStakeWeightsSql not found in ${p}`);
            return m[1].replace(/\s+/g, ' ').trim();
        }
        const syncPath = require('path').resolve(__dirname, '../../src/db.js');
        const indexerPath = indexerFile('src/db.js');
        if(!requireSibling(this, indexerPath)) return;
        assert.strictEqual(cappedSql(syncPath), cappedSql(indexerPath),
            '_cappedStakeWeightsSql drifted between xchain-sync/src/db.js and xchain-indexer/src/db.js; keep them byte-identical (feeds the consensus stakes_root at/after the source-cap flag-day)');
    });

    // Bespoke-logic parity (not a table-name check): the cooldown-maturity reversal is an
    // in-place reset on SURVIVING credits/unstakes/contract_unstakes rows. Those tables are
    // already in both rollback lists, so the table-membership guard above structurally cannot
    // catch an unmirrored reset (this is exactly how the gap reached HEAD undetected).
    // Assert both rollback.js (source) and ClientRollback.js (replica) carry all four operations
    // the source added in 309fec7. If you add a new in-place reorg reset to one file, mirror it
    // in the other and extend this guard.
    it('cooldown-maturity reversal is mirrored across xchain-indexer and xchain-sync (bespoke-logic drift guard)', function(){
        const fs = require('fs'), pathMod = require('path');
        const syncPath = pathMod.resolve(__dirname, '../../src/ClientRollback.js');
        const indexerPath = indexerFile('src/rollback.js');
        if(!requireSibling(this, indexerPath)) return;
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
                assert.ok(op.re.test(norm), `${label} is missing the cooldown-maturity ${op.name}; source and replica must both reverse it on reorg`);
            }
        }
    });

    // Orphan-sweep parity, driven by the registry: every ORPHAN_SWEEPS
    // entry with `replica: true` is a derived/lookup table the replica's
    // replication never deletes (markets upserts from the full-dump, pubkeys is
    // INSERT IGNORE, icons is an operator-local cache), so the source's reorg
    // sweep MUST be mirrored in ClientRollback or the replica keeps the orphan
    // and serves the OLD row on index-id reclaim. balances stays replica:false:
    // both sides rebuild it wholesale (rebuildBalances), so the orphan never
    // re-derives. The regexes stay hand-written (the markets sweep spans two
    // columns and cannot be generated from {table, index}), but their key set
    // must exactly equal the registry's replica-flagged tables, so flipping a
    // flag without shipping (or removing) the matching sweep fails here.
    it('registry replica-flagged orphan sweeps are mirrored across xchain-indexer and xchain-sync (parity drift guard)', function(){
        const fs = require('fs'), pathMod = require('path');
        const syncPath = pathMod.resolve(__dirname, '../../src/ClientRollback.js');
        const indexerPath = indexerFile('src/rollback.js');
        if(!requireSibling(this, indexerPath)) return;
        const SWEEP_RES = {
            icons:   /DELETE FROM icons WHERE token_id NOT IN \(SELECT id FROM tokens\)/,
            markets: /DELETE FROM markets WHERE tick1_id NOT IN \(SELECT id FROM index_tickers\) OR tick2_id NOT IN \(SELECT id FROM index_tickers\)/,
            pubkeys: /DELETE FROM pubkeys WHERE address_id NOT IN \(SELECT id FROM index_addresses\)/,
        };
        const flagged = [...new Set(lifecycleTwin.ORPHAN_SWEEPS.filter(s => s.replica).map(s => s.table))].sort();
        assert.deepStrictEqual(Object.keys(SWEEP_RES).sort(), flagged,
            'SWEEP_RES keys must equal the registry\'s replica-flagged ORPHAN_SWEEPS tables; add/remove the regex alongside the flag');
        for(const [label, p] of [['ClientRollback.js (replica)', syncPath], ['rollback.js (source)', indexerPath]]){
            const norm = fs.readFileSync(p, 'utf8')
                .replace(/[`"']/g, ' ')
                .replace(/\s+\+\s+/g, ' ')
                .replace(/\s+/g, ' ');
            for(const table of flagged){
                assert.ok(SWEEP_RES[table].test(norm), `${label} is missing the ${table} orphan sweep; source and replica must both sweep it on reorg or the replica diverges on id reclaim`);
            }
        }
    });

    // IDX-2 pair-scoped markets parity. The dangling-tick sweep guarded above
    // misses a market whose pair kept both ticks but lost its only order/trade to the
    // reorg; the source deletes that row, and the replica used to miss it too. markets
    // rides the snapshot as an UPSERT-only full dump, so a row the replica keeps can
    // never be removed by replication and xchain-explorer serves the stale zeroed OHLCV
    // forever. Both sides must carry the same three statements: the survival probe over
    // orders, the survival probe over order_matches, and the two-orientation delete. The
    // source aliases its probe tables (o. / om.) and the replica does not, so the alias
    // is optional in the regex; everything else must match text-for-text.
    it('pair-scoped IDX-2 markets deletion is mirrored across xchain-indexer and xchain-sync (parity drift guard)', function(){
        const fs = require('fs'), pathMod = require('path');
        const syncPath = pathMod.resolve(__dirname, '../../src/ClientRollback.js');
        const indexerPath = indexerFile('src/rollback.js');
        if(!requireSibling(this, indexerPath)) return;
        const IDX2_OPS = [
            { name: 'surviving-orders probe (both orientations)',
              re: /SELECT 1 FROM orders (?:o )?WHERE \((?:o\.)?give_tick_id=\? AND (?:o\.)?get_tick_id=\?\) OR \((?:o\.)?give_tick_id=\? AND (?:o\.)?get_tick_id=\?\) LIMIT 1/ },
            { name: 'surviving-matches probe (both orientations)',
              re: /SELECT 1 FROM order_matches (?:om )?WHERE \((?:om\.)?give_tick_id=\? AND (?:om\.)?get_tick_id=\?\) OR \((?:om\.)?give_tick_id=\? AND (?:om\.)?get_tick_id=\?\) LIMIT 1/ },
            { name: 'two-orientation markets delete',
              re: /DELETE FROM markets WHERE \(tick1_id=\? AND tick2_id=\?\) OR \(tick1_id=\? AND tick2_id=\?\)/ },
        ];
        for(const [label, p] of [['ClientRollback.js (replica)', syncPath], ['rollback.js (source)', indexerPath]]){
            const norm = fs.readFileSync(p, 'utf8')
                .replace(/[`"']/g, ' ')
                .replace(/\s+\+\s+/g, ' ')
                .replace(/\s+/g, ' ');
            for(const op of IDX2_OPS){
                assert.ok(op.re.test(norm), `${label} is missing the IDX-2 ${op.name}; source and replica must both drop a market whose pair kept no surviving order/match, or the replica serves a stale zeroed OHLCV row no snapshot can remove`);
            }
        }
    });

    // Forward parity (the other half of the cooldown-maturity twin): the reverse delete
    // above removes the refund credit on reorg, but the FORWARD path must stream that same
    // credit to followers in the first place, keyed by maturity block, since its backdated
    // action_index escapes every action-scoped channel. The selection lives in
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
            assert.ok(op.re.test(fwd), `cooldownCredits.js is missing the forward ${op.name}; it must mirror ClientRollback's reverse delete keys`);
        }
        // Both forward channels (live per-block + incremental snapshot) must actually invoke it,
        // or one of them silently re-opens the gap for its replication path.
        for(const f of ['../../src/ServerPoller.js', '../../src/SnapshotBuilder.js']){
            const src = fs.readFileSync(pathMod.resolve(__dirname, f), 'utf8');
            assert.ok(/collectMaturedCooldownCredits\s*\(/.test(src),
                `${f} does not call collectMaturedCooldownCredits; its replication channel drops cooldown-maturity refunds`);
        }
    });

    // The maturity event has TWO forward effects that must both be replicated: the refund
    // credit (above, via cooldownCredits.js) AND the in-place status_id flip to 'completed'
    // on the surviving unstake row. The credit rides the credits channel; the status flip
    // must ride the updated_rows channel keyed by cooldown_end_block (the forward twin of
    // ClientRollback's reverse status reset). Without it the follower keeps a stale 'valid'
    // unstake while its balance is already refunded.
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

    // Forward parity for recovery-redriven validator rewards: a reorg re-drain
    // re-materializes a survivor reward at block_index = earn-block E < B, which escapes
    // the block-scoped forward channels. recoveryRewards.js must select it by applied_block
    // (the re-drain point B, the forward analogue of ClientRollback's block_index >= B
    // delete), guard to genuine survivors (vr.block_index < applied_block), and be invoked
    // by BOTH forward channels. The rollback re-arm must reset applied_block so a later
    // reorg restamps it. If you change one side, change the other and this guard.
    it('forward recovery-reward selection mirrors the rollback key (bespoke-logic drift guard)', function(){
        const fs = require('fs'), pathMod = require('path');
        const norm = s => s.replace(/[`"']/g, ' ').replace(/\s+\+\s+/g, ' ').replace(/\s+/g, ' ');
        const fwd = norm(fs.readFileSync(pathMod.resolve(__dirname, '../../src/recoveryRewards.js'), 'utf8'));
        const FWD_OPS = [
            { name: 'validator_rewards / recovery_pending_rewards join (NULL-safe round_reference)', re: /JOIN recovery_pending_rewards rpr ON rpr\.source_id = vr\.source_id AND rpr\.reward_type = vr\.reward_type AND rpr\.round_reference <=> vr\.round_reference/ },
            { name: 'pubkey bridge (lowercase-hex match)',         re: /JOIN index_pubkeys ip ON ip\.id = vr\.signing_pubkey_id AND ip\.pubkey = rpr\.validator_pubkey/ },
            { name: 'applied_block forward-window predicate',      re: /rpr\.applied_block BETWEEN \? AND \?/ },
            { name: 'survivors-only backdating guard',             re: /vr\.block_index < rpr\.applied_block/ },
        ];
        for(const op of FWD_OPS){
            assert.ok(op.re.test(fwd), `recoveryRewards.js is missing the forward ${op.name}; it must mirror the rollback re-drain keys`);
        }
        // Both forward channels (live per-block + incremental snapshot) must invoke it.
        for(const f of ['../../src/ServerPoller.js', '../../src/SnapshotBuilder.js']){
            const src = fs.readFileSync(pathMod.resolve(__dirname, f), 'utf8');
            assert.ok(/collectRedrivenValidatorRewards\s*\(/.test(src),
                `${f} does not call collectRedrivenValidatorRewards; its replication channel drops reorg-redriven recovery rewards`);
        }
        // The source rollback re-arm must reset applied_block=NULL alongside applied/source_id,
        // or a stale applied_block from a prior reorg misses the next re-delivery.
        const rbPath = indexerFile('src/rollback.js');
        if(!requireSibling(this, rbPath)) return;
        const rb = norm(fs.readFileSync(rbPath, 'utf8'));
        assert.ok(/SET applied=0, source_id=NULL, applied_block=NULL/.test(rb),
            'rollback.js re-arm must reset applied_block=NULL alongside applied=0 and source_id=NULL');
    });

    // Forward parity for DERIVED anchor/archive validator rewards: the BTC-side derivation
    // stamps block_index = SNAPSHOT_BLOCK E with derive_block_index = the minting block B,
    // and ClientRollback's reverse delete already keys on derive_block_index >= B. The
    // forward channels must key on the same column (derivedRewards.js) and BOTH forward
    // channels must invoke it, or a continuously-live follower never receives a derived
    // reward (#5605). The source's winner-collapse DELETE must be mirrored forward from the
    // replicated reconcile-log pre-images, the twin of the RB-ANCHOR restore.
    it('forward derived-reward selection mirrors the derive_block_index rollback key, and the reconcile DELETE is mirrored (bespoke-logic drift guard)', function(){
        const fs = require('fs'), pathMod = require('path');
        const norm = s => s.replace(/[`"']/g, ' ').replace(/\s+\+\s+/g, ' ').replace(/\s+/g, ' ');
        const fwd = norm(fs.readFileSync(pathMod.resolve(__dirname, '../../src/derivedRewards.js'), 'utf8'));
        assert.ok(/vr\.derive_block_index BETWEEN \? AND \?/.test(fwd), 'derivedRewards.js must key on derive_block_index (the materialization window)');
        assert.ok(/vr\.block_index < vr\.derive_block_index/.test(fwd), 'derivedRewards.js must restrict to backdated rows (earn-block below the materialization block)');
        const rbSync = norm(fs.readFileSync(pathMod.resolve(__dirname, '../../src/ClientRollback.js'), 'utf8'));
        assert.ok(/DELETE FROM validator_rewards WHERE derive_block_index >= \?/.test(rbSync), 'ClientRollback.js must keep the derive_block_index reverse delete the forward collector twins');
        for(const f of ['../../src/ServerPoller.js', '../../src/SnapshotBuilder.js']){
            const src = fs.readFileSync(pathMod.resolve(__dirname, f), 'utf8');
            assert.ok(/collectDerivedAnchorRewards\s*\(/.test(src),
                `${f} does not call collectDerivedAnchorRewards; its replication channel drops derived anchor/archive rewards`);
        }
        const applier = norm(fs.readFileSync(pathMod.resolve(__dirname, '../../src/ClientApplier.js'), 'utf8'));
        assert.ok(/DELETE vr FROM validator_rewards vr JOIN anchor_reward_reconcile_log d ON d\.source_id = vr\.source_id AND d\.signing_pubkey_id = vr\.signing_pubkey_id AND d\.reward_type = vr\.reward_type AND d\.round_reference <=> vr\.round_reference/.test(applier),
            'ClientApplier.js must mirror the reconcile DELETE from the replicated pre-image log (forward twin of the RB-ANCHOR restore)');
    });

    // Bespoke-logic parity: anchor invalid_archive to unverified reset. When the final v2
    // chunk of a chunked archive batch is orphaned by a reorg, the parent v1 (in a surviving
    // earlier block) was stamped 'invalid_archive' IN PLACE by that chunk's apply; deleting
    // the chunk row leaves the parent stuck 'invalid_archive' on both source and replica until
    // a from-genesis replay. Both rollback.js (source) and ClientRollback.js (replica) must
    // carry the self-join UPDATE that resets the parent to 'unverified'. If you change one,
    // change the other and extend this guard.
    it('anchor invalid_archive to unverified reset is mirrored across xchain-indexer and xchain-sync (bespoke-logic drift guard)', function(){
        const fs = require('fs'), pathMod = require('path');
        const syncPath    = pathMod.resolve(__dirname, '../../src/ClientRollback.js');
        const indexerPath = indexerFile('src/rollback.js');
        if(!requireSibling(this, indexerPath)) return;
        const norm = s => s.replace(/[`"']/g, ' ').replace(/\s+\+\s+/g, ' ').replace(/\s+/g, ' ');
        const ANCHOR_OPS = [
            // NB: norm() strips quote chars and collapses whitespace, so a quoted SQL literal
            // like 'invalid_archive' normalizes to a bare ` invalid_archive ` -- match that form.
            { name: 'anchor invalid_archive parent join',   re: /JOIN index_statuses ps ON ps\.id = p\.status_id AND ps\.status = invalid_archive/ },
            { name: 'anchor orphaned v2 chunk join',        re: /JOIN anchor_actions c ON c\.version = 2 AND c\.match_batch_seq = p\.match_batch_seq/ },
            { name: 'anchor reset to unverified',          re: /JOIN index_statuses us ON us\.status = unverified SET p\.status_id = us\.id/ },
            // The parent predicate must select the FULL archive-head version set
            // (v1 legacy + v6 publisher-bearing) via the shared stateHash.js constant, on
            // both sides. A literal `p.version = 1` regression re-wedges a reorg-orphaned
            // v6 archive batch permanently. Matches the indexer's template-literal splice
            // (${ARCHIVE_HEAD_VERSIONS_SQL}) and the sync side's string concat.
            { name: 'archive-head version predicate (shared v1+v6 constant)',
              re: /WHERE p\.version (\$\{)?ARCHIVE_HEAD_VERSIONS_SQL\}? AND p\.action_index < \?/ },
        ];
        for(const [label, p] of [['ClientRollback.js (replica)', syncPath], ['rollback.js (source)', indexerPath]]){
            const src = norm(fs.readFileSync(p, 'utf8'));
            for(const op of ANCHOR_OPS){
                assert.ok(op.re.test(src), `${label} is missing the anchor ${op.name}; source and replica must both reverse the invalid_archive stamp on reorg`);
            }
        }
    });

    // The archive-head version set is defined ONCE (stateHash.js, twinned across
    // repos) and consumed by every parent-selecting predicate. Pin its value and the SQL
    // fragment shape, and pin the forward updatedRows class to the same constant so a
    // v6 parent's invalid_archive stamp keeps replicating to followers.
    it('archive-head version set is [1, 6] via the shared stateHash constant, consumed by updatedRows', function(){
        const assertLocal = require('assert');
        const sh = require('../../src/stateHash');
        assertLocal.deepStrictEqual(sh.ARCHIVE_HEAD_VERSIONS, [1, 6],
            'ARCHIVE_HEAD_VERSIONS must be exactly [1, 6]');
        assertLocal.strictEqual(sh.ARCHIVE_HEAD_VERSIONS_SQL, 'IN (1, 6)',
            'ARCHIVE_HEAD_VERSIONS_SQL must render as IN (1, 6)');
        const fs = require('fs'), pathMod = require('path');
        const norm = s => s.replace(/[`"']/g, ' ').replace(/\s+\+\s+/g, ' ').replace(/\s+/g, ' ');
        const ur = norm(fs.readFileSync(pathMod.resolve(__dirname, '../../src/updatedRows.js'), 'utf8'));
        assertLocal.ok(/WHERE p\.version ARCHIVE_HEAD_VERSIONS_SQL AND ARCHIVE_CHUNK_HEIGHT_COL BETWEEN \? AND \?/.test(ur),
            'updatedRows.js anchor class must select archive-head parents via ARCHIVE_HEAD_VERSIONS_SQL, ' +
            'scoped by the shared ARCHIVE_CHUNK_HEIGHT_COL');
        // The completing chunk's height key is the shared constant, never a literal
        // `c.block_index`: that column is NULL on every v2 continuation row, so the
        // class shipped ZERO rows and a follower never received the stamped parent.
        assertLocal.strictEqual(sh.ARCHIVE_CHUNK_HEIGHT_COL, 'c.block_index_doge',
            'ARCHIVE_CHUNK_HEIGHT_COL must be c.block_index_doge (block_index is NULL on v2 chunks)');
        assertLocal.ok(!/AND c\.block_index BETWEEN/.test(ur),
            'updatedRows.js must not regress to the never-populated c.block_index key');
        // Twin-parity: the indexer stateHash.js copy (when the sibling checkout exists)
        // must carry the identical constant, or the two repos disagree on the parent set.
        const indexerPath = indexerFile('src/stateHash.js');
        if(fs.existsSync(indexerPath)){
            const ish = require(indexerPath);
            assertLocal.deepStrictEqual(ish.ARCHIVE_HEAD_VERSIONS, sh.ARCHIVE_HEAD_VERSIONS,
                'indexer and sync ARCHIVE_HEAD_VERSIONS must be identical');
        }
    });

    // Bespoke-logic parity: VOTE polls re-open reset. An orphaned VOTE v2 finalization
    // flipped a surviving polls row terminal IN PLACE; the generic action_index delete
    // drops the v2's poll_results rows but cannot re-open the poll. Both rollback.js
    // (source) and ClientRollback.js (replica) must carry the reset UPDATE keyed on
    // resolved_block, or a reorged replica keeps serving a terminal poll the source
    // re-opened. If you change one side, change the other and extend this guard.
    it('VOTE polls re-open reset is mirrored across xchain-indexer and xchain-sync (bespoke-logic drift guard)', function(){
        const fs = require('fs'), pathMod = require('path');
        const syncPath    = pathMod.resolve(__dirname, '../../src/ClientRollback.js');
        const indexerPath = indexerFile('src/rollback.js');
        if(!requireSibling(this, indexerPath)) return;
        const norm = s => s.replace(/[`"']/g, ' ').replace(/\s+\+\s+/g, ' ').replace(/\s+/g, ' ');
        // The FULL re-open column list is pinned, not a prefix: a prefix match let the
        // replica drop callback_due_block = NULL (and miss the timelock reset entirely)
        // while this guard stayed green (#5607).
        const POLL_OPS = [
            { name: 'summary re-open (full column list)', re: /UPDATE polls SET poll_status = open , winning_option = NULL, total_weight = NULL, total_voters = NULL, quorum_met = NULL, min_voters_met = NULL, fail_reason = NULL, decided_early = NULL, effective_close_block = NULL, finalized_action_index = NULL, resolved_block = NULL, deposit_resolved = NULL, callback_execute_action_index = NULL, callback_due_block = NULL WHERE poll_status IN \( finalized , failed_quorum \) AND resolved_block >= \?/ },
            { name: 'timelock re-fire reset (orphaned due block, surviving finalization)', re: /UPDATE polls SET callback_execute_action_index = NULL WHERE poll_status IN \( finalized , failed_quorum \) AND callback_due_block >= \? AND callback_execute_action_index IS NOT NULL/ },
        ];
        for(const [label, p] of [['ClientRollback.js (replica)', syncPath], ['rollback.js (source)', indexerPath]]){
            const src = norm(fs.readFileSync(p, 'utf8'));
            for(const op of POLL_OPS){
                assert.ok(op.re.test(src), `${label} is missing the polls ${op.name}; source and replica must both re-open finalized polls on reorg`);
            }
        }
    });

    // Forward parity for the polls finalization flip: the reverse re-open above needs a
    // forward twin or a follower never learns a poll went terminal (the flip mutates a
    // surviving row the action-scoped stream cannot reach). Pin the class table list and
    // its resolved_block window predicate in updatedRows.js.
    it('updated_rows carries the VOTE poll finalization flip keyed by resolved_block', function(){
        const { POLL_FINALIZE_TABLES } = require('../../src/updatedRows');
        assert.deepStrictEqual(POLL_FINALIZE_TABLES, ['polls'],
            'updated_rows must track the poll finalization flip on polls');
        const fs = require('fs'), pathMod = require('path');
        const src = fs.readFileSync(pathMod.resolve(__dirname, '../../src/updatedRows.js'), 'utf8')
            .replace(/[`"']/g, ' ').replace(/\s+\+\s+/g, ' ').replace(/\s+/g, ' ');
        assert.ok(/WHERE resolved_block BETWEEN \? AND \?/.test(src),
            'updatedRows.js must select the poll finalization flip by resolved_block (the same key the reverse re-open resets)');
        // Second key: the deferred binding-callback fire stamp lands at the due block
        // (above the finalize window), the forward twin of the timelock re-fire reset.
        assert.ok(/OR \(callback_due_block BETWEEN \? AND \? AND callback_execute_action_index IS NOT NULL\)/.test(src),
            'updatedRows.js must also select polls by callback_due_block with a fired stamp (the deferred callback fire is an in-place UPDATE at the due block)');
    });

    // The state_hash (replication-integrity 4th hash) is computed on BOTH sides from
    // src/stateHash.js: the indexer stores it at index-time, the follower recomputes it
    // apply-time and halts on mismatch. The two copies are a byte-aligned twin (separate
    // npm packages, no cross-dep); any drift silently turns a divergence detector into a
    // false-halt generator. Lock them identical (skip if the sibling indexer repo absent).
    it('stateHash.js is byte-identical across xchain-sync and xchain-indexer (cross-repo twin)', function(){
        const fs = require('fs'), pathMod = require('path');
        const syncPath    = pathMod.resolve(__dirname, '../../src/stateHash.js');
        const indexerPath = indexerFile('src/stateHash.js');
        if(!requireSibling(this, indexerPath)) return;
        assert.strictEqual(fs.readFileSync(syncPath, 'utf8'), fs.readFileSync(indexerPath, 'utf8'),
            'stateHash.js drifted between xchain-sync and xchain-indexer; keep the twin byte-identical');
    });

    // ServerPoller must stream a block's FULL index_addresses set, not just the addresses
    // that appear as a tx source/destination. An address first receives its deterministic
    // in-block id via many non-tx columns too (credits.address_id, contract_executions.caller_id,
    // injected cross-chain counterparties, action-data recipients). The explicit join in
    // _buildBlockPayload only sees tx source/dest, so completeness depends on the generic
    // *_id reference pass ALSO covering index_addresses. If index_addresses is excluded from
    // that pass, a non-tx-interned address is never delivered at its intern block: the follower's
    // index map forks (a reorg crossing such an address fail-closed halts it today, and a
    // per-block halt once the index-map state_hash class is armed). index_transactions stays
    // excluded (it is keyed by block-hash/tx-hash ids the generic scan can't see).
    it('ServerPoller streams index_addresses via the generic *_id pass (non-tx-interned completeness) @regression', function(){
        const fs = require('fs'), pathMod = require('path');
        const norm = fs.readFileSync(pathMod.resolve(__dirname, '../../src/ServerPoller.js'), 'utf8')
            .replace(/[`"']/g, ' ').replace(/\s+/g, ' ');
        assert.ok(/table === index_transactions\s*\) continue/.test(norm),
            'ServerPoller generic *_id pass must still exclude index_transactions (block-hash/tx-hash keyed)');
        assert.ok(/table !== index_addresses && payload\.data\[table\]\s*\) continue/.test(norm),
            'ServerPoller generic *_id pass must let index_addresses past the already-populated skip so the ' +
            'superset re-fetch delivers non-tx-interned addresses; do not re-add it to the exclusion list');
    });

    // Light-client state commitment (SPV spec sec.4-5): merkle.js (the SMT + leaf
    // encoders) and state_commitment_activation.js (the flag-day map) are copied
    // VERBATIM from xchain-indexer. The follower recomputes the per-block roots from
    // these and HALTs on divergence, so any drift turns the divergence detector into
    // a false-halt generator. tableLifecycle.js is the table-lifecycle registry that
    // GENERATES the replicated topology and both rollback table sets; a drifted copy
    // would silently re-open the very source<->replica divergence it exists to close.
    // Lock them byte-identical (skip if the sibling repo absent).
    // state_key_collation_activation.js gates the state_key COLLATE in the
    // contract_hash/block_merkle_root preimage on BOTH sides; a drifted copy
    // forks the recomputed hashes at/after an armed height.
    // state_subtree_activation.js is the flag-day gate for the RESERVED state_root
    // slots (ownership/tokens/contract_state) and for the balances_root escrow leaf.
    // It is inert today, but it decides on BOTH sides which sub-roots stop being
    // EMPTY at an armed height; a drifted copy forks state_root the moment a slot
    // arms, or halts every follower before it.
    // contractStateSubtree.js is the contract_state_root DERIVATION (SPV sub-tree
    // spec §3 Stage A): the row-to-leaf mapping, the touched-key query and the
    // incremental-vs-full-build decision. It deliberately owns no SMT engine and
    // no db handle so it CAN be byte-identical, because the source and the
    // follower must answer "which leaves does this block commit" with the same
    // bytes: the follower recomputes and HALTs on divergence, so drift here is a
    // fleet halt at the first armed block rather than a subtle fork.
    for(const twin of ['merkle.js', 'state_commitment_activation.js', 'swq_source_cap_activation.js', 'state_key_collation_activation.js', 'state_subtree_activation.js', 'contractStateSubtree.js', 'escrowLeafSubtree.js', 'tableLifecycle.js']){
        it(twin + ' is byte-identical across xchain-sync and xchain-indexer (cross-repo twin)', function(){
            const fs = require('fs'), pathMod = require('path');
            const syncPath    = pathMod.resolve(__dirname, '../../src/' + twin);
            const indexerPath = indexerFile('src/' + twin);
            if(!requireSibling(this, indexerPath)) return;
            assert.strictEqual(fs.readFileSync(syncPath, 'utf8'), fs.readFileSync(indexerPath, 'utf8'),
                twin + ' drifted between xchain-sync and xchain-indexer; keep the twin byte-identical');
        });
    }

    // The gate's own conformance suite is a twin too: it is what proves the carrier
    // is inert (identical state_root to the two-sub-root v1 assembly) and that the
    // slot list matches merkle.STATE_SUBTREES. Both repos must assert the same
    // thing, or one side can land an arming change the other never checked.
    // state_subtree_activation.js has a THIRD carrier: xchain-sdk ships it as a
    // client-facing consensus constant, because no proof can tell a client whether
    // a slot is live (an armed-but-empty slot and an inert slot commit the same
    // EMPTY_SMT_ROOT). The loop above only pairs sync with the indexer, so the SDK
    // copy is checked here as well; xchain-sdk carries its own copy of this guard
    // plus a golden pin for standalone checkouts.
    it('state_subtree_activation.js is byte-identical in xchain-sdk too (client liveness export)', function(){
        const fs = require('fs'), pathMod = require('path');
        const sdkPath = pathMod.resolve(__dirname, '..', '..', '..', 'xchain-sdk/src/state_subtree_activation.js');
        if(!requireSibling(this, sdkPath)) return;
        assert.strictEqual(fs.readFileSync(pathMod.resolve(__dirname, '../../src/state_subtree_activation.js'), 'utf8'),
                           fs.readFileSync(sdkPath, 'utf8'),
            'the SDK activation copy drifted; a client would read different armed heights than the fleet commits');
    });

    // And a FOURTH carrier: the explorer, whose locked-balance proof endpoint
    // refuses below the escrow leaf's armed height (Stage B2). Reserved
    // slots need no gate there (their stored column carries the armed decision
    // per height), but the escrow leaf lives inside balances_root with no stored
    // signal, so the refusal requires the map itself. A drifted explorer copy
    // either serves meaningless absence proofs early (the §4 hazard the refusal
    // exists for; the SDK verifier's own copy still protects conforming clients)
    // or refuses real proofs late (an availability gap).
    it('state_subtree_activation.js is byte-identical in xchain-explorer too (escrow-leaf liveness refusal)', function(){
        const fs = require('fs'), pathMod = require('path');
        const expPath = pathMod.resolve(__dirname, '..', '..', '..', 'xchain-explorer/src/state_subtree_activation.js');
        if(!requireSibling(this, expPath)) return;
        assert.strictEqual(fs.readFileSync(pathMod.resolve(__dirname, '../../src/state_subtree_activation.js'), 'utf8'),
                           fs.readFileSync(expPath, 'utf8'),
            'the explorer activation copy drifted; its escrow-leaf proof refusal boundary would disagree with the fleet');
    });

    for(const twin of ['stateSubtreeActivation.test.js', 'contractStateSubtree.test.js', 'escrowLeafSubtree.test.js']){
        it(twin + ' is byte-identical across xchain-sync and xchain-indexer (cross-repo twin)', function(){
            const fs = require('fs'), pathMod = require('path');
            const syncPath    = pathMod.resolve(__dirname, '../../test/unit/' + twin);
            const indexerPath = indexerFile('test/unit/' + twin);
            if(!requireSibling(this, indexerPath)) return;
            assert.strictEqual(fs.readFileSync(syncPath, 'utf8'), fs.readFileSync(indexerPath, 'utf8'),
                twin + ' drifted between xchain-sync and xchain-indexer; keep the twin byte-identical');
        });
    }

    // The state_hash selection must mirror the SAME mutation classes the updated_rows +
    // cooldownCredits channels carry (and that ClientRollback reverses), keyed on the same
    // block columns, or the integrity hash covers a different row set than it protects.
    it('stateHash.js selection predicates mirror the replicated mutation classes', function(){
        const fs = require('fs'), pathMod = require('path');
        const src = fs.readFileSync(pathMod.resolve(__dirname, '../../src/stateHash.js'), 'utf8')
            .replace(/[`"']/g, ' ').replace(/\s+\+\s+/g, ' ').replace(/\s+/g, ' ');
        const PREDICATES = [
            { name: 'deactivation_block stamp',  re: /WHERE deactivation_block BETWEEN \? AND \?/ },
            { name: 'SLASH debit-log join',      re: /JOIN .* d ON d\.stake_action_index = t\.action_index/ },
            { name: 'v0 request_status flip',    re: /WHERE version = 0 AND resolved_block BETWEEN \? AND \?/ },
            { name: 'cooldown status by maturity block', re: /WHERE t\.cooldown_end_block BETWEEN \? AND \?/ },
            { name: 'capability GAS refund credit',      re: /JOIN unstakes u ON \(u\.action_index = c\.action_index/ },
            { name: 'contract own-tick refund credit',   re: /JOIN contract_unstakes cu ON \(cu\.action_index = c\.action_index/ },
            // Poll-finalize flip (flag-day gated): same resolved_block key the
            // POLL_FINALIZE_TABLES forward channel and the rollback re-open use.
            { name: 'poll finalization flip',    re: /FROM polls WHERE resolved_block BETWEEN \? AND \? ORDER BY action_index ASC/ },
            // Token-supply refresh (flag-day gated, F-1 closure): same
            // ledger-touched-tick selection shape as the forward tokens class.
            { name: 'token supply refresh',      re: /SELECT tk\.tick AS tick, t\.supply AS supply FROM tokens t/ },
        ];
        for(const p of PREDICATES){
            assert.ok(p.re.test(src), `stateHash.js is missing the ${p.name} selection; its hash would not cover that replicated mutation class`);
        }
        // The mid-chain-armed classes (poll_finalize / token_supply) are gated by
        // per-chain '<COIN>:<network>' keys, so the follower's recompute MUST
        // thread coin alongside network or it computes without the armed classes
        // while the source computes with them (guaranteed halt at the height).
        const bh = fs.readFileSync(pathMod.resolve(__dirname, '../../src/BlockHasher.js'), 'utf8');
        assert.ok(/computeStateHash\(block_index, activationDelay, gasTick, network, coin\)/.test(bh),
            'BlockHasher.computeStateHash must accept and forward the coin gate parameter');
        // Threading a coin is necessary but NOT sufficient - the FORMAT must
        // match the source. The activation maps are keyed '<TICKER>:<network>' exactly
        // as the source indexer writes it (its db.js passes config['COIN'], a ticker),
        // but the sync layer names a chain by cfg.coin = the FULL LOWERCASE NAME
        // ('litecoin'). This guard previously pinned `this.chain`, which threaded the
        // full name, missed the per-chain key, and silently computed WITHOUT the armed
        // classes: the "guaranteed halt at the height" predicted above actually
        // happened on every production replica (BTC 958500 / LTC 3143000 / DOGE 6291000
        // / BTC-testnet 145000, each chain's armed height). Pin the normalized ticker.
        const cs = fs.readFileSync(pathMod.resolve(__dirname, '../../src/ClientSync.js'), 'utf8')
            .replace(/\s+/g, ' ');
        assert.ok(/computeStateHash\( ?event\.block_index,.*?this\.network, this\.coinTicker\)/.test(cs),
            'ClientSync must pass this.coinTicker (the normalized TICKER, not this.chain) as the coin gate parameter to computeStateHash');
    });

    // Value-level fixture assertions (F-1, F-2, F-5).
    // The structural membership guard above catches new tables that are replicated but
    // not handled. These tests go one level deeper: they exercise the actual runtime
    // predicates against canned fixtures so a wrong predicate (wrong column, wrong join
    // key, off-by-one range) fails here rather than silently shipping a divergent replica.

    // F-1: tokens.supply is an in-place mutation on SURVIVING rows (ISSUE/DESTROY
    // increment/decrement supply on a token created by an earlier action). It is
    // deliberately NOT in the named mutation-class arrays: it rides its own
    // ledger-driven pass in collectUpdatedRows (ticks touched by credits/debits/
    // escrows in the window), and since 2026-07-07 that pass has a state_hash twin
    // (the flag-day-gated token_supply class in stateHash.js), closing the original
    // F-1 hash gap. Pin the array design by value: a table added to one of these
    // arrays is a deliberate, visible change, not drift.
    it('F-1: tokens is not in any updatedRows mutation-class array (supply rides the ledger-driven pass + token_supply hash class)', function(){
        const { DEACTIVATION_TABLES, SLASH_SPECS, REQUEST_STATUS_TABLES, COOLDOWN_STATUS_TABLES } = require('../../src/updatedRows');
        const allMutationTables = new Set([
            ...DEACTIVATION_TABLES,
            ...SLASH_SPECS.map(s => s.table),
            ...REQUEST_STATUS_TABLES,
            ...COOLDOWN_STATUS_TABLES,
        ]);
        assert.ok(
            !allMutationTables.has('tokens'),
            'tokens is now in an updatedRows mutation-class array; update the supply-tracking ' +
            'channel description and remove this assertion, or revert the unintended addition'
        );
        // Anchor the exact mutation-class tables so any new class is a deliberate, visible change.
        assert.deepStrictEqual(DEACTIVATION_TABLES.slice().sort(), ['contract_delegations', 'contract_stakes', 'delegations', 'stakes'],
            'DEACTIVATION_TABLES value changed; update the supply-forward gap commentary if intentional');
        assert.deepStrictEqual(REQUEST_STATUS_TABLES.slice().sort(), ['attests', 'xcalls'],
            'REQUEST_STATUS_TABLES value changed unexpectedly');
        assert.deepStrictEqual(COOLDOWN_STATUS_TABLES.slice().sort(), ['contract_unstakes', 'unstakes'],
            'COOLDOWN_STATUS_TABLES value changed unexpectedly');
    });

    // F-2: anchor CRC-failure fixture (value, not regex). When the final v2 chunk of a
    // chunked archive batch lands in the window and the reassembled blob fails its CRC
    // check, anchor.js stamps the v1 parent 'invalid_archive' IN PLACE (a surviving row
    // in an earlier block). collectUpdatedRows must return that parent by value so the
    // follower upserts the updated status_id. buildStateHashData must include it in the
    // anchor_invalid preimage class so a follower that silently drops the upsert halts.
    it('F-2: collectUpdatedRows returns the invalid_archive-stamped anchor parent by value (CRC-failure fixture)', async function(){
        const { collectUpdatedRows } = require('../../src/updatedRows');
        // Fake DB: the anchor self-join query returns a v1 parent stamped invalid_archive.
        let anchorParent = { action_index: 301, version: 1, status_id: 99, match_batch_seq: 7 };
        let db = {
            dbType: 'indexer',
            doQuery: async (sql) => {
                if(sql.indexOf('anchor_actions p') !== -1) return [anchorParent];
                return [];
            }
        };
        let out = await collectUpdatedRows(db, 300, 300, null);
        assert.ok(Array.isArray(out.anchor_actions) && out.anchor_actions.length === 1,
            'collectUpdatedRows must return the anchor_actions parent row for the CRC-failure window');
        assert.strictEqual(out.anchor_actions[0].action_index, 301,
            'anchor_actions result must carry the correct action_index');
        assert.strictEqual(out.anchor_actions[0].status_id, 99,
            'anchor_actions result must carry the current (invalid_archive) status_id so the follower upsert refreshes it');
    });

    it('F-2: buildStateHashData includes the anchor CRC-failure parent in the anchor_invalid preimage class (value fixture)', async function(){
        const { buildStateHashData } = require('../../src/stateHash');
        let anchor = { action_index: 301, status: 'invalid_archive' };
        let db = {
            doQuery: async (sql) => {
                if(sql.indexOf('anchor_actions p') !== -1) return [anchor];
                return [];
            },
            getStatusId: async () => 5
        };
        const data = await buildStateHashData(db, 300, { activationDelay: 6, gasTick: 'GAS' });
        assert.ok(Array.isArray(data.anchor_invalid) && data.anchor_invalid.length === 1,
            'state_hash preimage must include the anchor_invalid class row for the CRC-failure block');
        assert.strictEqual(data.anchor_invalid[0].action_index, 301,
            'anchor_invalid preimage entry must carry the correct action_index');
        assert.strictEqual(data.anchor_invalid[0].status, 'invalid_archive',
            'anchor_invalid preimage entry must carry the resolved status string, not a raw status_id');
    });

    it('F-2: buildStateHashData includes cooldown-maturity refund credit in credits class (maturity fixture)', async function(){
        const { buildStateHashData } = require('../../src/stateHash');
        let credit = { action_index: 77, address: 'bc1qtest', tick: 'GAS', amount: '1000' };
        let db = {
            doQuery: async (sql) => {
                if(sql.indexOf('FROM credits c') !== -1) return [credit];
                return [];
            },
            getStatusId: async () => 5
        };
        const data = await buildStateHashData(db, 200, { activationDelay: 6, gasTick: 'GAS' });
        assert.ok(Array.isArray(data.credits) && data.credits.length === 1,
            'state_hash preimage must include backdated cooldown-maturity refund credits');
        assert.strictEqual(data.credits[0].action_index, 77,
            'credits preimage entry must carry the correct (backdated) action_index');
        assert.strictEqual(data.credits[0].address, 'bc1qtest',
            'credits preimage entry must carry the resolved address string, not address_id');
    });

    // F-5: snapshot-inclusion/exclusion contract. The five hub-mirrored tables
    // (oracle_prices, capability_snapshots, cross_chain_calls, cross_chain_matches,
    // state_checkpoints) are intentionally excluded from both the per-block replicated
    // set AND from consensus snapshots. If one is accidentally re-admitted to either
    // channel, followers would apply divergent hub-timing data as if it were
    // block-deterministic, silently forking the replica. Pin the exclusion by value.
    it('F-5: hub-mirrored tables are absent from the ServerPoller replicated universe (snapshot-exclusion contract)', function(){
        // Derived from the registry, not a hand-copied literal, so a new
        // hub-mirror entry is covered automatically.
        const HUB_MIRRORED = lifecycleTwin.tablesWhere(t => t.replication === 'hub-mirror');
        assert.ok(HUB_MIRRORED.length >= 5, 'expected at least the five known hub-mirrored tables in the registry');
        const { universe } = replicatedTables('indexer');
        const universeSet = new Set(universe);
        for(const t of HUB_MIRRORED){
            assert.ok(
                !universeSet.has(t),
                `${t} appeared in the ServerPoller indexer replicated set; hub-mirrored tables ` +
                `must not ride the per-block stream (they diverge by hub-WS arrival timing, not block determinism)`
            );
        }
    });

    it('F-5: OPERATOR_LOCAL_TABLES equals the registry-derived exclusion set plus the three permitted non-registry names', function(){
        // Both directions. Forward: every registry table whose
        // replication mode is local / hub-mirror / follower-derived must be
        // snapshot-excluded (a new local table that rode consensus snapshots
        // would ship divergent per-node state to every follower). Reverse:
        // OPERATOR_LOCAL_TABLES may contain NOTHING else beyond the three
        // sanctioned non-registry names, so over-exclusion (silently dropping
        // a consensus table from snapshots) fails just as loudly. Uses the
        // exported Set, not a source-text scrape, so it pins the value the
        // runtime consumers (SnapshotBuilder, ClientApplier, explorer) see.
        const { OPERATOR_LOCAL_TABLES } = require('../../src/SnapshotBuilder');
        const derived = lifecycleTwin.tablesWhere(t =>
            ['local', 'hub-mirror', 'follower-derived'].includes(t.replication));
        // The ONLY permitted members with no registry entry: mempool_transactions is a
        // decoder-DB table (registry covers the indexer DB); sync_halt / sync_state are
        // replica-created control tables the full-snapshot clear loop must never wipe.
        const NON_REGISTRY = ['mempool_transactions', 'sync_halt', 'sync_state'];
        const expected = new Set([...derived, ...NON_REGISTRY]);

        for(const t of expected){
            assert.ok(OPERATOR_LOCAL_TABLES.has(t),
                `${t} is missing from OPERATOR_LOCAL_TABLES; a registry table marked ` +
                `local/hub-mirror/follower-derived must not ride consensus snapshots`);
        }
        for(const t of OPERATOR_LOCAL_TABLES){
            assert.ok(expected.has(t),
                `${t} is in OPERATOR_LOCAL_TABLES but is neither registry-derived nor a ` +
                `sanctioned non-registry name; removing a table from consensus snapshots ` +
                `is a consensus-visible change that needs a registry replication label`);
        }
        // Belt-and-suspenders: the two historically-uncovered members stay pinned by name.
        for(const t of ['recovery_pending_rewards', 'state_tree_roots']){
            assert.ok(OPERATOR_LOCAL_TABLES.has(t), `${t} dropped out of OPERATOR_LOCAL_TABLES`);
        }
    });
});
