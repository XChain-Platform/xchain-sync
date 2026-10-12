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
 *
 * XChain Indexer Sync - Block Hasher (independent recomputation)
 *
 * Recomputes a block's chained consensus hashes (ledger / actions /
 * contract) from the RAW ROWS this validator replicated, rather than
 * trusting the committed hash a source published. Comparing the recomputed
 * hash to the committed hash detects a replica whose data does NOT match
 * the hash that was committed for it: replication corruption, a partial /
 * truncated apply, or a source serving rows inconsistent with its own
 * committed hash. (It composes with HashVerifier's cross-source check, which
 * catches two internally-consistent-but-divergent honest sources; neither
 * subsumes the other.)
 *
 * !!! CONSENSUS CONFORMANCE PAIR !!!
 * This is a byte-for-byte port of xchain-indexer/src/db/actions.js getBlockHashes()
 * (+ utility.js getDataHash, reused here via the same conformance copy in
 * xchain-sync/src/utility.js). The two MUST stay identical: same SELECT
 * column sets, same ORDER BY, same object key-insertion order, the same
 * array-with-props quirk for `actions`, and the same previous-block chaining.
 * ANY change to the indexer's hash inputs MUST be mirrored here, in BOTH the call-site
 * template and its BLOCK_HASH_QUERIES entry (consensusSql throws on a mismatch), and the
 * test/fixtures/block-hash-vectors.json regenerated, but only once
 * test/unit/blockhash_conformance_twin.test.js passes: that golden is a sync
 * self-lock, so never regenerate it to clear a failure. The xchain-e2e-test
 * recompute-conformance scenario (indexer + sync on the regtest stack) is the
 * live drift guard.
 *
 ********************************************************************/

// Consensus block-hash scheme version. MUST stay identical to
// xchain-indexer/src/db/shared.js BLOCK_HASH_VERSION; see the conformance-pair banner above.
// The scheme hashes the resolved canonical strings (address/tick/action/status) rather
// than raw AUTO_INCREMENT lookup ids (which diverge across nodes after a reorg); it is
// id-independent. This resolved-string scheme is the only one that has shipped: version 1.
// Bumping it is a consensus break requiring a coordinated validator checkpoint re-baseline.
const BLOCK_HASH_VERSION = 1;

// Default block/row span of the advisory content-parity window when the caller
// passes none. Big enough that a periodic verification pass covers the blocks
// applied since the last one, small enough that turning the check on never reads a
// table's whole history. The SOURCE's value is the one that counts: it publishes the
// window it used and the follower recomputes over that, so the two sides cannot
// compare different spans.
const DEFAULT_CONTENT_PARITY_WINDOW = 100;

const replicatedTables = require('../schema/replicated_tables');
const lifecycle = require('../table_lifecycle');
const tokenRefold = require('../db/token_refold');
const { buildStateHashData } = require('../consensus/state_hash');
const { gasTickSymbol } = require('../consensus-constants');
const { canonicalizeHashAddress } = require('../util/protocol_address_roles');
// The state-key binary collation flag day is a registry row read by literal key (W5),
// keyed '<COIN>:<network>' so the coin goes with the height.
const gateRegistry = require('../consensus/gate_registry');
const STATE_KEY_COLLATION_KEY = 'state_key_collation_activation.STATE_KEY_COLLATION_ACTIVATION';

// Preserve the database-facing query text while compact literals keep the gather helpers
// within the function-size limit. The normalized key is used only to recover its original
// layout, so query order and values remain byte-identical to the unsplit implementation.
function contractStateHashQuery(stateKeyCollate){
    return 'SELECT cs.contract_index, cs.state_key, cs.state_value\n                 FROM contract_state cs\n                 INNER JOIN (\n                     SELECT MAX(id) as max_id\n                     FROM contract_state\n                     WHERE block_index=?\n                     GROUP BY contract_index, state_key' + stateKeyCollate + '\n                 ) latest ON cs.id = latest.max_id\n                 ORDER BY cs.contract_index ASC, cs.state_key' + stateKeyCollate + ' ASC';
}

const BLOCK_HASH_QUERIES = [
    'SELECT\n                    c.action_index,\n                    a1.address AS address,\n                    t1.tick    AS tick,\n                    c.amount\n                FROM\n                    credits c\n                    INNER JOIN actions        a  ON (a.action_index=c.action_index)                    LEFT  JOIN index_addresses a1 ON (a1.id=c.address_id)\n                    LEFT  JOIN index_tickers   t1 ON (t1.id=c.tick_id)\n                WHERE\n                    a.block_index=?\n                ORDER BY\n                    c.action_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, c.amount ASC',
    'SELECT\n                    d.action_index,\n                    a1.address AS address,\n                    t1.tick    AS tick,\n                    d.amount\n                FROM\n                    debits d\n                    INNER JOIN actions        a  ON (a.action_index=d.action_index)                    LEFT  JOIN index_addresses a1 ON (a1.id=d.address_id)\n                    LEFT  JOIN index_tickers   t1 ON (t1.id=d.tick_id)\n                WHERE\n                    a.block_index=?\n                ORDER BY\n                    d.action_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, d.amount ASC',
    'SELECT\n                    e.action_index,\n                    a1.address AS address,\n                    t1.tick    AS tick,\n                    e.amount\n                FROM\n                    escrows e\n                    INNER JOIN actions        a  ON (a.action_index=e.action_index)                    LEFT  JOIN index_addresses a1 ON (a1.id=e.address_id)\n                    LEFT  JOIN index_tickers   t1 ON (t1.id=e.tick_id)\n                WHERE\n                    a.block_index=?\n                ORDER BY\n                    e.action_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, e.amount ASC',
    'SELECT\n                    a.action_index,\n                    a.tx_index,\n                    ia.action AS action\n                FROM\n                    actions a                    LEFT  JOIN index_actions ia ON (ia.id=a.action_id)\n                WHERE\n                    a.block_index=?\n                ORDER BY\n                    a.action_index ASC',
    'SELECT c.action_index, a1.address AS source_address, c.code_hash, s1.status AS status\n                 FROM contracts c\n                 INNER JOIN actions a ON (a.action_index=c.action_index)                 LEFT  JOIN index_addresses a1 ON (a1.id=c.source_id)\n                 LEFT  JOIN index_statuses  s1 ON (s1.id=c.status_id)\n                 WHERE a.block_index=?\n                 ORDER BY c.action_index ASC',
    contractStateHashQuery(''),
    contractStateHashQuery(' COLLATE utf8_bin'),
    'SELECT ce.action_index, ce.contract_index, a1.address AS caller_address, ce.gas_used, s1.status AS status, ce.emitted_count\n                 FROM contract_executions ce\n                 INNER JOIN actions a ON (a.action_index=ce.action_index)                 LEFT  JOIN index_addresses a1 ON (a1.id=ce.caller_id)\n                 LEFT  JOIN index_statuses  s1 ON (s1.id=ce.status_id)\n                 WHERE a.block_index=?\n                 ORDER BY ce.action_index ASC',
    'SELECT em.execution_index, em.emitted_action, em.action_index, em.position\n                 FROM contract_emissions em\n                 INNER JOIN contract_executions ce ON (ce.action_index=em.execution_index)\n                 INNER JOIN actions a ON (a.action_index=ce.action_index)\n                 WHERE a.block_index=?\n                 ORDER BY em.execution_index ASC, em.position ASC',
    'SELECT d.action_index, d.contract_index, a1.address AS source_address, t1.tick AS tick, d.amount, s1.status AS status\n                 FROM deposits d\n                 INNER JOIN actions a ON (a.action_index=d.action_index)                 LEFT  JOIN index_addresses a1 ON (a1.id=d.source_id)\n                 LEFT  JOIN index_tickers   t1 ON (t1.id=d.tick_id)\n                 LEFT  JOIN index_statuses  s1 ON (s1.id=d.status_id)\n                 WHERE a.block_index=?\n                 ORDER BY d.action_index ASC, d.contract_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, d.amount ASC, s1.status COLLATE utf8_bin ASC',
    'SELECT w.action_index, w.contract_index, a1.address AS source_address, t1.tick AS tick, w.amount, s1.status AS status\n                 FROM withdrawals w\n                 INNER JOIN actions a ON (a.action_index=w.action_index)                 LEFT  JOIN index_addresses a1 ON (a1.id=w.source_id)\n                 LEFT  JOIN index_tickers   t1 ON (t1.id=w.tick_id)\n                 LEFT  JOIN index_statuses  s1 ON (s1.id=w.status_id)\n                 WHERE a.block_index=?\n                 ORDER BY w.action_index ASC, w.contract_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, w.amount ASC, s1.status COLLATE utf8_bin ASC',
    'SELECT\n                t1.hash as ledger,\n                t2.hash as actions,\n                t3.hash as contracts\n            FROM\n                blocks b\n                LEFT JOIN index_transactions t1 ON (t1.id=b.ledger_hash_id)\n                LEFT JOIN index_transactions t2 ON (t2.id=b.actions_hash_id)\n                LEFT JOIN index_transactions t3 ON (t3.id=b.contract_hash_id)\n            WHERE\n                b.block_index=?'
];
const BLOCK_HASH_QUERY_BY_SHAPE = new Map(BLOCK_HASH_QUERIES.map(query => [query.replace(/\s+/g, ' ').trim(), query]));

// Fail CLOSED on a shape with no BLOCK_HASH_QUERIES entry: an undefined query reaches
// doQuery's null guard and comes back as [], which would hash a truncated preimage.
function consensusSql(strings, ...values){
    let query = strings;
    if(typeof strings !== 'string'){
        query = strings[0];
        for(let i = 0; i < values.length; i++) query += values[i] + strings[i + 1];
    }
    let shape = query.replace(/\s+/g, ' ').trim();
    let resolved = BLOCK_HASH_QUERY_BY_SHAPE.get(shape);
    if(resolved === undefined) throw new Error('consensusSql: no BLOCK_HASH_QUERIES entry for query shape: ' + shape);
    return resolved;
}

class BlockHasher {

    // db:   a DB handle exposing async doQuery(sql, params) and
    //       doQueryStrict(sql, params) against the REPLICA (schema-identical to
    //       the indexer, with surrogate ids preserved).
    // util: xchain-sync Utility; its getDataHash() is the conformance copy of
    //       the indexer's (JSON.stringify(Object.assign({}, data), bigint->string),
    //       SHA-256 hex).
    constructor(db, util){
        this.db   = db;
        this.util = util;
    }

    // Recompute { ledger_hash, actions_hash, contract_hash } for a block from the
    // replicated raw rows. Mirrors xchain-indexer/src/db/actions.js getBlockHashes().
    // `network`/`coin` drive the state_key collation flag-day
    // (the state_key_collation_activation row in consensus/gate_registry/shared_rows_4.js,
    // byte-identical twin of the indexer's src/protocol_changes/shared_rows_4.js);
    // omitted -> legacy folding collation, matching pre-activation blocks. Live
    // recompute callers MUST pass them or the replica gates differently than the
    // source at/after an armed height and false-halts on divergence.
    // Gather every preimage row set with doQueryStrict: a swallowed read error
    // returning [] would hash a truncated preimage and halt on a false divergence
    // instead of surfacing as a recompute error (same rule as db/actions.js).
    async computeBlockHashes(block_index, network, coin){
        async function getLedgerHashData(height){
            const ledger = { credits: [], debits: [], escrows: [] };
            ledger.credits = await this.db.doQueryStrict(consensusSql`SELECT c.action_index, a1.address AS address, t1.tick AS tick, c.amount FROM credits c INNER JOIN actions a ON (a.action_index=c.action_index) LEFT JOIN index_addresses a1 ON (a1.id=c.address_id) LEFT JOIN index_tickers t1 ON (t1.id=c.tick_id) WHERE a.block_index=? ORDER BY c.action_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, c.amount ASC`, [height]);
            ledger.debits = await this.db.doQueryStrict(consensusSql`SELECT d.action_index, a1.address AS address, t1.tick AS tick, d.amount FROM debits d INNER JOIN actions a ON (a.action_index=d.action_index) LEFT JOIN index_addresses a1 ON (a1.id=d.address_id) LEFT JOIN index_tickers t1 ON (t1.id=d.tick_id) WHERE a.block_index=? ORDER BY d.action_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, d.amount ASC`, [height]);
            ledger.escrows = await this.db.doQueryStrict(consensusSql`SELECT e.action_index, a1.address AS address, t1.tick AS tick, e.amount FROM escrows e INNER JOIN actions a ON (a.action_index=e.action_index) LEFT JOIN index_addresses a1 ON (a1.id=e.address_id) LEFT JOIN index_tickers t1 ON (t1.id=e.tick_id) WHERE a.block_index=? ORDER BY e.action_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, e.amount ASC`, [height]);
            for (const row of ledger.credits) row.address = canonicalizeHashAddress(row.address);
            for (const row of ledger.debits)  row.address = canonicalizeHashAddress(row.address);
            for (const row of ledger.escrows) row.address = canonicalizeHashAddress(row.address);
            return ledger;
        }
        async function getActionsHashData(height){
            return await this.db.doQueryStrict(consensusSql`SELECT a.action_index, a.tx_index, ia.action AS action FROM actions a LEFT JOIN index_actions ia ON (ia.id=a.action_id) WHERE a.block_index=? ORDER BY a.action_index ASC`, [height]);
        }
        async function getContractsHashData(height){
            const contracts = { contracts: [], state: [], executions: [], emissions: [], deposits: [], withdrawals: [] };
            contracts.contracts = await this.db.doQueryStrict(consensusSql`SELECT c.action_index, a1.address AS source_address, c.code_hash, s1.status AS status FROM contracts c INNER JOIN actions a ON (a.action_index=c.action_index) LEFT JOIN index_addresses a1 ON (a1.id=c.source_id) LEFT JOIN index_statuses s1 ON (s1.id=c.status_id) WHERE a.block_index=? ORDER BY c.action_index ASC`, [height]);
            let stateKeyCollate = gateRegistry.activeAt(STATE_KEY_COLLATION_KEY, network, coin, block_index, null) ? ' COLLATE utf8_bin' : '';
            const stateQuery = `SELECT cs.contract_index, cs.state_key, cs.state_value FROM contract_state cs INNER JOIN ( SELECT MAX(id) as max_id FROM contract_state WHERE block_index=? GROUP BY contract_index, state_key` + stateKeyCollate + ` ) latest ON cs.id = latest.max_id ORDER BY cs.contract_index ASC, cs.state_key` + stateKeyCollate + ` ASC`;
            contracts.state = await this.db.doQueryStrict(consensusSql(stateQuery), [height]);
            contracts.executions = await this.db.doQueryStrict(consensusSql`SELECT ce.action_index, ce.contract_index, a1.address AS caller_address, ce.gas_used, s1.status AS status, ce.emitted_count FROM contract_executions ce INNER JOIN actions a ON (a.action_index=ce.action_index) LEFT JOIN index_addresses a1 ON (a1.id=ce.caller_id) LEFT JOIN index_statuses s1 ON (s1.id=ce.status_id) WHERE a.block_index=? ORDER BY ce.action_index ASC`, [height]);
            contracts.emissions = await this.db.doQueryStrict(consensusSql`SELECT em.execution_index, em.emitted_action, em.action_index, em.position FROM contract_emissions em INNER JOIN contract_executions ce ON (ce.action_index=em.execution_index) INNER JOIN actions a ON (a.action_index=ce.action_index) WHERE a.block_index=? ORDER BY em.execution_index ASC, em.position ASC`, [height]);
            contracts.deposits = await this.db.doQueryStrict(consensusSql`SELECT d.action_index, d.contract_index, a1.address AS source_address, t1.tick AS tick, d.amount, s1.status AS status FROM deposits d INNER JOIN actions a ON (a.action_index=d.action_index) LEFT JOIN index_addresses a1 ON (a1.id=d.source_id) LEFT JOIN index_tickers t1 ON (t1.id=d.tick_id) LEFT JOIN index_statuses s1 ON (s1.id=d.status_id) WHERE a.block_index=? ORDER BY d.action_index ASC, d.contract_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, d.amount ASC, s1.status COLLATE utf8_bin ASC`, [height]);
            contracts.withdrawals = await this.db.doQueryStrict(consensusSql`SELECT w.action_index, w.contract_index, a1.address AS source_address, t1.tick AS tick, w.amount, s1.status AS status FROM withdrawals w INNER JOIN actions a ON (a.action_index=w.action_index) LEFT JOIN index_addresses a1 ON (a1.id=w.source_id) LEFT JOIN index_tickers t1 ON (t1.id=w.tick_id) LEFT JOIN index_statuses s1 ON (s1.id=w.status_id) WHERE a.block_index=? ORDER BY w.action_index ASC, w.contract_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, w.amount ASC, s1.status COLLATE utf8_bin ASC`, [height]);
            return contracts;
        }
        const info = [], hashes = [];
        const ledger = await getLedgerHashData.call(this, block_index);
        const actions = await getActionsHashData.call(this, block_index);
        const contracts_data = await getContractsHashData.call(this, block_index);
        // Previous block's committed hashes, which chain this block to the last.
        const prev_block_index = block_index - 1;
        const results = await this.db.doQueryStrict(consensusSql`SELECT t1.hash as ledger, t2.hash as actions, t3.hash as contracts FROM blocks b LEFT JOIN index_transactions t1 ON (t1.id=b.ledger_hash_id) LEFT JOIN index_transactions t2 ON (t2.id=b.actions_hash_id) LEFT JOIN index_transactions t3 ON (t3.id=b.contract_hash_id) WHERE b.block_index=?`, [prev_block_index]);
        if(results.length > 0){
            hashes['ledger']    = results[0].ledger;
            hashes['actions']   = results[0].actions;
            hashes['contracts'] = results[0].contracts;
        }
        // Each of ledger / actions / contracts is hashed with block_index + previous hash.
        let tables = ['ledger','actions','contracts'];
        tables.forEach(table => {
            var data = null;
            if(table=='ledger')    data = ledger;
            if(table=='actions')   data = actions;
            if(table=='contracts') data = contracts_data;
            data['block_index']   = block_index;
            data['previous_hash'] = hashes[table];
            // Fold the consensus hash-scheme version into the preimage (mirrors the indexer).
            data['hash_version']  = BLOCK_HASH_VERSION;
            info[table] = [];
            info[table]['hash'] = this.util.getDataHash(data);
        });
        // Normalise to the field names used across xchain-sync (ledger_hash, ...).
        return {
            ledger_hash:   info['ledger']['hash'],
            actions_hash:  info['actions']['hash'],
            contract_hash: info['contracts']['hash']
        };
    }

    // Recompute the replication-integrity state_hash for a block from the replicated
    // raw rows (the fourth hash covering the in-place mutations + backdated refund
    // credits the three consensus hashes structurally cannot see (see src/consensus/state_hash.js).
    // Conformance twin of xchain-indexer/src/db/actions.js getBlockHashes' state_hash branch:
    // both call the byte-identical buildStateHashData + the shared getDataHash. The
    // caller MUST invoke this APPLY-TIME (tip = block_index), never via a historical
    // recompute, where the in-place-mutated rows have since moved on (see ClientSync).
    // activationDelay is the frozen per-chain ACTIVATION_DELAY_BLOCKS; gasTick defaults
    // to the consensus GAS constant.
    async computeStateHashPreimage(block_index, activationDelay, gasTick, network, coin){
        return buildStateHashData(this.db, block_index, {
            activationDelay: activationDelay,
            gasTick:         (gasTick !== undefined) ? gasTick : gasTickSymbol(),
            // network gates the additive index-map class (id-determinism P4); coin extends
            // the lookup to the per-chain '<COIN>:<network>' keys the mid-chain-armed
            // classes (poll_finalize / token_supply) use. The follower MUST pass the SAME
            // (network, coin) pair the source used so its recompute matches byte-for-byte
            // across each chain's activation height.
            network:         network,
            coin:            coin
        });
    }

    async computeStateHash(block_index, activationDelay, gasTick, network, coin){
        let stateData = await this.computeStateHashPreimage(
            block_index, activationDelay, gasTick, network, coin);
        return this.util.getDataHash(stateData);
    }

    // ADVISORY, NON-CONSENSUS. Not part of the conformance pair above, not a block
    // hash, not in BLOCK_HASH_VERSION: the indexer never computes this, so there is
    // no indexer twin to stay byte-identical with. Its ONLY conformance requirement
    // is server-vs-client agreement, and both sides call THIS one method (server over
    // its source DB, client over the replica), so they match by construction.
    //
    // Cumulative checksum over the DETERMINISTIC subset of the id->address map: the
    // rows whose id was assigned inside a consensus block tx (block_index IS NOT NULL),
    // up to and including uptoBlock. Rows assigned outside a block tx carry a NULL
    // block_index (recovery reward-source pre-seed, API read-path createAddress; see
    // xchain-indexer/src/db/index_tables.js createAddress) and are EXCLUDED, so the benign id drift
    // those paths legitimately produce never registers as a mismatch.
    //
    // Purpose: the source resolves a wire ^<id> address reference to its canonical
    // STRING before hashing it into the consensus ledger/actions/contract hash, never
    // the raw id, so a divergent id map is invisible to those hashes (and to a plain
    // per-table ROW COUNT, which agrees when the maps have the same size but different
    // contents). This checksum is the one signal that catches a replica whose id->address
    // map content diverged from the source's, e.g. a local INSERT IGNORE that kept a
    // pre-existing colliding id and dropped the source's authoritative row.
    //
    // Ordered by the numeric PK so the result is collation-independent (the address is
    // a hashed value, never a sort key). Empty subset hashes to a stable defined value.
    // Cost note: this scans the deterministic subset up to uptoBlock; on a large
    // index_addresses table an index on block_index is advisable before enabling this
    // on a high-volume chain. Gated off by default (INDEX_MAP_PARITY_CHECK).
    //
    // Fail the read closed: a query error reaches the caller (source publishes null,
    // follower skips) instead of hashing an empty map that reads as a false mismatch.
    async computeIndexMapChecksum(uptoBlock){
        let rows = await this.db.doQuery(
            "SELECT id, address FROM index_addresses WHERE block_index IS NOT NULL AND block_index <= ? ORDER BY id ASC",
            [uptoBlock], null, { rethrow: true }
        );
        let mapped = rows.map(r => ({ id: String(r.id), address: String(r.address) }));
        return this.util.getDataHash({ index_map: mapped });
    }

    // ADVISORY, NON-CONSENSUS, same posture as computeIndexMapChecksum. The tokens
    // metadata columns ISSUE folds in place (owner, locks, callback, lists, mint window,
    // bridge policy) sit in no block hash, no state_hash class and no content-parity
    // window, so a replica that missed an edit (updated_rows class 7) or its reversal on a
    // reorg (ClientRollback -> token_refold.refoldTokenRows) diverged with nothing to see
    // it. This digests every tokens row's fold columns (token_refold.FOLD_COLUMNS).
    //
    // The table is current state, not a block window, so the SOURCE bounds it instead:
    // a row whose last_action_index is above the last action at uptoBlock was edited
    // after the height this status publishes, and is left out and named in `ahead`. A
    // FOLLOWER passes that list back as opts.exclude and applies no bound of its own, so
    // a replica row wrongly carrying a later last_action_index still lands in its digest.
    async computeTokenFoldChecksum(uptoBlock, opts){
        let follower = !!(opts && Array.isArray(opts.exclude));
        let bound = null;
        if(!follower){
            let r = await this.db.doQuery("SELECT MAX(action_index) AS m FROM actions WHERE block_index <= ?",
                [uptoBlock], null, { rethrow: true });
            bound = (r && r.length && r[0].m !== null && r[0].m !== undefined) ? String(r[0].m) : '0';
        }
        let res = await tokenRefold.tokenFoldRows(this.db, bound, follower ? opts.exclude : []);
        return { h: this.util.getDataHash({ token_fold: res.rows }), n: res.rows.length,
                 ahead: follower ? opts.exclude.map(String) : res.ahead };
    }

    // ADVISORY, NON-CONSENSUS. Same posture as computeIndexMapChecksum
    // above: not a block hash, not in BLOCK_HASH_VERSION, no indexer twin. Its only
    // conformance requirement is server-vs-client agreement, and both sides call
    // THIS method over the bound the SOURCE published, so they agree by construction.
    //
    // Per-table content checksums over a bounded window, for every replicated table
    // the registry declares content-parity-covered (src/table_lifecycle.js
    // CONTENT_PARITY_*, resolved by replicatedTables.contentParityPlan). This is the
    // only signal that catches an equal-COUNT content substitution in a table no
    // consensus hash reads: the three block hashes cover the ledger/actions/contract
    // projections, state_hash covers in-place mutations, the /status row counts cover
    // cardinality, and everything else was uncommitted.
    //
    // Shape (published on /status, consumed by ClientSync.verifyAgainstSource):
    //   { window, block, tables: { <table>: { n, h, id_max? } } }
    // Sparse by design: a table with no rows in the window is OMITTED rather than
    // carried as an empty digest, which keeps the status payload small on a quiet
    // chain and lets the verifier treat "present on one side only" as its own case.
    //
    // Bounding. Block-bounded tables use the window [uptoBlock - window + 1,
    // uptoBlock] through their own scope join. Append-only lookups have no block
    // column, so they use the id window (id_max - window, id_max]; a SOURCE publishes
    // id_max and a follower MUST pass it back through opts.idBounds so both sides
    // read the same range rather than each hashing its own tail.
    //
    // Fail-soft per table: a schema gap or a transient read drops that ONE table from
    // the result (the verifier then skips it) instead of failing the whole advisory
    // pass. Cost note, same as the index-map checksum: this reads a window of every
    // covered table, so it is gated OFF by default (TABLE_CONTENT_PARITY_CHECK) and
    // the window is operator-tunable.
    async computeTableContentChecksums(uptoBlock, opts){
        let o        = opts || {};
        let dbType   = (this.db && this.db.dbType) === 'decoder' ? 'decoder' : 'indexer';
        let window   = Number.isFinite(Number(o.window)) && Number(o.window) > 0 ? Math.floor(Number(o.window)) : DEFAULT_CONTENT_PARITY_WINDOW;
        let idBounds = o.idBounds || {};
        let fromBlock = Math.max(0, Number(uptoBlock) - window + 1);

        // Ask ONCE which tables exist, so a replica whose schema predates a family
        // skips it instead of raising (and logging) a missing-table error per table
        // on every status poll, which is the lesson from table_counts. A failed
        // listing degrades to probing, never to "nothing exists".
        let present = null;
        try { present = await this.db.listExistingTables(); } catch(e){ /* fall back to probing */ }

        let tables = {};
        for(let step of replicatedTables.contentParityPlan(dbType)){
            if(present && !present.has(step.table)) continue;
            try {
                let rows, idMax = null;
                if(step.bound === 'id'){
                    idMax = (idBounds[step.table] !== undefined && idBounds[step.table] !== null)
                        ? Number(idBounds[step.table])
                        : await this.db.getMaxRowId(step.table);
                    if(idMax === null || !Number.isFinite(idMax)) continue;
                    rows = await this.db.getContentIdWindowRows(step.table, Math.max(0, idMax - window), idMax);
                } else {
                    rows = await this.db.getContentWindowRows(step.table, step.bound, fromBlock, Number(uptoBlock));
                }
                if(!rows || rows.length === 0) continue;
                tables[step.table] = { n: rows.length, h: this.contentDigest(step.table, rows) };
                if(idMax !== null) tables[step.table].id_max = String(idMax);
            } catch(e){
                // Advisory: one unreadable table must not cost the other 92 their check.
            }
        }
        return { window: window, block: Number(uptoBlock), tables: tables };
    }

    // Canonical content digest for one table's window of rows.
    //
    // Order-independent BY CONSTRUCTION: each row is reduced to a sorted-key JSON
    // string and the strings are then sorted, so neither storage order nor column
    // order nor a collation difference between source and follower can move the
    // digest. That is deliberately unlike the consensus hashes, whose ORDER BY is
    // part of the preimage: here a reordering is not a divergence, only differing
    // CONTENT is.
    //
    // Values are stringified defensively: the shared pool returns a BIGINT as a
    // Number while it is a safe integer and as its exact decimal string above that,
    // on both sides, so the two forms of one value agree here and a follower that
    // stored a rounded value past 2^53 no longer matches its source. Buffers go to
    // hex and Dates to ISO so neither a binary
    // column nor the local timezone can fork the digest. Columns the follower is not
    // expected to match (the stripped surrogate id, generated columns, in-place
    // edited columns) are dropped per the registry declaration.
    contentDigest(table, rows){
        let excluded = new Set(lifecycle.contentParityExcludedColumns(table));
        let canon = rows.map(row => {
            let out = {};
            for(let key of Object.keys(row).filter(k => !excluded.has(k)).sort()){
                let v = row[key];
                if(v === null || v === undefined)      out[key] = null;
                else if(Buffer.isBuffer(v))            out[key] = v.toString('hex');
                else if(v instanceof Date)             out[key] = v.toISOString();
                else                                   out[key] = String(v);
            }
            return JSON.stringify(out);
        }).sort();
        return this.util.getDataHash({ table: table, rows: canon });
    }
}

module.exports = BlockHasher;
