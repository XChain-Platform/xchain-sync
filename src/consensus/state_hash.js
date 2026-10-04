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
 * Per-block state-hash preimage (in-place mutations + backdated credits)
 *
 * The three consensus hashes (ledger/actions/contract) cover only rows scoped
 * by actions.block_index = B (new, immutable rows). They deliberately CANNOT see
 * the in-place mutations the replication "updated_rows" channel carries on
 * SURVIVING (earlier-block) rows, nor the backdated cooldown-refund credits that
 * reuse an earlier-block action_index. A follower that silently fails to apply
 * one of those mutations therefore diverges with NO hash mismatch to flag it.
 *
 * This builds a canonical, id-RESOLVED preimage over exactly those mutated rows
 * for block B, so a fourth `state_hash` can be computed and compared. It captures
 * the same row classes the source replicates:
 *   - deactivation_block stamps (stakes/delegations/contract_stakes/contract_delegations)
 *   - SLASH amount cuts (stakes/unstakes/contract_stakes/contract_unstakes via debit logs)
 *   - v0 request_status flips (attests/xcalls)
 *   - cooldown-maturity status flips (unstakes/contract_unstakes)
 *   - backdated cooldown refund credits (capability GAS + contract own-tick)
 *   - invalid_archive stamp on anchor_actions archive-head parent rows (CRC-failed
 *     chunked batches; version set and completing-chunk height key both flag-day gated)
 *   - VOTE poll finalization flips on surviving polls rows (flag-day gated per chain)
 *   - tokens.supply refreshes on surviving token rows (flag-day gated per chain; the
 *     hash twin of the updated_rows tokens-supply replication class)
 *
 * CONSENSUS-STYLE DETERMINISM (mirrors db.js getBlockHashes):
 *   - surrogate AUTO_INCREMENT ids (address_id/tick_id/status_id) are NEVER hashed;
 *     they are resolved to canonical strings via LEFT JOIN (they diverge across
 *     nodes after a reorg, which is the reason BLOCK_HASH_VERSION exists).
 *     action_index is the deterministic on-chain index, safe to hash raw.
 *   - every row set is ORDER BY'd with BINARY-collation-pinned tie-breaks so the
 *     order is independent of each node's default collation.
 *   - the preimage object's key order is fixed in code; the caller hashes it with
 *     the shared util.getDataHash (JSON.stringify + bigint replacer + sha256).
 *
 * BYTE-ALIGNED TWIN: this file is copied verbatim into xchain-sync/src/stateHash.js
 * (the source computes+stores from here; the follower recomputes from the identical
 * copy at apply-time and HALTS on mismatch). Keep them identical; the
 * state-hash-vectors golden + the xchain-e2e recompute-conformance scenario guard
 * the pair. The selection predicates also mirror xchain-sync/src/updatedRows.js +
 * cooldownCredits.js (forward) and ClientRollback.js + rollback.js (reverse).
 *
 ********************************************************************/

const { get, copy, activeAt } = require('./gate_registry');

const STATE_HASH_VERSION = copy('stateHash.STATE_HASH_VERSION');

const INDEX_MAP_STATE_HASH_ACTIVATION = copy('stateHash.INDEX_MAP_STATE_HASH_ACTIVATION');

// Whether the index-map class is folded into state_hash at `blockIndex` on `network`.
// Below the threshold / unknown network -> off (safe; the class is omitted and the
// preimage stays byte-identical to the pre-feature shape).
function isIndexMapStateHashActive(blockIndex, network){
    let b = parseInt(blockIndex);
    if(!Number.isFinite(b)) return false;
    let threshold = INDEX_MAP_STATE_HASH_ACTIVATION[network];
    if(threshold === undefined) return false;
    return b >= threshold;
}

const POLL_FINALIZE_STATE_HASH_ACTIVATION = copy('stateHash.POLL_FINALIZE_STATE_HASH_ACTIVATION');

// Resolve a per-chain activation threshold: '<COIN>:<network>' key first, then
// the bare network key. A production caller on mainnet/testnet MUST pass coin
// (both real call sites do: indexer db.js getBlockHashes and sync
// BlockHasher.computeStateHash); a coin-less lookup on those networks finds no
// key and stays inert, which is safe only while the OTHER side of the
// conformance pair is equally coin-less (unit fixtures/vectors).
function _activationThreshold(map, network, coin){
    if(coin != null && map[coin + ':' + network] !== undefined) return map[coin + ':' + network];
    return map[network];
}

// Whether the poll-finalize class is folded into state_hash at `blockIndex` on
// `network` for `coin`. Below the threshold / unknown network -> off (safe; the
// class is omitted and the preimage stays byte-identical to the pre-feature shape).
function isPollFinalizeStateHashActive(blockIndex, network, coin){
    let b = parseInt(blockIndex);
    if(!Number.isFinite(b)) return false;
    let threshold = _activationThreshold(POLL_FINALIZE_STATE_HASH_ACTIVATION, network, coin);
    if(threshold === undefined) return false;
    return b >= threshold;
}

const TOKEN_SUPPLY_STATE_HASH_ACTIVATION = copy('stateHash.TOKEN_SUPPLY_STATE_HASH_ACTIVATION');

// Whether the token-supply class is folded into state_hash at `blockIndex` on
// `network` for `coin`. Same fail-inert semantics as the poll-finalize gate.
function isTokenSupplyStateHashActive(blockIndex, network, coin){
    let b = parseInt(blockIndex);
    if(!Number.isFinite(b)) return false;
    let threshold = _activationThreshold(TOKEN_SUPPLY_STATE_HASH_ACTIVATION, network, coin);
    if(threshold === undefined) return false;
    return b >= threshold;
}

const BET_STATUS_STATE_HASH_ACTIVATION = copy('stateHash.BET_STATUS_STATE_HASH_ACTIVATION');

// Whether the BET status-flip class is folded into state_hash at `blockIndex`
// on `network` for `coin`. Same fail-inert semantics as the gates above.
function isBetStatusStateHashActive(blockIndex, network, coin){
    let b = parseInt(blockIndex);
    if(!Number.isFinite(b)) return false;
    let threshold = _activationThreshold(BET_STATUS_STATE_HASH_ACTIVATION, network, coin);
    if(threshold === undefined) return false;
    return b >= threshold;
}

const ARCHIVE_HEAD_VERSIONS = copy('stateHash.ARCHIVE_HEAD_VERSIONS');
const ARCHIVE_HEAD_VERSIONS_SQL = copy('stateHash.ARCHIVE_HEAD_VERSIONS_SQL');

function archiveHeadPredicate(alias){
    return alias + ".match_batch_seq IS NOT NULL AND " + alias + ".version <> 2";
}

function checkpointSectionPredicate(alias){
    return alias + ".chain IS NOT NULL";
}

const ARCHIVE_INVALID_STATE_HASH_ACTIVATION = copy('stateHash.ARCHIVE_INVALID_STATE_HASH_ACTIVATION');

// Whether the anchor_invalid class covers the full archive-head version set at
// `blockIndex` on `network` for `coin`. Below the threshold / unknown network ->
// off (safe; the class keeps its legacy v1-only selection, preimage unchanged).
function isArchiveInvalidStateHashActive(blockIndex, network, coin){
    let b = parseInt(blockIndex);
    if(!Number.isFinite(b)) return false;
    let threshold = _activationThreshold(ARCHIVE_INVALID_STATE_HASH_ACTIVATION, network, coin);
    if(threshold === undefined) return false;
    return b >= threshold;
}

const ARCHIVE_INVALID_HEIGHT_KEY_ACTIVATION = copy('stateHash.ARCHIVE_INVALID_HEIGHT_KEY_ACTIVATION');

const ARCHIVE_CHUNK_HEIGHT_COL = copy('stateHash.ARCHIVE_CHUNK_HEIGHT_COL');
const ARCHIVE_CHUNK_HEIGHT_COL_LEGACY = copy('stateHash.ARCHIVE_CHUNK_HEIGHT_COL_LEGACY');

// Whether class 6 scopes the completing v2 chunk by the repaired
// `block_index_doge` key at `blockIndex` on `network` for `coin`. Below the
// threshold / unknown network -> off (safe; the class keeps the legacy
// `block_index` key, so the preimage is byte-identical to the pre-repair shape).
function isArchiveInvalidHeightKeyActive(blockIndex, network, coin){
    let b = parseInt(blockIndex);
    if(!Number.isFinite(b)) return false;
    let threshold = _activationThreshold(ARCHIVE_INVALID_HEIGHT_KEY_ACTIVATION, network, coin);
    if(threshold === undefined) return false;
    return b >= threshold;
}

const DEACTIVATION_TABLES = copy('stateHash.DEACTIVATION_TABLES');
const SLASH_SPECS = copy('stateHash.SLASH_SPECS');
const REQUEST_STATUS_TABLES = copy('stateHash.REQUEST_STATUS_TABLES');
const COOLDOWN_TABLES = copy('stateHash.COOLDOWN_TABLES');

async function buildDeactivations(db, B, delay){
    let deactivations = {};
    for(let t of DEACTIVATION_TABLES){
        deactivations[t] = [];
        if(delay == null) continue;
        try {
            deactivations[t] = await db.doQuery(
                "SELECT action_index, deactivation_block FROM `" + t + "` " +
                "WHERE deactivation_block BETWEEN ? AND ? ORDER BY action_index ASC",
                [B + delay, B + delay]);
        } catch(e){ if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e; /* table/column may not exist on older schemas */ }
    }
    return deactivations;
}

async function buildSlashes(db, B){
    let slashes = {};
    for(let s of SLASH_SPECS){
        slashes[s.table] = [];
        try {
            slashes[s.table] = await db.doQuery(
                "SELECT DISTINCT t.action_index, t.amount FROM `" + s.table + "` t " +
                "JOIN `" + s.debits + "` d ON d.stake_action_index = t.action_index " +
                "WHERE d.target_table = ? AND d.block_index BETWEEN ? AND ? ORDER BY t.action_index ASC",
                [s.target, B, B]);
        } catch(e){ if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e; /* table may not exist on older schemas */ }
    }
    return slashes;
}

async function buildRequestStatus(db, B){
    let request_status = {};
    for(let t of REQUEST_STATUS_TABLES){
        request_status[t] = [];
        try {
            request_status[t] = await db.doQuery(
                "SELECT action_index, request_status, resolved_block FROM `" + t + "` " +
                "WHERE version = 0 AND resolved_block BETWEEN ? AND ? ORDER BY action_index ASC",
                [B, B]);
        } catch(e){ if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e; /* table/column may not exist on older schemas */ }
    }
    return request_status;
}

async function buildCooldown(db, B){
    let cooldown = {};
    for(let t of COOLDOWN_TABLES){
        cooldown[t] = [];
        try {
            cooldown[t] = await db.doQuery(
                "SELECT t.action_index, s.status AS status FROM `" + t + "` t " +
                "LEFT JOIN index_statuses s ON (s.id = t.status_id) " +
                "WHERE t.cooldown_end_block BETWEEN ? AND ? ORDER BY t.action_index ASC",
                [B, B]);
        } catch(e){ if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e; /* table/column may not exist on older schemas */ }
    }
    return cooldown;
}

async function buildCredits(db, B, gasTick, completedStatusId){
    let credits = [];
    if(completedStatusId != null && completedStatusId !== undefined){
        try {
            credits = await db.doQuery(
                "SELECT action_index, address, tick, amount FROM ( " +
                    "SELECT c.action_index, a.address AS address, ti.tick AS tick, c.amount " +
                    "FROM credits c " +
                    "JOIN unstakes u ON (u.action_index = c.action_index AND u.source_id = c.address_id) " +
                    "JOIN index_tickers g ON (g.id = c.tick_id AND g.tick = ?) " +
                    "LEFT JOIN index_addresses a ON (a.id = c.address_id) " +
                    "LEFT JOIN index_tickers ti ON (ti.id = c.tick_id) " +
                    "WHERE u.status_id = ? AND u.cooldown_end_block BETWEEN ? AND ? " +
                    "UNION ALL " +
                    "SELECT c.action_index, a.address AS address, ti.tick AS tick, c.amount " +
                    "FROM credits c " +
                    "JOIN contract_unstakes cu ON (cu.action_index = c.action_index AND cu.source_id = c.address_id AND cu.tick_id = c.tick_id) " +
                    "LEFT JOIN index_addresses a ON (a.id = c.address_id) " +
                    "LEFT JOIN index_tickers ti ON (ti.id = c.tick_id) " +
                    "WHERE cu.status_id = ? AND cu.cooldown_end_block BETWEEN ? AND ? " +
                ") x ORDER BY action_index ASC, address COLLATE utf8_bin ASC, tick COLLATE utf8mb4_bin ASC, amount ASC",
                [gasTick, completedStatusId, B, B, completedStatusId, B, B]);
        } catch(e){ if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e; /* table may not exist on older schemas */ }
    }
    return credits;
}

async function buildAnchorInvalid(db, B, network, coin){
    let archiveInvalidActive = isArchiveInvalidStateHashActive(B, network, coin);
    let chunkHeightCol = isArchiveInvalidHeightKeyActive(B, network, coin)
                            ? ARCHIVE_CHUNK_HEIGHT_COL : ARCHIVE_CHUNK_HEIGHT_COL_LEGACY;
    let anchor_invalid = [];
    try {
        anchor_invalid = await db.doQuery(
            "SELECT p.action_index, s.status AS status FROM anchor_actions p " +
            "JOIN anchor_actions c ON c.version = 2 AND c.match_batch_seq = p.match_batch_seq " +
            "JOIN index_statuses s ON s.id = p.status_id AND s.status = 'invalid_archive' " +
            "JOIN index_statuses cs ON cs.id = c.status_id AND cs.status = 'valid' " +
            "WHERE " + (archiveInvalidActive
                ? archiveHeadPredicate('p') + " AND p.version " + ARCHIVE_HEAD_VERSIONS_SQL
                : "p.version = 1") +
            " AND " + chunkHeightCol + " BETWEEN ? AND ? " +
            "ORDER BY p.action_index ASC",
            [B, B]);
    } catch(e){ if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e; /* table/columns may not exist on older schemas */ }
    return anchor_invalid;
}

async function buildIndexMap(db, B, network){
    let indexMapActive = isIndexMapStateHashActive(B, network);
    let index_addresses_new = [];
    let index_tickers_new   = [];
    if(indexMapActive){
        try {
            index_addresses_new = await db.doQuery(
                "SELECT id, address FROM index_addresses WHERE block_index = ? ORDER BY id ASC", [B]);
        } catch(e){ if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e; /* table/column may not exist on older schemas */ }
        try {
            index_tickers_new = await db.doQuery(
                "SELECT id, tick FROM index_tickers WHERE block_index = ? ORDER BY id ASC", [B]);
        } catch(e){ if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e; /* table/column may not exist on older schemas */ }
    }
    return { indexMapActive, index_addresses_new, index_tickers_new };
}

async function buildPollFinalize(db, B, network, coin){
    let pollFinalizeActive = isPollFinalizeStateHashActive(B, network, coin);
    let poll_finalize = [];
    if(pollFinalizeActive){
        try {
            poll_finalize = await db.doQuery(
                "SELECT action_index, poll_status, winning_option, total_weight, total_voters, " +
                "quorum_met, min_voters_met, fail_reason, decided_early, effective_close_block, " +
                "finalized_action_index, resolved_block, deposit_resolved, callback_execute_action_index " +
                "FROM polls WHERE resolved_block BETWEEN ? AND ? ORDER BY action_index ASC",
                [B, B]);
        } catch(e){ if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e; /* table/columns may not exist on older schemas */ }
    }
    return { pollFinalizeActive, poll_finalize };
}

async function buildTokenSupply(db, B, network, coin){
    let tokenSupplyActive = isTokenSupplyStateHashActive(B, network, coin);
    let token_supply = [];
    if(tokenSupplyActive){
        try {
            token_supply = await db.doQuery(
                "SELECT tk.tick AS tick, t.supply AS supply FROM tokens t " +
                "JOIN index_tickers tk ON (tk.id = t.tick_id) " +
                "WHERE t.tick_id IN ( " +
                    "SELECT c.tick_id FROM credits c JOIN actions a ON (a.action_index = c.action_index) WHERE a.block_index BETWEEN ? AND ? AND c.tick_id IS NOT NULL " +
                    "UNION " +
                    "SELECT d.tick_id FROM debits d JOIN actions a ON (a.action_index = d.action_index) WHERE a.block_index BETWEEN ? AND ? AND d.tick_id IS NOT NULL " +
                    "UNION " +
                    "SELECT e.tick_id FROM escrows e JOIN actions a ON (a.action_index = e.action_index) WHERE a.block_index BETWEEN ? AND ? AND e.tick_id IS NOT NULL " +
                ") ORDER BY tick COLLATE utf8mb4_bin ASC",
                [B, B, B, B, B, B]);
        } catch(e){ if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e; /* table/columns may not exist on older schemas */ }
    }
    return { tokenSupplyActive, token_supply };
}

async function buildBetStatus(db, B, network, coin){
    let betStatusActive = isBetStatusStateHashActive(B, network, coin);
    let bet_feed_status = [];
    let bet_status = [];
    if(betStatusActive){
        try {
            bet_feed_status = await db.doQuery(
                "SELECT f.action_index, s.status AS feed_status, f.closed_block, f.terminal_block " +
                "FROM bet_feeds f JOIN index_statuses s ON (s.id = f.feed_status_id) " +
                "WHERE f.closed_block = ? OR f.terminal_block = ? ORDER BY f.action_index ASC",
                [B, B]);
        } catch(e){ if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e; /* table/columns may not exist on older schemas */ }
        try {
            bet_status = await db.doQuery(
                "SELECT b.action_index, s.status AS bet_status, b.settled_block " +
                "FROM bets b JOIN index_statuses s ON (s.id = b.bet_status_id) " +
                "WHERE b.settled_block = ? ORDER BY b.action_index ASC",
                [B]);
        } catch(e){ if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e; /* table/columns may not exist on older schemas */ }
    }
    return { betStatusActive, bet_feed_status, bet_status };
}

function assemblePreimage(B, sections){
    let preimage = {
        deactivations:      sections.deactivations,
        slashes:            sections.slashes,
        request_status:     sections.request_status,
        cooldown:           sections.cooldown,
        credits:            sections.credits,
        anchor_invalid:     sections.anchor_invalid
    };
    if(sections.indexMapActive){
        preimage.index_addresses_new = sections.index_addresses_new;
        preimage.index_tickers_new   = sections.index_tickers_new;
    }
    if(sections.pollFinalizeActive){
        preimage.poll_finalize = sections.poll_finalize;
    }
    if(sections.tokenSupplyActive){
        preimage.token_supply = sections.token_supply;
    }
    if(sections.betStatusActive){
        preimage.bet_feed_status = sections.bet_feed_status;
        preimage.bet_status      = sections.bet_status;
    }
    preimage.block_index        = B;
    preimage.state_hash_version = STATE_HASH_VERSION;
    return preimage;
}

// Build the canonical state-hash preimage object for block B. Each section is
// awaited in its original order so database calls remain strictly sequential.
async function buildStateHashData(db, blockIndex, opts){
    let B       = Number(blockIndex);
    let delay   = (opts && opts.activationDelay != null) ? Number(opts.activationDelay) : null;
    let gasTick = (opts && opts.gasTick != null) ? opts.gasTick : null;
    let network = (opts && opts.network != null) ? opts.network : null;
    let coin    = (opts && opts.coin != null) ? opts.coin : null;
    let completedStatusId = await db.getStatusId('completed');
    let sections = {};
    sections.deactivations = await buildDeactivations(db, B, delay);
    sections.slashes = await buildSlashes(db, B);
    sections.request_status = await buildRequestStatus(db, B);
    sections.cooldown = await buildCooldown(db, B);
    sections.credits = await buildCredits(db, B, gasTick, completedStatusId);
    sections.anchor_invalid = await buildAnchorInvalid(db, B, network, coin);
    Object.assign(sections, await buildIndexMap(db, B, network));
    Object.assign(sections, await buildPollFinalize(db, B, network, coin));
    Object.assign(sections, await buildTokenSupply(db, B, network, coin));
    Object.assign(sections, await buildBetStatus(db, B, network, coin));
    return assemblePreimage(B, sections);
}

module.exports = { buildStateHashData, STATE_HASH_VERSION,
                   DEACTIVATION_TABLES, SLASH_SPECS, REQUEST_STATUS_TABLES, COOLDOWN_TABLES,
                   INDEX_MAP_STATE_HASH_ACTIVATION, isIndexMapStateHashActive,
                   POLL_FINALIZE_STATE_HASH_ACTIVATION, isPollFinalizeStateHashActive,
                   TOKEN_SUPPLY_STATE_HASH_ACTIVATION, isTokenSupplyStateHashActive,
                   BET_STATUS_STATE_HASH_ACTIVATION, isBetStatusStateHashActive,
                   ARCHIVE_HEAD_VERSIONS, ARCHIVE_HEAD_VERSIONS_SQL,
                   archiveHeadPredicate, checkpointSectionPredicate,
                   ARCHIVE_INVALID_STATE_HASH_ACTIVATION, isArchiveInvalidStateHashActive,
                   ARCHIVE_INVALID_HEIGHT_KEY_ACTIVATION, isArchiveInvalidHeightKeyActive,
                   ARCHIVE_CHUNK_HEIGHT_COL, ARCHIVE_CHUNK_HEIGHT_COL_LEGACY };
