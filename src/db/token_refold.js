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
 * Replica refold of the `tokens` metadata columns from surviving `issues`
 *
 * The reverse leg of updated_rows class 7 (src/server/updated_rows/token_rows.js).
 * The source rewrites the WHOLE tokens row from the tick's valid ISSUE history on
 * every refresh, and its reorg refreshes every tick the orphaned range touched:
 *
 *   xchain-indexer/src/rollback/index.js:165   collectAffectedEntities (issues read
 *                                              at src/db/rollback/read_phase.js:136)
 *   xchain-indexer/src/rollback/commit.js:60   updateTokens(tickers, true)
 *   xchain-indexer/src/db/database/ledger_checks.js:130  updateTokenInfo
 *   xchain-indexer/src/db/issues/token_info.js:29        getTokenInfo, whose replay is
 *       rowsQuery (:84), rowValues (:131) and foldRow (:169)
 *   xchain-indexer/src/db/tokens/token_writer.js:26      createToken, via
 *       normalizeDataValues (src/db/database/normalize.js:136), fields (:57) and
 *       updateSql (:113)
 *
 * A replica cannot call that code, and nothing streams the refolded row back: the
 * row's action_index is pinned at the first issuance, and the surviving issues are
 * older than any window the stream will carry again. So this module restates the
 * fold, step for step, over the replica's own replicated `issues` rows, and writes
 * the same column values the source's UPDATE binds. `supply` is left to
 * balance_helpers.recomputeTokenSupplies, which already mirrors getTokenSupply and
 * must run AFTER this (it reads tokens.decimals). `bridged`, `escrow_action_index`,
 * `coin_price` and `coin_floor` are not fold output and are never touched.
 *
 * test/unit/token_refold.test.js runs the indexer's own getTokenInfo + createToken
 * beside this port over the same issue rows when the sibling checkout is present.
 *
 ********************************************************************/

'use strict';

const mathjs = require('mathjs');

// Wire-field lists the source's normalizeDataValues applies, restricted to the keys
// the fold produces (xchain-indexer/src/config/wire_fields.js, token_limits.js).
const NUMBER_FIELDS  = ['ALLOW_LIST', 'BLOCK_LIST', 'CALLBACK_AMOUNT', 'CALLBACK_BLOCK', 'DECIMALS',
                        'MAX_SUPPLY', 'MAX_MINT', 'MINT_ADDRESS_MAX', 'MINT_START_BLOCK', 'MINT_STOP_BLOCK'];
const LIST_FIELDS    = ['ALLOW_LIST', 'BLOCK_LIST'];
const INTEGER_FIELDS = ['ALLOW_LIST', 'BLOCK_LIST', 'CALLBACK_BLOCK', 'MINT_START_BLOCK', 'MINT_STOP_BLOCK'];
const U64_MAX        = '18446744073709551615';
// LOCK_BRIDGE is deliberately absent: the source's LOCK_FIELDS list omits it too.
const LOCK_FIELDS    = ['LOCK_MAX_SUPPLY', 'LOCK_MINT', 'LOCK_MINT_SUPPLY', 'LOCK_MAX_MINT',
                        'LOCK_DESCRIPTION', 'LOCK_SLEEP', 'LOCK_CALLBACK'];
const MIN_TOKEN_DECIMALS = 0;
const MAX_TOKEN_DECIMALS = 18;

// The fold-owned columns, in the order the source's UPDATE lists them (minus supply).
// Shared with the advisory parity digest so the refold and its detector cover one set.
const FOLD_COLUMNS = Object.freeze(['max_supply', 'max_mint', 'decimals', 'description',
    'lock_max_supply', 'lock_mint', 'lock_mint_supply', 'lock_max_mint', 'lock_description',
    'lock_sleep', 'lock_callback', 'callback_block', 'callback_tick_id', 'callback_amount',
    'allow_list', 'block_list', 'mint_address_max', 'mint_start_block', 'mint_stop_block',
    'bridge_chains', 'min_depth', 'lock_bridge', 'owner_id', 'last_action_index']);

const CHUNK = 500;

// Source Utility.isNull / isNumeric / exceedsUnsignedColumn / bcformat, restated.
function isNull(v){ return (v === null || v === undefined || v === ''); }
function isNumeric(v){ return typeof v === 'bigint' || (!isNaN(parseFloat(v)) && isFinite(v)); }
function exceedsU64(v){
    let raw = String(v).trim();
    if(/^[+-]?[0-9]+$/.test(raw)){ let n = BigInt(raw); return (n < 0n || n > BigInt(U64_MAX)); }
    let approx = Number(raw);
    return (Number.isFinite(approx) && (approx < 0 || approx > Number(U64_MAX)));
}
function bcformat(num, decimals){
    let str = String(num).trim();
    let bn  = (str === 'NaN' || str === 'Infinity' || str === '-Infinity' || !isNumeric(num))
        ? mathjs.bignumber(0) : mathjs.bignumber(str);
    return mathjs.format(bn, { notation: 'fixed', precision: isNull(decimals) ? 0 : parseInt(decimals) });
}

// One issues row keyed as the source's token-info fields (rowValues). Ids stand in for
// the strings the source later interns back to the same ids: the tick, the owner
// (transfer address when it resolves, else the action's source address) and the
// callback tick (only when it resolves, since the source's LEFT JOIN gives null).
function rowValues(row){
    return {
        ACTION_INDEX: row.action_index,
        TICK_ID: row.tick_id,
        OWNER_ID: (row.transfer_addr_id !== null && row.transfer_addr_id !== undefined) ? row.transfer_addr_id : row.source_addr_id,
        MAX_SUPPLY: row.max_supply, MAX_MINT: row.max_mint,
        DECIMALS: (!isNull(row.decimals)) ? parseInt(row.decimals) : 0,
        DESCRIPTION: row.description,
        LOCK_MAX_SUPPLY: row.lock_max_supply, LOCK_MINT_SUPPLY: row.lock_mint_supply,
        LOCK_MINT: row.lock_mint, LOCK_MAX_MINT: row.lock_max_mint,
        LOCK_DESCRIPTION: row.lock_description, LOCK_SLEEP: row.lock_sleep, LOCK_CALLBACK: row.lock_callback,
        CALLBACK_TICK_ID: row.callback_tick_ref, CALLBACK_BLOCK: row.callback_block, CALLBACK_AMOUNT: row.callback_amount,
        ALLOW_LIST: row.allow_list, BLOCK_LIST: row.block_list,
        BRIDGE_CHAINS: row.bridge_chains, MIN_DEPTH: row.min_depth, LOCK_BRIDGE: row.lock_bridge,
        MINT_ADDRESS_MAX: row.mint_address_max, MINT_START_BLOCK: row.mint_start_block, MINT_STOP_BLOCK: row.mint_stop_block,
    };
}

// foldRow, verbatim in effect: a set LOCK_ never unsets, DECIMALS never drops, an empty
// value inherits. ACTION_INDEX is overwritten by every row (the source's first-issuance
// branch does not `continue`), so it ends on the LAST valid issue, which is the value the
// source's UPDATE writes to last_action_index.
function foldRow(data, arr){
    for(let key in arr){
        let value = arr[key];
        if(key === 'ACTION_INDEX' && isNull(data[key])) data[key] = value;
        if(key.substr(0, 5) === 'LOCK_' && data[key] == 1) continue;
        if(key === 'DECIMALS' && data[key] > value) continue;
        if(isNull(value)) continue;
        data[key] = value;
    }
}

// normalizeDataValues over the fold's keys. The fold carries no ACTION key, so the
// source's per-action text truncations never apply.
function normalize(input){
    let data = Object.assign({}, input);
    for(let key in data)
        if(!isNull(data[key]) && typeof data[key] === 'object' && !Buffer.isBuffer(data[key])) data[key] = String(data[key]);
    for(let f of LIST_FIELDS)    if(!isNull(data[f]) && !isNumeric(data[f])) data[f] = null;
    for(let f of NUMBER_FIELDS)  if(isNull(data[f]) || !isNumeric(data[f])) data[f] = null;
    for(let f of INTEGER_FIELDS) if(!isNull(data[f]) && exceedsU64(data[f])) data[f] = null;
    for(let f of LOCK_FIELDS){
        let v = data[f];
        if(typeof v === 'string' && isNumeric(v)) v = parseInt(v);
        data[f] = ([0, 1].indexOf(v) === -1) ? null : v;
    }
    if(!isNull(data.DECIMALS) && (data.DECIMALS < MIN_TOKEN_DECIMALS || data.DECIMALS > MAX_TOKEN_DECIMALS)) data.DECIMALS = null;
    return data;
}

// createToken's fields(): the fold state rendered as the column values its UPDATE binds.
function tokenColumns(folded){
    let d = normalize(folded);
    let num = (v) => (!isNull(v) && isNumeric(v)) ? v : 0;
    let decimals = (!isNull(d.DECIMALS) && isNumeric(d.DECIMALS)) ? parseInt(d.DECIMALS) : 0;
    let c = {
        max_supply: num(d.MAX_SUPPLY), max_mint: num(d.MAX_MINT), mint_address_max: num(d.MINT_ADDRESS_MAX),
        decimals: decimals, description: (d.DESCRIPTION === undefined) ? null : d.DESCRIPTION,
        lock_max_supply: (d.LOCK_MAX_SUPPLY == 1) ? 1 : 0, lock_mint: (d.LOCK_MINT == 1) ? 1 : 0,
        lock_mint_supply: (d.LOCK_MINT_SUPPLY == 1) ? 1 : 0, lock_max_mint: (d.LOCK_MAX_MINT == 1) ? 1 : 0,
        lock_description: (d.LOCK_DESCRIPTION == 1) ? 1 : 0, lock_sleep: (d.LOCK_SLEEP == 1) ? 1 : 0,
        lock_callback: (d.LOCK_CALLBACK == 1) ? 1 : 0,
        callback_block: (d.CALLBACK_BLOCK > 0) ? d.CALLBACK_BLOCK : 0,
        callback_tick_id: isNull(d.CALLBACK_TICK_ID) ? null : d.CALLBACK_TICK_ID,
        callback_amount: num(d.CALLBACK_AMOUNT),
        allow_list: (!isNull(d.ALLOW_LIST) && isNumeric(d.ALLOW_LIST)) ? parseInt(d.ALLOW_LIST) : null,
        block_list: (!isNull(d.BLOCK_LIST) && isNumeric(d.BLOCK_LIST)) ? parseInt(d.BLOCK_LIST) : null,
        mint_start_block: num(d.MINT_START_BLOCK), mint_stop_block: num(d.MINT_STOP_BLOCK),
        bridge_chains: (!isNull(d.BRIDGE_CHAINS) && String(d.BRIDGE_CHAINS) !== '-') ? String(d.BRIDGE_CHAINS) : null,
        min_depth: (!isNull(d.MIN_DEPTH) && isNumeric(d.MIN_DEPTH)) ? parseInt(d.MIN_DEPTH) : null,
        lock_bridge: (d.LOCK_BRIDGE == 1) ? 1 : 0,
        owner_id: isNull(d.OWNER_ID) ? null : d.OWNER_ID,
        last_action_index: d.ACTION_INDEX,
    };
    // The source formats the amount limits at the token's precision; callback_amount stays raw.
    if(decimals >= MIN_TOKEN_DECIMALS && decimals <= MAX_TOKEN_DECIMALS){
        c.max_supply       = bcformat(c.max_supply, decimals);
        c.max_mint         = bcformat(c.max_mint, decimals);
        c.mint_address_max = bcformat(c.mint_address_max, decimals);
    }
    return c;
}

// Fold rows already grouped in (tick_id, action_index) order into one column set per
// tick. Each set also carries first_action_index (not a FOLD_COLUMN, never written): the
// first surviving valid issue, which refoldTokenRows checks against tokens.action_index.
function foldIssueRows(rows){
    let byTick = new Map();
    let first = new Map();
    for(let row of (rows || [])){
        let key = String(row.tick_id);
        if(!byTick.has(key)){ byTick.set(key, {}); first.set(key, row.action_index); }
        foldRow(byTick.get(key), rowValues(row));
    }
    let out = new Map();
    for(let [tick, data] of byTick) out.set(tick, Object.assign(tokenColumns(data), { first_action_index: first.get(tick) }));
    return out;
}

// rowsQuery for a set of ticks. The INNER JOINs are the source's, so an issue whose
// action, transaction, ticker, source address or status does not resolve drops out on
// both sides alike.
function foldRowsSql(n){
    return "SELECT i.tick_id, i.action_index, i.max_supply, i.max_mint, i.decimals, i.description, " +
        "i.lock_max_supply, i.lock_mint_supply, i.lock_mint, i.lock_max_mint, i.lock_description, " +
        "i.lock_sleep, i.lock_callback, i.callback_block, i.callback_amount, i.mint_address_max, " +
        "i.mint_start_block, i.mint_stop_block, i.allow_list, i.block_list, i.bridge_chains, " +
        "i.min_depth, i.lock_bridge, a2.id AS source_addr_id, a3.id AS transfer_addr_id, " +
        "t3.id AS callback_tick_ref " +
        "FROM issues i " +
        "INNER JOIN actions a1 ON (a1.action_index=i.action_index) " +
        "INNER JOIN transactions t1 ON (t1.tx_index=a1.tx_index) " +
        "INNER JOIN index_tickers t2 ON (t2.id=i.tick_id) " +
        "INNER JOIN index_addresses a2 ON (a2.id=a1.source_id) " +
        "INNER JOIN index_statuses s1 ON (s1.id=i.status_id) " +
        "LEFT JOIN index_addresses a3 ON (a3.id=i.transfer_id) " +
        "LEFT JOIN index_tickers t3 ON (t3.id=i.callback_tick_id) " +
        "WHERE s1.status='valid' AND i.tick_id IN (" + new Array(n).fill('?').join(',') + ") " +
        "ORDER BY i.tick_id ASC, i.action_index ASC";
}

async function foldTicks(db, tickIds){
    let out = new Map();
    for(let i = 0; i < tickIds.length; i += CHUNK){
        let part = tickIds.slice(i, i + CHUNK);
        let rows = await db.doQuery(foldRowsSql(part.length), part, null, { rethrow: true });
        for(let [k, v] of foldIssueRows(rows)) out.set(k, v);
    }
    return out;
}

// Ticks named by any issues row in the orphaned range, valid or not, read BEFORE the
// dataTables delete removes those rows. The source's read phase reads `issues` with no
// status filter too, so both sides refresh the same set. An empty result makes the
// refold below a no-op, which is what a rollback that touched no ISSUE must be.
async function collectIssueTickIds(db, firstActionIndex){
    let rows = await db.doQuery(
        "SELECT DISTINCT tick_id FROM issues WHERE action_index >= ? AND tick_id IS NOT NULL ORDER BY tick_id",
        [firstActionIndex], null, { rethrow: true });
    return (rows || []).map(r => r.tick_id);
}

// The surviving tokens rows' first-issuance action_index, by tick_id.
async function tokenAnchors(db, tickIds){
    let out = new Map();
    for(let i = 0; i < tickIds.length; i += CHUNK){
        let part = tickIds.slice(i, i + CHUNK);
        let rows = await db.doQuery("SELECT tick_id, action_index FROM tokens WHERE tick_id IN (" +
            new Array(part.length).fill('?').join(',') + ")", part, null, { rethrow: true });
        for(let r of (rows || [])) out.set(String(r.tick_id), String(r.action_index));
    }
    return out;
}

// Rewrite each collected tick's fold columns from its surviving valid issues. A tick
// with no surviving valid issue is skipped, as the source skips it (getTokenInfo
// returns false); its row, if it had one, went with the dataTables delete. UPDATE only:
// the replica never mints a tokens id the source did not stream.
//
// A tick is refolded only when its FIRST surviving valid issue is the row's own
// first issuance (tokens.action_index). Otherwise this replica does not hold the
// tick's whole history (a truncated SYNC_BOOTSTRAP_DEPTH replica keeps only
// [base..tip] of issues), and a fold over the tail would write a row the source never
// had. Those ticks are returned as skipped, left as they are, and reported by the
// caller; TOKEN_FOLD_PARITY_CHECK is what then shows whether they diverged.
async function refoldTokenRows(db, tickIds){
    let result = { refolded: 0, skipped: [] };
    if(!tickIds || tickIds.length === 0) return result;
    let folded = await foldTicks(db, tickIds);
    let anchors = await tokenAnchors(db, [...folded.keys()]);
    let sets = FOLD_COLUMNS.map(c => c + '=?').join(', ');
    for(let [tick, cols] of folded){
        if(!anchors.has(tick)) continue;
        if(anchors.get(tick) !== String(cols.first_action_index)){ result.skipped.push(tick); continue; }
        await db.doQuery("UPDATE tokens SET " + sets + " WHERE tick_id=?",
            FOLD_COLUMNS.map(c => cols[c]).concat([tick]), null, { rethrow: true });
        result.refolded++;
    }
    return result;
}

// Advisory parity digest input: every tokens row's fold columns (supply and the
// non-fold columns excluded, since they move outside the fold) in tick_id order.
// Rows whose last_action_index is above `maxActionIndex` are left out and returned by
// tick_id, so a source whose indexer has run past the published height can name the
// ticks it edited since, and the follower can leave out the same ones.
async function tokenFoldRows(db, maxActionIndex, excludeTickIds){
    let rows = await db.doQuery(
        "SELECT tick_id, action_index, " + FOLD_COLUMNS.join(', ') + " FROM tokens ORDER BY tick_id ASC",
        [], null, { rethrow: true });
    let skip = new Set((excludeTickIds || []).map(String));
    let kept = [], ahead = [];
    for(let r of (rows || [])){
        let tick = String(r.tick_id);
        let beyond = (maxActionIndex !== null && maxActionIndex !== undefined && r.last_action_index !== null &&
                      BigInt(String(r.last_action_index)) > BigInt(String(maxActionIndex)));
        if(beyond) ahead.push(tick);
        if(beyond || skip.has(tick)) continue;
        let o = { tick_id: tick, action_index: r.action_index === null ? null : String(r.action_index) };
        for(let c of FOLD_COLUMNS) o[c] = (r[c] === null || r[c] === undefined) ? null : String(r[c]);
        kept.push(o);
    }
    return { rows: kept, ahead: ahead };
}

module.exports = { FOLD_COLUMNS, foldIssueRows, collectIssueTickIds, refoldTokenRows, tokenFoldRows };
