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
 * XChain Sync - Updated row table specifications
 *
 ********************************************************************/

// Tables carrying the deactivation_block stamp (value-threshold detection).
const DEACTIVATION_TABLES = ['stakes', 'delegations', 'contract_stakes', 'contract_delegations'];

// SLASH amount reductions: each surviving stake/unstake row whose amount was cut
// is reachable by joining its action_index to the slash debit log for this window.
// target_table is the literal the indexer writes (createContractSlashDebit /
// createCapabilitySlashDebit), matching ClientRollback's restore JOIN.
const SLASH_SPECS = [
    { table: 'contract_stakes',   debits: 'contract_slash_debits',  target: 'contract_stakes'   },
    { table: 'contract_unstakes', debits: 'contract_slash_debits',  target: 'contract_unstakes' },
    { table: 'stakes',            debits: 'capability_slash_debits', target: 'stakes'            },
    { table: 'unstakes',          debits: 'capability_slash_debits', target: 'unstakes'          }
];

// Stake-ledger tables the DELEGATE v1 materialization sweep rewrites signing_pubkey_id on
// (CONTRACT_DELEGATION_MATERIALIZE). Each rewrite is journaled in contract_delegation_rotations
// with the table it landed on, exactly as the slash debits are, so both directions (this
// forward carry and ClientRollback's reverse restore) key the same way.
const ROTATION_TABLES = ['contract_stakes', 'contract_unstakes'];

// v0 request rows whose request_status went terminal in this window (resolved_block stamp).
const REQUEST_STATUS_TABLES = ['attests', 'xcalls'];

// VOTE polls whose finalization went terminal in this window. The per-block sweep
// flips a surviving polls row (created at the v0 block, below the window) from
// 'open' to 'finalized'/'failed_quorum' IN PLACE, stamping resolved_block; the
// action-scoped stream carries the v2's poll_results rows but not this summary
// flip. Forward twin of ClientRollback's polls re-open reset (same key). The class
// carries a SECOND key as well: a DEFERRED binding-callback fire (callback_delay_blocks
// > 0) stamps callback_execute_action_index IN PLACE at the due block D = F + delay,
// which is above the finalize window, so keying on resolved_block alone dropped that
// stamp on every follower (forward twin of ClientRollback's timelock re-fire reset).
const POLL_FINALIZE_TABLES = ['polls'];

// Surviving unstake rows whose status_id was flipped to 'completed' in place when their
// cooldown matured (markCooldownsCompleted). Keyed by cooldown_end_block (the maturity
// block), exactly as ClientRollback's reverse reset and cooldownCredits.js's forward
// credit select. action_index is UNIQUE on both, so the follower's upsert lands cleanly.
const COOLDOWN_STATUS_TABLES = ['unstakes', 'contract_unstakes'];

// ATTEST batch rail: a v5 head declares a window and holds slot 0, each v6 continuation
// holds a later slot, and the continuation that COMPLETES the slot coverage reassembles
// the body. A failed reassembly (bad CRC, or a batch quorum that does not verify) is the
// BATCH's fault, so the verdict is stamped IN PLACE on the head, which was written in an
// earlier block. Byte-identical copies of the indexer's own values
// (xchain-indexer/src/actions/attest/attest_batch_wire.js for the versions,
// xchain-indexer/src/actions/attest/index.js for the marker, which
// xchain-indexer/src/rollback.js already keeps a second copy of); keep all copies in step.
const ATTEST_BATCH_HEAD_VERSION         = 5;
const ATTEST_BATCH_CONTINUATION_VERSION = 6;
const ATTEST_BATCH_COMPLETION_STAMP     = ' (stamped on batch completion)';

// BET in-place flips ( P4): a surviving bet_feeds row is mutated in place by
// the closed latch (closed_block stamp, end-of-block pass) and by the terminal flip
// (terminal_block stamp: resolve tx / cancel tx / BET_EXPIRE pass); a surviving bets
// row is mutated by settlement (settled_block stamp: won/lost/refunded). Each class
// is keyed by its stamp landing in the window, mirroring (forward) the exact
// ClientRollback resets and the state_hash bet_feed_status/bet_status classes.
// status_id is not hashed raw; the follower's upsert resolves it via the replicated
// index_statuses table.
const BET_STATUS_SPECS = [
    { table: 'bet_feeds', stamps: ['closed_block', 'terminal_block'] },
    { table: 'bets',      stamps: ['settled_block'] }
];

module.exports = {
    DEACTIVATION_TABLES, SLASH_SPECS, ROTATION_TABLES, REQUEST_STATUS_TABLES,
    POLL_FINALIZE_TABLES, COOLDOWN_STATUS_TABLES, ATTEST_BATCH_HEAD_VERSION,
    ATTEST_BATCH_CONTINUATION_VERSION, ATTEST_BATCH_COMPLETION_STAMP, BET_STATUS_SPECS
};
