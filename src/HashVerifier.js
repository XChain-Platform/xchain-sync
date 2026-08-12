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
 * XChain Indexer Sync - Hash Verifier
 *
 * Cross-source hash comparison and hash chain continuity verification.
 * Uses the indexer's existing per-block chained SHA256 hashes.
 *
 ********************************************************************/

class HashVerifier {

    constructor(){}

    // Returns { match: bool, mismatches: [...] }
    compareBlockHashes(blockHeight, hashesA, hashesB){
        let mismatches = [];

        if(hashesA.ledger_hash !== hashesB.ledger_hash)
            mismatches.push({ field: 'ledger_hash', a: hashesA.ledger_hash, b: hashesB.ledger_hash });
        if(hashesA.actions_hash !== hashesB.actions_hash)
            mismatches.push({ field: 'actions_hash', a: hashesA.actions_hash, b: hashesB.actions_hash });
        if(hashesA.contract_hash !== hashesB.contract_hash)
            mismatches.push({ field: 'contract_hash', a: hashesA.contract_hash, b: hashesB.contract_hash });

        return {
            match: mismatches.length === 0,
            blockHeight: blockHeight,
            mismatches: mismatches
        };
    }

    // Advisory: compare the local vs remote index-map parity checksum (NON-consensus).
    // A mismatch means this replica's id->address map content diverged from the
    // source's authoritative map (e.g. a local INSERT IGNORE that kept a pre-existing
    // colliding id and dropped the source's row). It is NOT a consensus fault: the
    // ledger/actions/contract hashes resolve ids to strings, so they still agree.
    // Caller logs + counts a mismatch and CONTINUES; it must never halt on this.
    compareIndexMap(blockHeight, localChecksum, remoteChecksum){
        return {
            match:       localChecksum === remoteChecksum,
            blockHeight: blockHeight,
            local:       localChecksum,
            remote:      remoteChecksum
        };
    }

    // Advisory: compare local vs remote per-table content checksums (NON-consensus).
    // Both sides are BlockHasher.computeTableContentChecksums results over the SAME
    // window and the same published bounds.
    //
    // Gating on EQUAL ROW COUNTS is what makes the signal trustworthy rather than
    // noisy. Differing counts, or a table present on one side only, mean a
    // completeness gap or a bound the two sides read a beat apart (a source one block
    // ahead has already inserted or deleted inside the window); those are reported as
    // SKIPPED with a reason, never as a mismatch, because incompleteness is what the
    // /status row-count check exists for. Equal counts with differing digests is
    // CONTENT DIVERGENCE, the case no other follower check can see, and the only one
    // reported.
    //
    // Returns { match, blockHeight, compared, mismatches: [...], skipped: [...] }.
    // The caller logs + counts a mismatch and CONTINUES; it must never halt on this.
    compareTableContent(blockHeight, local, remote){
        let localTables  = (local  && local.tables)  || {};
        let remoteTables = (remote && remote.tables) || {};
        let mismatches = [], skipped = [], compared = 0;

        for(let table of [...new Set([...Object.keys(localTables), ...Object.keys(remoteTables)])].sort()){
            let l = localTables[table], r = remoteTables[table];
            if(!l || !r){
                skipped.push({ table: table, reason: l ? 'absent-on-source' : 'absent-locally' });
                continue;
            }
            if(Number(l.n) !== Number(r.n)){
                skipped.push({ table: table, reason: 'row-count-differs', local: Number(l.n), remote: Number(r.n) });
                continue;
            }
            compared++;
            if(l.h !== r.h)
                mismatches.push({ table: table, rows: Number(l.n), local: l.h, remote: r.h });
        }

        return {
            match:       mismatches.length === 0,
            blockHeight: blockHeight,
            compared:    compared,
            mismatches:  mismatches,
            skipped:     skipped
        };
    }

    // prevHashes is { ledger_hash, actions_hash, contract_hash } from the client's last
    // applied block; returns { valid, reason: string|null }.
    verifyChainContinuity(prevBlockIndex, prevHashes, payload){
        // No previous block means the first block after bootstrap, nothing to chain to.
        if(prevBlockIndex === null || prevHashes === null)
            return { valid: true, reason: null };

        if(!payload || payload.block_index === undefined || payload.block_index === null)
            return { valid: false, reason: 'Invalid payload: missing block_index' };

        if(payload.block_index !== prevBlockIndex + 1){
            return {
                valid: false,
                reason: 'Block gap: expected ' + (prevBlockIndex + 1) + ', got ' + payload.block_index
            };
        }

        // Sequential ordering is ALL this verifies. The indexer folds previous_hash into
        // each block's hash, but confirming that embedding would mean recomputing from
        // raw data the client does not hold. Tamper detection therefore rests on the
        // cross-source comparison (two honest sources produce identical hashes) plus the
        // chain property that any modification cascades into every later hash.
        return { valid: true, reason: null };
    }
}

module.exports = HashVerifier;
