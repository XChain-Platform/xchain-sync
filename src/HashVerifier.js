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

    // Advisory: compare local vs remote per-table content checksums (NON-consensus,
    // ). Both sides are BlockHasher.computeTableContentChecksums results over
    // the SAME window and the same published bounds.
    //
    // The comparison is deliberately gated on EQUAL ROW COUNTS, and that gate is what
    // makes the signal trustworthy rather than noisy:
    //
    //   - counts differ  -> a completeness gap or a bound the two sides read a beat
    //     apart (a source one block ahead has already inserted or deleted inside the
    //     window). Reported as SKIPPED, never as a mismatch: incompleteness is what
    //     the /status row-count check exists for, and the source having advanced is
    //     not a defect at all.
    //   - table on one side only -> same reasoning, skipped with its own reason so an
    //     operator can tell "the source has rows here and we have none" from a
    //     genuine content fork.
    //   - counts equal, digests differ -> CONTENT DIVERGENCE. This is the case no
    //     other check on the follower can see, and the only one reported.
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

    // Verify hash chain continuity between a stored previous block and a new payload
    // prevHashes: { ledger_hash, actions_hash, contract_hash } from the client's last applied block
    // payload: incoming block event with hashes
    // Returns { valid: bool, reason: string|null }
    verifyChainContinuity(prevBlockIndex, prevHashes, payload){
        // If no previous block, can't verify continuity (first block after bootstrap)
        if(prevBlockIndex === null || prevHashes === null)
            return { valid: true, reason: null };

        // Guard against null/undefined payload
        if(!payload || payload.block_index === undefined || payload.block_index === null)
            return { valid: false, reason: 'Invalid payload: missing block_index' };

        // The new block should be exactly prevBlockIndex + 1 for continuity
        if(payload.block_index !== prevBlockIndex + 1){
            return {
                valid: false,
                reason: 'Block gap: expected ' + (prevBlockIndex + 1) + ', got ' + payload.block_index
            };
        }

        // Hash chain verification: the indexer includes previous_hash in each block's hash
        // computation, so if we verify the hash chain across our stored blocks, we can detect
        // any tampering. However, we can't directly verify the previous_hash embedding without
        // recomputing the hash from raw data (which we don't have on the client side).
        //
        // Instead, we rely on:
        // 1. Cross-source comparison (two honest sources produce identical hashes)
        // 2. The hash chain property (any modification cascades to all subsequent hashes)
        //
        // The continuity check here just verifies sequential block ordering.
        return { valid: true, reason: null };
    }
}

module.exports = HashVerifier;
