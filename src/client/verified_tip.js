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
 * XChain Sync - durable last-verified block (replica side, indexer)
 *
 * A replica commits a block (live apply or a catch-up range) before it
 * recomputes the block's consensus hashes, so the highest committed block is
 * not the highest verified one. The sync_state key verified_tip:<dbType>
 * records the highest block whose own rows reproduced its committed hashes,
 * every block below it included. It only rises after a recompute passes, and
 * it is lowered durably before a rollback deletes rows above it.
 *
 * Every committed block above the marker is recomputed, in ascending order,
 * before the replica advances: at startup before any catch-up, at the start
 * of each catch-up pass, after a catch-up range or a from-height bootstrap
 * window applies, and before a live block applies. The stored tip also has
 * its stored state_hash recomputed, the one block where that is possible
 * (later blocks mutate its inputs in place). The source's state-commitment
 * roots are never stored on the replica, so that live check has no committed
 * counterpart to re-run. That one walk is what makes a crash between commit
 * and verification, a recompute that errored, a lost halt write, and a halt
 * clear all end the same way as an uninterrupted run: a block that fails its
 * own hashes halts the replica wherever the process stopped.
 *
 * Walks run beside the apply lock, not under it. A reorg rollback or a full
 * snapshot opens a row removal first (beginRowRemoval), which bumps a row
 * generation; a walk that sees the generation move drops what it read,
 * without a halt and without raising the marker.
 *
 * A Mixin: the methods below are installed on ClientSync.prototype by
 * client/sync.js, so `this` is the ClientSync instance. Inert unless the
 * client is an indexer with VERIFY_RECOMPUTE on.
 *
 ********************************************************************/

const util = require('node:util');
const { getLogger } = require('../observability');

// Key an earlier build wrote for one committed-but-unverified block. Read once, on
// the first start without a verified_tip marker, so that block is re-verified too.
const LEGACY_PENDING_PREFIX = 'post_commit_verify_pending:';

function parseHeight(raw){
    let n = (raw === null || raw === undefined || raw === '') ? NaN : Number(raw);
    return Number.isInteger(n) ? n : null;
}

// A walk writes the marker it reached every MARKER_WRITE_STEP verified blocks, as well
// as at the end of each window, so a restart loses at most this many blocks of progress.
const MARKER_WRITE_STEP = 50;

// One macrotask, so a long walk lets WebSocket and HTTP work run between windows.
function yieldToEventLoop(){
    return new Promise(resolve => setImmediate(resolve));
}

module.exports = {

    verifiedTipKey(){ return 'verified_tip:' + this.dbType; },

    tracksVerifiedTip(){ return this.dbType === 'indexer' && !!this.config['VERIFY_RECOMPUTE']; },

    verifyBatchSize(){ return this.numericSetting('VERIFY_RANGE_BATCH', 500, 1); },

    // Read the durable marker. A failed read throws: an unreadable marker is not an
    // absent one, and reading it as absent would vouch for the stored tip unverified.
    async loadVerifiedTip(){
        let raw = (this.db && typeof this.db.getSyncState === 'function')
            ? await this.db.getSyncState(this.verifiedTipKey(), { rethrow: true }) : null;
        this._verifiedTip = parseHeight(raw);
        this._persistedVerifiedTip = this._verifiedTip;
        return this._verifiedTip;
    },

    // Generation of the replica's rows: bumped whenever rows are about to be deleted or
    // replaced (a reorg rollback, a full-snapshot apply) and when the marker is lowered
    // or reset. A walk records it when it starts and drops everything it computed once
    // it moves, so a recompute that may have read rows mid-delete never halts the
    // replica and never raises the marker.
    rowGeneration(){ return this._rowGeneration || 0; },

    rowsChangedSince(generation){
        return this.rowGeneration() !== generation || (this._rowRemovalsInFlight || 0) > 0;
    },

    bumpRowGeneration(){ this._rowGeneration = this.rowGeneration() + 1; },

    // Marker writes run one at a time in call order, so a raise issued before a lowering
    // can never land after it.
    queueMarkerWrite(fn){
        let run = (this._markerWrites || Promise.resolve()).then(fn, fn);
        this._markerWrites = run.catch(() => {});
        return run;
    },

    // Raise the marker to n once blocks up to n recomputed clean, unless the rows changed
    // since `generation` (when given). A failed durable write is logged by setSyncState
    // and leaves the stored marker lower, which costs a re-verification after a restart
    // and never skips one.
    advanceVerifiedTip(n, generation){
        if(typeof n !== 'number') return Promise.resolve();
        return this.queueMarkerWrite(async () => {
            if(generation !== undefined && this.rowsChangedSince(generation)) return;
            if(typeof this._verifiedTip === 'number' && n <= this._verifiedTip) return;
            this._verifiedTip = n;
            await this.writeVerifiedTip(n);
        });
    },

    // Durable write of the marker; fail-soft (setSyncState logs a failure).
    async writeVerifiedTip(n){
        if(!this.db || typeof this.db.setSyncState !== 'function') return;
        if(await this.db.setSyncState(this.verifiedTipKey(), String(n)) !== false) this._persistedVerifiedTip = n;
    },

    // Record that block h verified, in memory, while the rows are unchanged since the
    // walk began (the caller checks that with no await in between). Progress made this
    // way survives a walk that a later row removal abandons: the next walk resumes above
    // it, and a lowering that reaches below it still moves it down.
    noteVerifiedBlock(h){
        if(typeof this._verifiedTip !== 'number' || h > this._verifiedTip) this._verifiedTip = h;
    },

    // Write the in-memory marker durably when it is ahead of the last durable write and
    // the rows have not changed since `generation`.
    persistVerifiedTip(generation){
        return this.queueMarkerWrite(async () => {
            if(this.rowsChangedSince(generation)) return;
            if(typeof this._verifiedTip !== 'number' || this._verifiedTip === this._persistedVerifiedTip) return;
            await this.writeVerifiedTip(this._verifiedTip);
        });
    },

    // Lower the marker to n BEFORE rows above n are deleted. Durable first and strict:
    // a failed write throws so the caller does not delete, because a marker left above
    // replaced rows would vouch for blocks that were never recomputed.
    async lowerVerifiedTip(n){
        if(!this.tracksVerifiedTip()) return;
        this.bumpRowGeneration();
        if(this._verifiedTip === undefined) await this.loadVerifiedTip();
        return this.queueMarkerWrite(async () => {
            if(typeof this._verifiedTip === 'number' && this._verifiedTip <= n) return;
            if(this.db && typeof this.db.setSyncState === 'function')
                await this.db.setSyncState(this.verifiedTipKey(), String(n), { rethrow: true });
            this._verifiedTip = n;
            this._persistedVerifiedTip = n;
        });
    },

    // Open a row removal: call under the apply lock before the first delete, and close it
    // with endRowRemoval in a finally. Walks see the removal and stop without a verdict;
    // with n, the marker is lowered to n first (a failed lowering throws, nothing opens).
    async beginRowRemoval(n){
        this.bumpRowGeneration();
        this._rowRemovalsInFlight = (this._rowRemovalsInFlight || 0) + 1;
        try { if(typeof n === 'number') await this.lowerVerifiedTip(n); }
        catch(e){ this.endRowRemoval(); throw e; }
    },

    endRowRemoval(){
        this._rowRemovalsInFlight = Math.max(0, (this._rowRemovalsInFlight || 0) - 1);
        this.bumpRowGeneration();
    },

    // Set the marker to n after a bootstrap (it may move down): the join floor of a
    // from-height window before the window lands (strict: a failed write throws before
    // any row is applied), or the tip of a full snapshot (fail-soft: a lagging marker
    // only costs a re-verification).
    async adoptVerifiedTip(n, opts){
        if(!this.tracksVerifiedTip() || typeof n !== 'number') return;
        this.bumpRowGeneration();
        let strict = !!(opts && opts.strict);
        return this.queueMarkerWrite(async () => {
            let written = true;
            if(this.db && typeof this.db.setSyncState === 'function')
                written = await this.db.setSyncState(this.verifiedTipKey(), String(n), strict ? { rethrow: true } : undefined) !== false;
            this._verifiedTip = n;
            if(written) this._persistedVerifiedTip = n;
        });
    },

    // A catch-up onto an empty replica: nothing below the requested range exists, so
    // the walk after the apply starts at the range's first block.
    startVerifiedTipBelow(sinceBlock){
        if(this.tracksVerifiedTip()) this._verifiedTip = sinceBlock - 1;
    },

    // Recompute every committed block above the marker. Returns 'ok' when the stored
    // tip is verified, 'halted' when a block failed (the halt is recorded), or 'held'
    // when the walk could not finish (a read fault, an unresolvable committed hash, a
    // stored-tip state_hash that could not be computed, rows removed mid-walk, a stop
    // request); on 'held' the caller must not advance the replica. Concurrent callers
    // share one walk.
    async verifyUnverifiedTail(){
        if(!this.tracksVerifiedTip()) return 'ok';
        if(this._halted) return 'halted';
        if(!this._tailVerify)
            this._tailVerify = this.walkUnverifiedTail().finally(() => { this._tailVerify = null; });
        return this._tailVerify;
    },

    // True when the in-memory marker already covers the in-memory tip, so the live
    // path can skip the database read on every block in steady state.
    verifiedThroughLastApplied(){
        return typeof this._verifiedTip === 'number' && this._verifiedTip === this.lastAppliedBlock;
    },

    async walkUnverifiedTail(){
        let generation = this.rowGeneration();
        if(this.rowsChangedSince(generation)) return 'held';
        let tip;
        let seeded = false;
        try {
            tip = await this.db.getLastBlock(null, { rethrow: true });
            if(tip === null) return 'ok';
            if(this._verifiedTip === undefined) await this.loadVerifiedTip();
            if(this._verifiedTip === null || this._verifiedTip > tip){
                await this.seedVerifiedTip(tip);
                seeded = true;
            }
        } catch(e){
            getLogger().error(util.format('Verified-tip check for ' + this.replicaLabel() +
                ' could not read the stored tip or marker; holding the replica where it is:', e));
            return 'held';
        }
        let start = this._verifiedTip + 1;
        if(typeof this._bootstrapBase === 'number' && start < this._bootstrapBase) start = this._bootstrapBase;
        if(start > tip) return 'ok';
        getLogger().warn('Re-verifying committed blocks ' + start + '..' + tip + ' of ' + this.replicaLabel() +
            ' above the last verified block before advancing');
        let verdict = await this.verifyCommittedRange(start, tip, generation);
        if(verdict === 'ok' && seeded) await this.dropLegacyPendingMarker();
        return verdict;
    },

    // First walk on a replica with no marker: an upgrade from a build without one, or a
    // marker above the stored tip after an out-of-band reseed. Stored history is
    // trusted as it was before the marker existed; the walk starts at the tip block,
    // or lower when the earlier build left a committed-but-unverified block key.
    async seedVerifiedTip(tip){
        let start = tip;
        let legacy = null;
        if(this.db && typeof this.db.getSyncState === 'function')
            legacy = parseHeight(await this.db.getSyncState(LEGACY_PENDING_PREFIX + this.dbType));
        if(legacy !== null && legacy < start) start = legacy;
        getLogger().warn('No verified-tip marker for ' + this.replicaLabel() + ' at stored tip ' + tip +
            '; recomputing from block ' + start + ' and recording the marker from there');
        this._verifiedTip = start - 1;
    },

    async dropLegacyPendingMarker(){
        if(this.db && typeof this.db.deleteSyncState === 'function')
            await this.db.deleteSyncState(LEGACY_PENDING_PREFIX + this.dbType);
    },

    // Recompute blocks start..end in ascending order, each against its own committed
    // hashes, and the stored state_hash of `end` (the stored tip) as the live path checks
    // it. computeBlockHashes folds the committed hash of the block below, and that block
    // has just recomputed to the same value, so every fold chains the recomputed hash of
    // its predecessor up from the last verified block. Runs in windows of
    // VERIFY_RANGE_BATCH blocks: the marker is written after each window, the event loop
    // is yielded, and a stop request ends the walk between windows (never between a
    // verified window and its marker write).
    async verifyCommittedRange(start, end, generation){
        let batch = this.verifyBatchSize();
        for(let lo = start; lo <= end; lo += batch){
            let hi = Math.min(lo + batch - 1, end);
            let verdict = await this.verifyCommittedWindow(lo, hi, end, generation);
            if(verdict !== 'ok') return verdict;
            if(hi < end){
                getLogger().info('Verified blocks ' + start + '..' + hi + ' of ' + end + ' for ' + this.replicaLabel());
                if(this._stopping) return 'held';
                await yieldToEventLoop();
            }
        }
        return 'ok';
    },

    // One window. The row generation is checked before and after every recompute: a
    // removal that began or ended around a read voids the read, so the window stops with
    // no halt and no further marker write. Each block that verified while the rows were
    // unchanged raises the in-memory marker at once, and the marker is written every
    // MARKER_WRITE_STEP blocks and at the window's end, so frequent reorgs cannot starve
    // the walk: an abandoned window keeps everything verified before the removal.
    async verifyCommittedWindow(lo, hi, tip, generation){
        for(let h = lo; h <= hi; h++){
            if(this.rowsChangedSince(generation)) return this.abandonWalk(h);
            let result = await this.evaluateCommittedBlock(h, { atTip: h === tip });
            if(this.rowsChangedSince(generation)) return this.abandonWalk(h);
            if(result.verdict !== 'ok'){
                await this.persistVerifiedTip(generation);
                return this.settleUnverifiedBlock(h, result);
            }
            this.noteVerifiedBlock(h);
            if((h - lo + 1) % MARKER_WRITE_STEP === 0) await this.persistVerifiedTip(generation);
        }
        await this.persistVerifiedTip(generation);
        return this.rowsChangedSince(generation) ? 'held' : 'ok';
    },

    abandonWalk(h){
        getLogger().warn('Rows of ' + this.replicaLabel() + ' were removed or replaced while block ' + h +
            ' was being verified; dropping this walk without a verdict');
        return 'held';
    },

    async settleUnverifiedBlock(h, result){
        if(result.verdict === 'halt'){
            await this.haltOnDivergence(h, result.mismatches, this.sources.slice(0, 1), result.reason);
            return 'halted';
        }
        if(result.verdict === 'unresolved'){
            getLogger().error('Block ' + h + ' of ' + this.replicaLabel() + ' has no resolvable committed hash; ' +
                'holding the replica at verified block ' + (h - 1) + ' until it resolves');
        }
        return 'held';
    },
};
