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
 * XChain Sync - durable halt write (replica side)
 *
 * A halt is only as good as its sync_halt row: start() reads that row to stay
 * halted across a restart. When the write fails, the halt stays in force in
 * memory (nothing applies), isHaltDurable() turns false (SyncService's client
 * state and the /health row report it as halt_durable), and:
 *   - a copy of the halt goes to sync_state (halt_pending:<dbType>), best
 *     effort; a restart that finds the copy restores the halt before any
 *     bootstrap or catch-up (an empty replica is not re-seeded under it) and
 *     writes sync_halt again. A copy that cannot be read holds the client, as
 *     an unreadable sync_halt does. An operator clear removes the copy;
 *   - the sync_halt write is retried on a timer until it lands, the operator
 *     clears the halt, or the client stops.
 * A halt on a block that fails its own hashes, or on a stored-tip state_hash,
 * is also covered by the verified-tip marker, which never rose past that
 * block: a restart that loses both writes re-verifies the block and halts
 * again. A halt of any other kind (no source quorum, train activation, a
 * checkpoint) survives a restart only through sync_halt or the sync_state copy.
 *
 * A Mixin: the methods below are installed on ClientSync.prototype by
 * client/sync.js, so `this` is the ClientSync instance.
 *
 ********************************************************************/

const util = require('node:util');
const { getLogger } = require('../observability');

const FIRST_RETRY_MS = 5000;
const MAX_RETRY_MS   = 60000;

module.exports = {

    pendingHaltKey(){ return 'halt_pending:' + this.dbType; },

    // Record `halt` (the object just installed as this._halted) in sync_halt.
    async persistHalt(halt){
        try {
            await this.db.recordHalt(this.dbType, halt.blockIndex, halt.reason, halt.mismatches, halt.sources);
            this._haltDurable = true;
            await this.clearPendingHaltCopy();
        } catch(e){
            this._haltDurable = false;
            getLogger().error(util.format('CRITICAL: failed to persist ' + halt.reason + ' halt for ' + this.replicaLabel() +
                ' (halted in memory; retrying the write every ' + (FIRST_RETRY_MS / 1000) + 's or more until it lands):', e));
            await this.writePendingHaltCopy(halt);
            this.scheduleHaltPersistRetry(halt, FIRST_RETRY_MS);
        }
    },

    // False while a halt is in force whose sync_halt row has not been written.
    isHaltDurable(){ return !this._halted || this._haltDurable !== false; },

    // Best effort: sync_state may be writable when sync_halt is not.
    async writePendingHaltCopy(halt){
        if(!this.db || typeof this.db.setSyncState !== 'function') return;
        this._pendingHaltCopy = true;
        await this.db.setSyncState(this.pendingHaltKey(), JSON.stringify({
            blockIndex: halt.blockIndex, reason: halt.reason, mismatches: halt.mismatches,
            sources: halt.sources, at: halt.at
        }));
    },

    // Drop the copy this process wrote or restored once sync_halt holds the halt; a halt
    // that was durable at once never had one, so the common path touches nothing.
    async clearPendingHaltCopy(){
        if(!this._pendingHaltCopy) return;
        await this.dropPendingHaltCopy();
    },

    // Remove the copy whoever wrote it (an operator clear). deleteSyncState fails soft, so
    // a delete that reports failure is followed by overwriting the copy with an empty
    // value, which restorePendingHalt reads as no copy. When both fail the copy survives
    // and the next start halts again (fail closed), which the log names.
    async dropPendingHaltCopy(){
        this._pendingHaltCopy = false;
        if(!this.db) return;
        let dropped = typeof this.db.deleteSyncState === 'function'
            && await this.db.deleteSyncState(this.pendingHaltKey()) !== false;
        if(!dropped && typeof this.db.setSyncState === 'function')
            dropped = await this.db.setSyncState(this.pendingHaltKey(), '') !== false;
        if(!dropped) getLogger().error('Could not remove the ' + this.pendingHaltKey() + ' halt copy for ' +
            this.replicaLabel() + '; the next start restores the cleared halt');
    },

    // At startup, before any bootstrap or catch-up: a sync_state copy of a halt whose
    // sync_halt write never landed puts the halt back in force and writes sync_halt
    // again. The read fails closed, as the sync_halt read does: a read error holds the
    // client as 'halt-state-check-failed' rather than reading as "no copy". True when the
    // client is now halted.
    async restorePendingHalt(){
        if(this._halted || !this.db || typeof this.db.getSyncState !== 'function') return false;
        let raw;
        try { raw = await this.db.getSyncState(this.pendingHaltKey(), { rethrow: true }); }
        catch(e){
            getLogger().error(util.format('halt copy read failed for ' + this.replicaLabel() +
                '; staying HALTED (fail-closed) until it can be read:', e));
            this._halted = { blockIndex: -1, reason: 'halt-state-check-failed', mismatches: [], sources: [], at: null };
            return true;
        }
        if(raw === null || raw === '') return false;
        let saved = null;
        try { saved = JSON.parse(raw); } catch(e){ saved = null; }
        if(!saved || typeof saved !== 'object' || typeof saved.reason !== 'string'){
            // Only this module writes the copy, so a malformed one is damage, not absence:
            // hold, and let an operator clear (which drops the copy) decide.
            getLogger().error('Unreadable ' + this.pendingHaltKey() + ' halt copy for ' + this.replicaLabel() +
                '; staying HALTED until an operator clears it');
            this._pendingHaltCopy = true;
            this._halted = { blockIndex: -1, reason: 'halt-copy-unreadable', mismatches: [], sources: [], at: null };
            return true;
        }
        this._pendingHaltCopy = true;
        this._halted = {
            blockIndex: Number(saved.blockIndex), reason: saved.reason,
            mismatches: saved.mismatches || [], sources: saved.sources || [], at: saved.at || null
        };
        getLogger().error('ClientSync for ' + this.replicaLabel() + ' restored a ' + saved.reason + ' halt at block ' +
            saved.blockIndex + ' whose sync_halt write never landed. Not resuming until cleared.');
        await this.persistHalt(this._halted);
        return true;
    },

    scheduleHaltPersistRetry(halt, delayMs){
        this.cancelHaltPersistRetry();
        if(this._stopping) return;
        this._haltPersistTimer = setTimeout(() => {
            this._haltPersistTimer = null;
            this.retryHaltPersist(halt, delayMs);
        }, delayMs);
        if(typeof this._haltPersistTimer.unref === 'function') this._haltPersistTimer.unref();
    },

    // One retry. Skipped when the halt it was written for is no longer in force
    // (cleared or replaced). A write already in flight when a clear lands can still
    // leave an active sync_halt row, which keeps the next start halted (fail closed).
    async retryHaltPersist(halt, delayMs){
        if(this._halted !== halt) return;
        try {
            await this.db.recordHalt(this.dbType, halt.blockIndex, halt.reason, halt.mismatches, halt.sources);
            if(this._halted !== halt) return;
            this._haltDurable = true;
            await this.clearPendingHaltCopy();
            getLogger().warn('Halt at block ' + halt.blockIndex + ' (' + halt.reason + ') for ' +
                this.replicaLabel() + ' is now durable in sync_halt');
        } catch(e){
            getLogger().error(util.format('Halt write retry failed for ' + this.replicaLabel() + ':', e));
            this.scheduleHaltPersistRetry(halt, Math.min(delayMs * 2, MAX_RETRY_MS));
        }
    },

    cancelHaltPersistRetry(){
        if(this._haltPersistTimer){
            clearTimeout(this._haltPersistTimer);
            this._haltPersistTimer = null;
        }
    },
};
