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
 * XChain Sync - per-source retry pacing (replica side)
 *
 * Two loops retry a sync source that has gone away: the WebSocket reconnect
 * after a close, and the status check behind a stream that stays open but
 * silent. Both back off (doubling to a 60 s cap, starting over once the source
 * answers) and log only when an outage starts, once per summary window while
 * it lasts, and when it ends. Pacing changes nothing about what is applied:
 * a reconnect still resumes after the committed tip.
 *
 * A Mixin: the methods below are installed on ClientSync.prototype by
 * client/sync.js, so `this` is the ClientSync instance.
 *
 ********************************************************************/

'use strict';

const { backoffDelay, errorText, OutageLog } = require('../util/retry_backoff');

const DEFAULT_RECONNECT_MS = 5000;
const STALE_CHECK_BASE_MS  = 1000;

module.exports = {

    // Retry state for one source, created on first use.
    sourceRetry(sourceIndex){
        if(!this.sourceRetryStates) this.sourceRetryStates = new Map();
        let state = this.sourceRetryStates.get(sourceIndex);
        if(state) return state;
        const name  = (this.sources && this.sources[sourceIndex]) || ('source #' + sourceIndex);
        const scope = ' for ' + this.chain + '/' + this.network + '/' + this.dbType;
        state = {
            reconnects:  0,
            staleChecks: 0,
            lastError:   null,
            connectLog:  new OutageLog('WebSocket to ' + name + scope),
            statusLog:   new OutageLog('Status check of ' + name + scope, { level: 'error' })
        };
        this.sourceRetryStates.set(sourceIndex, state);
        return state;
    },

    // True while reconnects to the source keep failing; per-attempt lines stay quiet then.
    isSourceReconnecting(sourceIndex){
        const state = this.sourceRetryStates && this.sourceRetryStates.get(sourceIndex);
        return Boolean(state && state.connectLog.active);
    },

    // Keep the latest socket error for the outage log's next line.
    noteSourceError(sourceIndex, err){
        this.sourceRetry(sourceIndex).lastError = errorText(err);
    },

    // Delay before the next reconnect. Counts this one as a failed attempt: the
    // first waits CLIENT_RECONNECT_DELAY, later ones double up to the cap.
    nextReconnectDelay(sourceIndex){
        const state = this.sourceRetry(sourceIndex);
        let base = Number(this.config['CLIENT_RECONNECT_DELAY']);
        if(!Number.isFinite(base) || base < 0) base = DEFAULT_RECONNECT_MS;
        const delay = backoffDelay(state.reconnects, base);
        state.reconnects += 1;
        state.connectLog.failure(state.lastError || 'connection closed', delay);
        state.lastError = null;
        return delay;
    },

    // A connect landed: the reconnect backoff starts over and an outage gets its closing line.
    sourceReconnected(sourceIndex){
        const state = this.sourceRetry(sourceIndex);
        state.reconnects = 0;
        state.lastError = null;
        state.connectLog.recovered();
    },

    // Delay before the next status check of a silent stream; `failure` is the
    // read's error, or null when the source answered but its tip had not moved.
    nextStaleCheckDelay(sourceIndex, failure){
        const state = this.sourceRetry(sourceIndex);
        const delay = backoffDelay(state.staleChecks, STALE_CHECK_BASE_MS);
        state.staleChecks += 1;
        if(failure) state.statusLog.failure(errorText(failure), delay);
        return delay;
    },

    // The source answered a status read, so a run of failed reads is over.
    statusCheckAnswered(sourceIndex){
        this.sourceRetry(sourceIndex).statusLog.recovered();
    },

    // A live event proves the stream: the next stale check waits a full window again.
    resetStaleChecks(sourceIndex){
        const state = this.sourceRetryStates && this.sourceRetryStates.get(sourceIndex);
        if(!state) return;
        state.staleChecks = 0;
        state.statusLog.recovered();
    },

    // Drop one source's retry state (eviction), or every source's (stop).
    forgetSourceRetry(sourceIndex){
        if(!this.sourceRetryStates) return;
        if(sourceIndex === undefined) this.sourceRetryStates.clear();
        else this.sourceRetryStates.delete(sourceIndex);
    }
};
