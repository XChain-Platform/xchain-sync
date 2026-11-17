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
 * XChain Sync - retry pacing and outage logging
 *
 * A loop that keeps retrying a peer that is down must neither hammer the peer
 * nor fill the disk with one log line per attempt. backoffDelay() paces the
 * retries (the first after the configured delay, then doubling to a cap), and
 * OutageLog turns a run of failures into a few lines: one when the outage
 * starts, one summary per window while it lasts, and one when it ends.
 *
 ********************************************************************/

'use strict';

const { getLogger } = require('../observability');

// Doubling starts from at least 1 s, so a tiny configured delay cannot make a storm.
const BACKOFF_FLOOR_MS   = 1000;
const BACKOFF_CAP_MS     = 60000;
const SUMMARY_EVERY_MS   = 5 * 60 * 1000;

// Delay before retry number `attempt` (0 = the first): the first waits baseMs,
// each later one doubles from at least 1 s and never passes the cap. A base
// above the cap is kept as is, so an operator's long fixed delay still holds.
function backoffDelay(attempt, baseMs, capMs){
    const base = (Number.isFinite(baseMs) && baseMs >= 0) ? baseMs : BACKOFF_FLOOR_MS;
    const cap  = Math.max(base, (Number.isFinite(capMs) && capMs > 0) ? capMs : BACKOFF_CAP_MS);
    if(!(attempt > 0)) return base;
    return Math.min(cap, Math.max(base, BACKOFF_FLOOR_MS) * Math.pow(2, Math.min(attempt, 30)));
}

// Short text for an error: its code, message and HTTP status only. An HTTP
// client error carries the request config (headers included), so it is never
// printed whole.
function errorText(e){
    if(e === null || e === undefined) return 'unknown error';
    if(typeof e !== 'object') return String(e);
    const parts = [];
    const message = e.message ? String(e.message) : '';
    if(e.code && !message.includes(String(e.code))) parts.push(String(e.code));
    if(message) parts.push(message);
    if(e.response && e.response.status) parts.push('HTTP ' + e.response.status);
    return parts.length ? parts.join(' ') : 'unknown error';
}

function formatMs(ms){
    if(ms < 1000) return ms + 'ms';
    if(ms < 120000) return Math.round(ms / 1000) + 's';
    return Math.round(ms / 60000) + 'm';
}

// Log state for one retried target. Callers count each failed attempt with
// failure() and close the outage with recovered(); only state changes and one
// summary per window reach the log.
class OutageLog {
    constructor(label, options){
        const opts = options || {};
        this.label = label;
        this.level = opts.level || 'warn';
        this.summaryEveryMs = opts.summaryEveryMs || SUMMARY_EVERY_MS;
        this.reset();
    }

    reset(){
        this.since = null;
        this.failures = 0;
        this.lastLoggedAt = 0;
    }

    get active(){ return this.since !== null; }

    // Count one failed attempt. Logs the first one, then at most one summary per
    // window; nextDelayMs (optional) names when the next attempt runs.
    failure(detail, nextDelayMs){
        const now = Date.now();
        const next = (typeof nextDelayMs === 'number') ? '; next attempt in ' + formatMs(nextDelayMs) : '';
        this.failures += 1;
        if(this.since === null){
            this.since = now;
            this.lastLoggedAt = now;
            getLogger()[this.level](this.label + ' unavailable (' + detail + ')' + next +
                ', backing off; repeats are summarized every ' + formatMs(this.summaryEveryMs));
            return true;
        }
        if(now - this.lastLoggedAt < this.summaryEveryMs) return false;
        this.lastLoggedAt = now;
        getLogger()[this.level](this.label + ' still unavailable after ' + this.failures +
            ' failed attempt(s) over ' + formatMs(now - this.since) + ' (last: ' + detail + ')' + next);
        return true;
    }

    // End the outage with one line; silent when there was none.
    recovered(){
        if(this.since === null) return false;
        getLogger().info(this.label + ' available again after ' + this.failures +
            ' failed attempt(s) over ' + formatMs(Date.now() - this.since));
        this.reset();
        return true;
    }
}

module.exports = { backoffDelay, errorText, OutageLog, BACKOFF_CAP_MS, SUMMARY_EVERY_MS };
