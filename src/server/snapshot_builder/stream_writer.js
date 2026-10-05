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
 * XChain Sync - Snapshot Stream Writer
 * Backpressure and client-abort handling for gzip snapshot streams, plus the
 * BigInt-safe JSON replacer every snapshot row write shares.
 *
 ********************************************************************/

const { once } = require('events');

// JSON replacer that converts BigInt to string (mariadb driver returns BigInt for BIGINT columns)
const bigIntReplacer = (k, v) => typeof v === 'bigint' ? v.toString() : v;

// Guards a gzip snapshot stream against a slow or vanished reader. The snapshot
// routes are unauthenticated behind only a per-IP/hr limiter, so a half-open
// reader that never drains would otherwise (a) buffer the entire snapshot in the
// server's RAM and (b) pin the REPEATABLE READ connection open the whole time,
// bloating InnoDB undo history; a few concurrent slow reads exhaust the pool and
// push the source toward OOM. write() applies real backpressure - it awaits
// 'drain' when the gzip buffer fills instead of writing unconditionally - and
// rejects with an aborted-tagged error the moment the client disconnects, so the
// caller tears the read snapshot down rather than blocking on a drain that will
// never come.
class SnapshotStreamWriter {
    constructor(gzip, res){
        this.gzip = gzip;
        this.aborted = false;
        this._ac = new AbortController();
        this._res = res;
        // A disconnect fires 'close' on the response before the stream drains.
        // Flag it, wake any pending drain wait (via the abort signal), and destroy
        // the gzip stream so its buffered chunks are freed.
        this._onClose = () => this.abort();
        // A gzip error (e.g. a write after the piped socket died) must also release
        // a pending drain wait rather than hang it, and must not throw unhandled.
        this._onError = () => this.abort();
        res.once('close', this._onClose);
        gzip.once('error', this._onError);
    }

    abort(){
        if(this.aborted) return;
        this.aborted = true;
        this._ac.abort();
        if(!this.gzip.destroyed) this.gzip.destroy();
    }

    abortError(){
        let e = new Error('snapshot stream aborted by client disconnect');
        e.aborted = true;
        return e;
    }

    // Write one chunk, applying backpressure. Resolves once the chunk is accepted
    // (immediately if the buffer has room, else after 'drain'). Rejects with an
    // aborted-tagged error if the client has gone away, so the caller stops.
    async write(chunk){
        if(this.aborted) throw this.abortError();
        // write() returns false when the internal buffer crosses the high-water
        // mark: pause and wait for 'drain' so we never outrun a slow reader.
        if(this.gzip.write(chunk)) return;
        try {
            await once(this.gzip, 'drain', { signal: this._ac.signal });
        } catch(e){
            // AbortError (client closed) or a gzip error surfaced by once(): either
            // way the stream is gone, so report it as an abort.
            throw this.abortError();
        }
    }

    // Normal completion: detach the disconnect handlers and flush-close the stream.
    finish(){
        this.detach();
        if(!this.gzip.destroyed) this.gzip.end();
    }

    // Error/abort teardown: detach handlers and drop any buffered output.
    dispose(){
        this.detach();
        if(!this.gzip.destroyed) this.gzip.destroy();
    }

    detach(){
        this._res.removeListener('close', this._onClose);
        this.gzip.removeListener('error', this._onError);
    }
}

module.exports = { SnapshotStreamWriter, bigIntReplacer };
