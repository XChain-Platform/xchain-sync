// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

// Tracks rollback depth cumulatively across reorg events. A source that splits one
// deep rewind into several shallow events would pass a per-event depth limit, so the
// depth is measured from the highest tip held before the first rewind of the streak.
// The streak ends once the replica re-advances to that tip, or climbs a full limit
// above the lowest rewind target.
class RollbackGuard {
    constructor(maxDepth){
        this.maxDepth = maxDepth;
        this.peak = null;
        this.low = null;
    }

    // Returns the cumulative depth the event would produce; does not record it.
    depthFor(tip, target){
        this.expire(tip);
        let peak = this.peak === null ? tip : this.peak;
        return peak - target + 1;
    }

    // Records an accepted rollback to `target` from `tip`.
    record(tip, target){
        this.expire(tip);
        if(this.peak === null) this.peak = tip;
        this.low = this.low === null ? target : Math.min(this.low, target);
    }

    exceeds(tip, target){
        return this.depthFor(tip, target) > this.maxDepth;
    }

    toState(){
        return {
            peak: this.peak,
            low: this.low,
        };
    }

    restoreState(state){
        this.reset();
        if(!state || typeof state !== 'object') return;
        if(state.peak === null && state.low === null) return;
        if(!Number.isSafeInteger(state.peak) || state.peak < 0) return;
        if(!Number.isSafeInteger(state.low) || state.low < 0 || state.low > state.peak) return;
        this.peak = state.peak;
        this.low = state.low;
    }

    expire(tip){
        if(this.peak === null) return;
        if(tip >= this.peak || tip - this.low >= this.maxDepth) this.reset();
    }

    reset(){
        this.peak = null;
        this.low = null;
    }
}

module.exports = RollbackGuard;
