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
 * XChain Sync - Hub Client Config Fetch
 *
 * Fetches config deltas, handles endpoint and watermark regressions,
 * and verifies the hub's consensus hashes against the local bundle.
 *
 ********************************************************************/

const coins = require('../../coins');
const { getLogger } = require('../../observability');
const logger = getLogger();

// Local { coin -> consensusHash } per network, computed on first use. The vendored
// bundle cannot change under a running process, so re-hashing it on every config
// poll would be pure waste.
const LOCAL_CONSENSUS_HASHES = {};
function localConsensusHashes(network){
    if(!LOCAL_CONSENSUS_HASHES[network]) LOCAL_CONSENSUS_HASHES[network] = coins.consensusHashes(network);
    return LOCAL_CONSENSUS_HASHES[network];
}

// Fold a getallconfigs delta (only the rows that changed since our cursor) into
// the cached nested config map, mutating and returning `base`. The hub's configs
// table is upsert-only (rows are never deleted), so applying successive deltas
// reconstructs exactly the tree a full fetch would have produced.
function mergeConfigDelta(base, delta){
    for(let coin in delta){
        if(!base[coin]) base[coin] = {};
        for(let network in delta[coin]){
            if(!base[coin][network]) base[coin][network] = {};
            for(let module in delta[coin][network]){
                if(!base[coin][network][module]) base[coin][network][module] = {};
                let params = delta[coin][network][module];
                for(let param in params){
                    base[coin][network][module][param] = params[param];
                }
            }
        }
    }
    return base;
}

// If call failed over to a different endpoint than the one our cursor came
// from, the delta we just received was filtered against a stale cross-hub
// cursor and may have skipped rows (each hub's updated_at for the same config
// differs). Discard it and re-fetch the full tree from the new endpoint with a
// reset cursor; the configs table is small and the merge is idempotent.
async function refetchAfterEndpointChange(hub, result, sentCursor, cursorEndpoint){
    if(sentCursor > 0 && hub._lastGoodIdx !== cursorEndpoint){
        hub.lastWatermark = 0;
        hub.configs       = null;
        result = await hub.call({
            jsonrpc: '2.0',
            method:  'getallconfigs',
            params:  hub.configParams(0),
            id:      1
        }, 10000);
    }
    return result;
}

// Hub restart / restore from an older snapshot: the same endpoint now serves a
// seq or watermark BELOW the last one it gave us. The delta we asked for (cursor
// from the lost window) cannot carry rows the restored hub holds at an OLDER
// updated_at, and mergeConfigDelta only upserts, so merging it would serve
// lost-window values forever while lastSuccessfulFetchAt stamps fresh.
// Mirror the indexer's / explorer's / SDK's HUB CONFIG REGRESSION handling:
// alarm (a hub that lost config state is an operator event), drop the cache,
// reset the cursor and re-fetch the full tree once, exactly as the failover
// block above does.
async function refetchAfterConfigRegression(hub, result){
    if(hub.lastWatermark > 0 && hub.configs && hub.hubConfigRegressed(result)){
        logger.error('HubClient: HUB CONFIG REGRESSION: hub served seq ' + (Number(result.seq) || 0) +
                     '/watermark ' + (Number(result.watermark) || 0) + ', below last-seen ' + hub.lastSeq +
                     '/' + hub.lastWatermark +
                     ' (hub restart or restore from an older snapshot); discarding cached config and re-fetching the full tree.');
        hub.lastWatermark = 0;
        hub.configs       = null;
        result = await hub.call({
            jsonrpc: '2.0',
            method:  'getallconfigs',
            params:  hub.configParams(0),
            id:      1
        }, 10000);
    }
    return result;
}

module.exports = {

    // Get all configs from the hub
    // Returns nested object: { coin: { network: { module: { param: value } } } }
    //
    // Newer hubs wrap the config map as { configs, seq } so consumers can detect a
    // config change committed between polls; older hubs return the bare nested map.
    // We record the committed sequence on this.lastSeq and always return the bare
    // map, so extractDbConfigs sees the same shape regardless of hub version. seq
    // is 0 against an old hub. (Sync discovers DBs at startup, so the seq is tracked
    // for completeness rather than used for invalidation here.)
    async getallconfigs(){
        let cursorEndpoint = this._watermarkEndpointIdx;
        let sentCursor     = this.lastWatermark;
        // The hub reads its watermark BEFORE it reads the config rows, so a row
        // committed after that read but stamped in the SAME epoch-second as the
        // returned watermark is only delivered because the hub's cursor is now
        // inclusive (`since_updated_at >= cursor`, item #2265); an older hub compared
        // `>` and stranded that row forever (the cursor had already advanced past it).
        // Keep sending the cursor one second behind the stored watermark so the
        // boundary second is re-fetched against either hub generation (do not
        // "simplify" the - 1 away); mergeConfigDelta's upsert-only merge makes
        // re-receiving it a harmless no-op. 0 still means "send me the full tree"
        // (initial fetch, post-restart, or a hub too old to report a watermark).
        let deltaCursor = this.lastWatermark > 0 ? this.lastWatermark - 1 : 0;
        let result = await this.call({
            jsonrpc: '2.0',
            method:  'getallconfigs',
            params:  this.configParams(deltaCursor),
            id:      1
        }, 10000);
        // call returns null when every endpoint failed; preserve that signal so
        // extractDbConfigs (which treats null as "no configs") stays unchanged.
        if(result === null) return null;

        result = await refetchAfterEndpointChange(this, result, sentCursor, cursorEndpoint);
        if(result === null) return null;
        result = await refetchAfterConfigRegression(this, result);
        if(result === null) return null;

        this.warnIfRedacted(result);
        this.configs = this.applyConfigResult(result);
        // Bind the (possibly advanced) cursor to the endpoint that answered.
        this._watermarkEndpointIdx = this._lastGoodIdx;
        // A non-null result means at least one endpoint answered; record the fetch time
        // even on a delta poll that changed nothing, so the age reflects last hub contact.
        this.lastSuccessfulFetchAt = Date.now();
        return this.configs;
    },

    // True when a watermarked envelope from the cursor's own endpoint reports a
    // seq or watermark BELOW the last one it served us (hub restart / restore
    // from an older snapshot). A missing watermark is the full tree (handled by
    // applyConfigResult) and a zero watermark means an empty configs table, so
    // neither counts; the next poll re-fetches in full either way.
    hubConfigRegressed(result){
        let wrapped = result && typeof result === 'object' && result.configs && typeof result.configs === 'object' && ('seq' in result);
        if(!wrapped || result.watermark === undefined || result.watermark === null) return false;
        let watermark = Number(result.watermark) || 0;
        let seq       = Number(result.seq) || 0;
        return (watermark > 0 && watermark < this.lastWatermark) ||
               ((this.lastSeq || 0) > 0 && seq < this.lastSeq);
    },

    // Fold a getallconfigs result into this.configs and return the full nested
    // map. Newer hubs wrap the payload as { configs, seq, watermark }: when a
    // watermark is present the payload is a delta (only rows changed since the
    // cursor we sent), so we MERGE it into the cache and advance the cursor.
    // Older hubs return the bare map (or a { configs, seq } wrapper without a
    // watermark); those are the full tree, so we REPLACE. extractDbConfigs sees
    // the same full-map shape regardless of hub version. seq is 0 against an old
    // hub. The configs table is upsert-only (no row deletes), so merging
    // successive deltas reconstructs exactly what a full fetch would have returned.
    applyConfigResult(result){
        // Every envelope, initial fetch and delta poll alike, funnels through here.
        this.checkHubConsensusHash(result && typeof result === 'object' ? result.coin_consensus_hashes : null);

        let payload, seq, watermark;
        if(result && typeof result === 'object' && result.configs && typeof result.configs === 'object' && ('seq' in result)){
            payload   = result.configs;
            seq       = Number(result.seq) || 0;
            watermark = ('watermark' in result) ? result.watermark : undefined;
        } else {
            payload   = result;
            seq       = 0;
            watermark = undefined;
        }
        this.lastSeq = seq;

        if(watermark === undefined || watermark === null){
            // Hub doesn't report a watermark; payload is the full tree. Reset the
            // cursor so the next poll also requests in full.
            this.lastWatermark = 0;
            return payload;
        }

        let sentCursor = this.lastWatermark > 0;
        this.lastWatermark = Number(watermark) || 0;

        if(sentCursor && this.configs){
            // Delta against the cursor we sent: merge changed rows into the cache.
            return mergeConfigDelta(this.configs, payload || {});
        }
        // First fetch (or post-restart): payload is the full tree.
        return payload;
    },

    // Transport-integrity check: compare the consensus-config hashes the hub serves
    // on getallconfigs against our OWN bundled ones. Hub-served consensus values are
    // never applied (sync derives them from the vendored src/coins bundle), so this
    // only logs; what it buys is that a hub built from a divergent bundle surfaces at
    // the first poll instead of later as an opaque local-recompute divergence.
    // Mirrors XChainIndexer.checkHubConsensusHash, widened to every coin and network
    // because sync serves whatever chain set the hub hands it.
    checkHubConsensusHash(hubHashes){
        if(!hubHashes || typeof hubHashes !== 'object') return;   // older hub: field absent
        let mismatches = [];
        for(const network of coins.NETWORKS){
            let served = hubHashes[network];
            if(!served || typeof served !== 'object') continue;
            let local = localConsensusHashes(network);
            for(const tick of Object.keys(local)){
                // A coin the hub does not serve is version skew, not drift; only a
                // hash the hub DOES serve and that differs counts as a mismatch.
                if(served[tick] && served[tick] !== local[tick])
                    mismatches.push(tick + '/' + network + ': hub ' + served[tick] + ' vs bundled ' + local[tick]);
            }
        }
        // This runs on every poll, so log only when the mismatch SET changes: a
        // standing divergence must not flood the log, and a drift that widens or
        // clears must still report.
        let key = mismatches.join('|');
        if(key === (this._lastConsensusMismatchKey || '')) return;
        this._lastConsensusMismatchKey = key;
        if(mismatches.length)
            logger.error('CONSENSUS HASH MISMATCH: the hub serves consensus config differing from this service\'s bundled coin files (' +
                mismatches.join('; ') + '). Hub consensus values are never applied (they are pinned locally); upgrade the lagging side.');
    }
};
