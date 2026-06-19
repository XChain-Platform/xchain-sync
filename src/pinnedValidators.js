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
 * xchain-sync: pinned launch validator sets (SPV checkpoint-quorum anchor)
 *
 * The OUT-OF-BAND trust root a CLIENT replica uses to verify a server-supplied,
 * quorum-signed checkpoint. The sync server transports the checkpoint (its
 * Ed25519 signatures are self-authenticating), but the client verifies those
 * signatures against THIS set, never a set the server hands it. The replica then
 * asserts the verified checkpoint's committed state_root equals its OWN
 * independently-recomputed state_tree_roots row (VERIFY_STATE_COMMITMENT). A
 * single lying source cannot forge a quorum of federation signatures, so it
 * cannot make a fabricated state_root pass.
 *
 * The sync-side analogue of the SDK D4 registry
 * (xchain-sdk/src/pinnedCheckpoints.js); kept here because xchain-sync has no
 * xchain-sdk dependency by design. Keyed by "chain:network".
 *
 * INERT until populated: every real (chain, network) ships null, so with
 * VERIFY_CHECKPOINT_QUORUM off (the default) AND no pinned set, behavior is
 * unchanged. An operator may also supply a set out of band via the env var
 *   CHECKPOINT_VALIDATORS_<CHAIN>_<NETWORK>   (JSON array, e.g.
 *   CHECKPOINT_VALIDATORS_BTC_MAINNET='[{"pubkey":"..","weight":"..","source":".."}]')
 * which overrides the baked-in entry for that key.
 *
 * Validator ROTATION past the launch epoch is out of scope here (shared with the
 * wallet's deferred followForward work); the launch set covers the launch epoch.
 *
 * ---- FILL AT LAUNCH --------------------------------------------------------
 * Replace a key's null with the oracle_publish signer set that signs the launch
 * checkpoints, stake-weighted shape per checkpoint.js verifyCheckpoint:
 *   [ { pubkey: '<64-hex ed25519>', weight: '<canonical amount>', source: '<signer/delegation source>' }, ... ]
 * ---------------------------------------------------------------------------
 *
 ********************************************************************/

'use strict';

/**
 * Baked-in pinned validator sets, keyed "chain:network". All real keys ship null
 * (inert) until launch values land.
 * @type {Record<string, Array<{pubkey:string, weight:string, source:string}> | null>}
 */
const PINNED = {
    'BTC:mainnet':  null,   // FILL AT LAUNCH
    'BTC:testnet':  null,
    'BTC:regtest':  null,
    'LTC:mainnet':  null,   // FILL AT LAUNCH
    'LTC:testnet':  null,
    'LTC:regtest':  null,
    'DOGE:mainnet': null,   // FILL AT LAUNCH
    'DOGE:testnet': null,
    'DOGE:regtest': null,
};

for (const k of Object.keys(PINNED)) {
    if (Array.isArray(PINNED[k])) {
        PINNED[k].forEach((v) => Object.freeze(v));
        Object.freeze(PINNED[k]);
    }
}
Object.freeze(PINNED);

function _envKey(chain, network) {
    return 'CHECKPOINT_VALIDATORS_' + String(chain).toUpperCase() + '_' + String(network).toUpperCase();
}

// Parse + lightly validate an env-supplied set; returns null on any malformation
// so a bad override never weakens verification (fails closed: no set to verify
// against means the quorum step is skipped, not bypassed).
function _fromEnv(chain, network) {
    const raw = process.env[_envKey(chain, network)];
    if (!raw) return null;
    let arr;
    try { arr = JSON.parse(raw); } catch (e) { return null; }
    if (!Array.isArray(arr) || arr.length === 0) return null;
    for (const v of arr) {
        if (!v || typeof v.pubkey !== 'string' || typeof v.weight !== 'string' || typeof v.source !== 'string') return null;
    }
    return arr;
}

/**
 * The pinned validator set for (chain, network): an env override if present and
 * well-formed, else the baked-in entry, else null. Lookup is case-insensitive.
 * @param {string} chain
 * @param {string} network
 * @returns {Array<{pubkey:string, weight:string, source:string}> | null}
 */
function getPinnedValidators(chain, network) {
    if (chain == null || network == null) return null;
    const env = _fromEnv(chain, network);
    if (env) return env;
    const entry = PINNED[String(chain).toUpperCase() + ':' + String(network).toLowerCase()];
    return entry || null;
}

module.exports = { getPinnedValidators, PINNED };
