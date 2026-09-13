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
 * The sync-side analogue of the SDK registry (xchain-sdk/src/pinnedCheckpoints.js),
 * duplicated here because xchain-sync has no xchain-sdk dependency by design.
 * Keyed by "chain:network".
 *
 * INERT until populated: every real (chain, network) ships null, so with
 * VERIFY_CHECKPOINT_QUORUM off (the default) AND no pinned set, behavior is
 * unchanged. An operator may also supply a set out of band via the env var
 *   CHECKPOINT_VALIDATORS_<CHAIN>_<NETWORK>   (JSON array, e.g.
 *   CHECKPOINT_VALIDATORS_BTC_MAINNET='[{"pubkey":"..","weight":"..","source":".."}]')
 * which overrides the baked-in entry for that key.
 *
 * ABSENT is not INVALID. An unset override is inert: no trust root exists, so
 * ClientSync._verifyCheckpointQuorum skips the step, which is what the config.js
 * VERIFY_CHECKPOINT_QUORUM contract means by "skipped, never bypassed". An override
 * the operator DID supply but got wrong used to resolve to the same null, and with
 * every baked-in key still null that silently switched checkpoint authentication OFF
 * on a replica whose operator armed the flag believing it on. So the getters still
 * return null (they are read on hot paths and must never throw), and
 * `assertPinnedEnvOverrides` runs once at client startup to REFUSE to start on a
 * present-but-invalid value, the same shape as config.js assertBootstrapDepthChains.
 *
 * Validator ROTATION past the launch epoch: the launch set eventually stops
 * signing, so `getPinnedCheckpoint` below provides the out-of-band SEED checkpoint
 * (a committed state_root plus its block/snapshot height) from which a client rolls
 * its trust root FORWARD, proving each successor oracle_publish set against the
 * committed BTC stakes_root (spec §7.3). That registry is INERT too until launch
 * values land. Env override: CHECKPOINT_SEED_<CHAIN>_<NETWORK> (JSON), under the same
 * absent-versus-invalid rule as the validator override above.
 *
 * FILL AT LAUNCH: replace a key's null with the oracle_publish signer set that signs
 * the launch checkpoints, in the stake-weighted shape checkpoint.js verifyCheckpoint
 * expects:
 *   [ { pubkey: '<64-hex ed25519>', weight: '<canonical amount>', source: '<signer/delegation source>' }, ... ]
 * The SAME change must flip the VERIFY_CHECKPOINT_QUORUM default to true in
 * config.js, because a pinned set with the anchor still defaulting off leaves
 * replicas trusting their source when a trust root is available.
 * test/unit/checkpointQuorumFlagDay.test.js fails until both halves land.
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

// Parse + lightly validate an env-supplied set into { set, error }: `set` when the
// value is usable, `error` naming WHY it is not. The reason is what separates an
// absent override from an explicitly supplied invalid one, which the getters cannot
// express in their null and assertPinnedEnvOverrides refuses to start on.
function _parseValidatorSetEnv(raw) {
    let arr;
    try { arr = JSON.parse(raw); } catch (e) { return { set: null, error: 'is not valid JSON (' + e.message + ')' }; }
    if (!Array.isArray(arr)) return { set: null, error: 'is not a JSON array' };
    if (arr.length === 0) return { set: null, error: 'is an empty array (an empty set verifies nothing)' };
    for (let i = 0; i < arr.length; i++) {
        const v = arr[i];
        if (!v || typeof v !== 'object' || Array.isArray(v)) return { set: null, error: 'entry ' + i + ' is not an object' };
        for (const f of ['pubkey', 'weight', 'source']) {
            if (typeof v[f] !== 'string') return { set: null, error: 'entry ' + i + ' has no string `' + f + '`' };
        }
    }
    return { set: arr, error: null };
}

// Resolve the env-supplied set for (chain, network), or null when it is absent or
// unusable. Never throws: this is on the per-verify read path, and startup already
// refused an invalid explicit value.
function _fromEnv(chain, network) {
    const raw = process.env[_envKey(chain, network)];
    if (!raw) return null;
    const { set, error } = _parseValidatorSetEnv(raw);
    if (error) {
        console.warn('[pinnedValidators] ' + _envKey(chain, network) + ' override ' + error + '; no env trust root for this key');
        return null;
    }
    return set;
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

/**
 * Out-of-band SEED checkpoints for forward-following (spec §7.3), keyed
 * "chain:network". The trust anchor a client rolls forward from once the launch
 * `validators` set stops signing: its committed `state_root` lets the client prove
 * each successor oracle_publish set against the BTC stakes_root. All real keys ship
 * null (INERT) until launch values land. Shape mirrors the SDK's pinnedCheckpoints
 * entry (sans validator_signatures: the seed is trusted out of band, and the launch
 * set that signed it is `getPinnedValidators`).
 * @type {Record<string, { block_index:number, snapshot_block:number, checkpoint_seq:number, state_root:string, state_root_version:number, block_merkle_root:string, block_merkle_version:number } | null>}
 */
const PINNED_CHECKPOINTS = {
    'BTC:mainnet':  null,   // FILL AT LAUNCH (the BTC launch checkpoint; stakes are BTC-only, §4.1)
    'BTC:testnet':  null,
    'BTC:regtest':  null,
    'LTC:mainnet':  null,   // FILL AT LAUNCH
    'LTC:testnet':  null,
    'LTC:regtest':  null,
    'DOGE:mainnet': null,   // FILL AT LAUNCH
    'DOGE:testnet': null,
    'DOGE:regtest': null,
};

for (const k of Object.keys(PINNED_CHECKPOINTS)) {
    if (PINNED_CHECKPOINTS[k]) Object.freeze(PINNED_CHECKPOINTS[k]);
}
Object.freeze(PINNED_CHECKPOINTS);

function _seedEnvKey(chain, network) {
    return 'CHECKPOINT_SEED_' + String(chain).toUpperCase() + '_' + String(network).toUpperCase();
}

// Parse + lightly validate an env-supplied seed checkpoint into { seed, error }, the
// same absent-versus-invalid split as _parseValidatorSetEnv. state_root is the field
// the forward walk anchors successor-set proofs to, so it is required and a string.
function _parseSeedEnv(raw) {
    let cp;
    try { cp = JSON.parse(raw); } catch (e) { return { seed: null, error: 'is not valid JSON (' + e.message + ')' }; }
    if (!cp || typeof cp !== 'object' || Array.isArray(cp)) return { seed: null, error: 'is not a JSON object' };
    if (typeof cp.state_root !== 'string' || !cp.state_root) return { seed: null, error: 'has no non-empty string `state_root`' };
    for (const f of ['block_index', 'snapshot_block']) {
        if (typeof cp[f] !== 'number' || !Number.isFinite(cp[f]) || cp[f] < 0)
            return { seed: null, error: 'has no finite non-negative number `' + f + '`' };
    }
    return { seed: cp, error: null };
}

// Resolve the env-supplied seed for (chain, network), or null when it is absent or
// unusable. Never throws, for the same reason _fromEnv does not.
function _seedFromEnv(chain, network) {
    const raw = process.env[_seedEnvKey(chain, network)];
    if (!raw) return null;
    const { seed, error } = _parseSeedEnv(raw);
    if (error) {
        console.warn('[pinnedValidators] ' + _seedEnvKey(chain, network) + ' override ' + error + '; no env seed for this key');
        return null;
    }
    return seed;
}

/**
 * The pinned SEED checkpoint for (chain, network): an env override if present and
 * well-formed, else the baked-in entry, else null. Lookup is case-insensitive.
 * @param {string} chain
 * @param {string} network
 * @returns {object | null}
 */
function getPinnedCheckpoint(chain, network) {
    if (chain == null || network == null) return null;
    const env = _seedFromEnv(chain, network);
    if (env) return env;
    const entry = PINNED_CHECKPOINTS[String(chain).toUpperCase() + ':' + String(network).toLowerCase()];
    return entry || null;
}

// Env names the getters can actually read: the prefix plus a CHAIN_NETWORK suffix,
// both halves non-empty. Mirrors config.js bootstrapDepthEnvKey's shape rule.
const _OVERRIDE_PREFIXES = [
    { prefix: 'CHECKPOINT_VALIDATORS_', parse: _parseValidatorSetEnv, what: 'pinned validator set' },
    { prefix: 'CHECKPOINT_SEED_',       parse: _parseSeedEnv,         what: 'pinned seed checkpoint' },
];

function _isChainNetworkShaped(prefix, envKey) {
    if (envKey.indexOf(prefix) !== 0) return false;
    const rest = envKey.slice(prefix.length);
    const sep  = rest.lastIndexOf('_');
    return sep > 0 && sep < rest.length - 1;
}

/**
 * REFUSE to start when an explicitly supplied CHECKPOINT_VALIDATORS_* or
 * CHECKPOINT_SEED_* value is present but unusable.
 *
 * A malformed override is not inert. The getters answer null for it, exactly as they
 * do for an override nobody set, and every baked-in pin still ships null, so
 * ClientSync._verifyCheckpointQuorum's `if(!validators || !validators.length) return;`
 * skips checkpoint authentication entirely on a replica whose operator turned
 * VERIFY_CHECKPOINT_QUORUM on. An unset variable stays inert and is NOT an error; only
 * a value the operator supplied and got wrong is. Same rationale, and the same
 * "Refusing to start" shape, as config.js assertBootstrapDepthChains.
 *
 * @param {Record<string, string>} [env] Environment to scan; defaults to process.env.
 * @throws {Error} naming every offending variable and why it is unusable.
 */
function assertPinnedEnvOverrides(env) {
    const source = env || process.env;
    const bad = [];
    for (const envKey of Object.keys(source)) {
        for (const { prefix, parse, what } of _OVERRIDE_PREFIXES) {
            if (!_isChainNetworkShaped(prefix, envKey)) continue;
            const raw = source[envKey];
            if (raw === undefined || raw === null || raw === '') break;   // absent: inert, not an error
            const { error } = parse(raw);
            if (error) bad.push(envKey + ' (' + what + ') ' + error);
            break;
        }
    }
    if (bad.length === 0) return;
    throw new Error(
        'Invalid checkpoint pin override: ' + bad.join('; ') +
        '. Refusing to start: an unusable override resolves to the same null as an ABSENT one, ' +
        'which silently skips checkpoint-quorum verification instead of anchoring it. ' +
        'Fix the value or unset the variable to run deliberately unanchored.'
    );
}

module.exports = { getPinnedValidators, getPinnedCheckpoint, assertPinnedEnvOverrides, PINNED, PINNED_CHECKPOINTS };
