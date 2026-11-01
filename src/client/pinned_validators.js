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
 * The sync-side analogue of the SDK registry (xchain-sdk/src/protocol/pinned_checkpoints.js),
 * duplicated here because xchain-sync has no xchain-sdk dependency by design.
 * Keyed by "chain:network".
 *
 * Mainnet and regtest stay inert until populated. Testnet ships the live
 * validator set so its default-on checkpoint-quorum anchor can verify. An
 * operator may also supply a set out of band via the env var
 *   CHECKPOINT_VALIDATORS_<CHAIN>_<NETWORK>   (JSON array, e.g.
 *   CHECKPOINT_VALIDATORS_BTC_MAINNET='[{"pubkey":"..","weight":"..","source":".."}]')
 * which overrides the baked-in entry for that key.
 *
 * The service names a chain by the hub's full name ('bitcoin'), so the getters fold
 * it onto the ticker every map key uses. An env name may spell the chain either way
 * (CHECKPOINT_VALIDATORS_BTC_MAINNET or CHECKPOINT_VALIDATORS_BITCOIN_MAINNET); the
 * ticker form is canonical and wins when both are set.
 *
 * ABSENT is not INVALID. An unset override leaves the baked-in choice unchanged. A
 * bad explicit override must not be mistaken for absence: an unpinned network would
 * skip authentication, while a pinned network would silently ignore the operator's
 * intended replacement. The hot-path getters never throw, so
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
 * The SAME change must flip that key's VERIFY_CHECKPOINT_QUORUM default to true in
 * CHECKPOINT_QUORUM_DEFAULTS below, because a pinned set with the anchor still
 * defaulting off leaves replicas trusting their source when a trust root is available.
 * test/unit/checkpoint_quorum_flag_day.test.js fails until both halves land.
 *
 ********************************************************************/

'use strict';
const { getLogger } = require('../observability');
const envConfig = require('../config');
const { coinTicker } = require('../consensus-constants');
const swq = require('../consensus/stake_weighted_quorum');
const logger = getLogger();
const HEX64 = /^[0-9a-fA-F]{64}$/;

const TESTNET_VALIDATORS = [
    { pubkey: '18e4c1d54c8932d51fa54e4bc0164461a0605fb801bfca21cad57fa74495c174', weight: '50000.00000000', source: 'mverify2tE4RK7QYHRXbpmj6iLabcYtpWY' },
    { pubkey: '2661bd8f42b910bbb471387b7c28eaa440fc3c99077c160a69f22859f924aa7d', weight: '50000.00000000', source: 'mverify1Q6YPNqP4UzqLoa1MpDDZUu4ogC' },
    { pubkey: '72a25832d5f9d30baf704579fc440723f03cba55098cb2aac21063bf71029bad', weight: '50000.00000000', source: 'mverify4Hoe6uqa7ZJanX7GFXDpZ6dmtyX' },
    { pubkey: '8a1215431e8143b2ebdc9fed2fe7f655ecc327c4efd66798bc4e5504a5d5b0a8', weight: '50000.00000000', source: 'mverify3LaXvGitmYx3nHa3y2LMJ1odWVc' },
    { pubkey: 'c1de91459e8bd93de9e3263c26d886058593d3fdae57ad0c1bd48ca3dd22f32a', weight: '50000.00000000', source: 'mverify5szar1XwwAjqAnPUHbA814kfHh4' },
];

// Fold a ticker or full coin name onto the uppercase ticker the pin maps are keyed by.
function tickerOf(chain) {
    return String(coinTicker(String(chain))).toUpperCase();
}

// Env names to try for (chain, network): the ticker form first, then the caller's own
// spelling when it differs, so the full-name form operators already set keeps resolving.
function envNames(keyFn, chain, network) {
    const names = [keyFn(tickerOf(chain), network)];
    const raw = keyFn(chain, network);
    if (raw !== names[0]) names.push(raw);
    return names;
}

/**
 * Baked-in pinned validator sets, keyed "chain:network". Mainnet and regtest
 * ship null until launch values land; all testnet chains share the BTC-anchored
 * oracle_publish set.
 * @type {Record<string, Array<{pubkey:string, weight:string, source:string}> | null>}
 */
const PINNED = {
    'BTC:mainnet':  null,   // FILL AT LAUNCH
    'BTC:testnet':  TESTNET_VALIDATORS,
    'BTC:regtest':  null,
    'LTC:mainnet':  null,   // FILL AT LAUNCH
    'LTC:testnet':  TESTNET_VALIDATORS,
    'LTC:regtest':  null,
    'DOGE:mainnet': null,   // FILL AT LAUNCH
    'DOGE:testnet': TESTNET_VALIDATORS,
    'DOGE:regtest': null,
};

for (const k of Object.keys(PINNED)) {
    if (Array.isArray(PINNED[k])) {
        PINNED[k].forEach((v) => Object.freeze(v));
        Object.freeze(PINNED[k]);
    }
}
Object.freeze(PINNED);

/**
 * Shipped VERIFY_CHECKPOINT_QUORUM defaults, keyed like PINNED. The operator ruled
 * per-(chain, network) defaults (2026-10-12) so testnet rehearses the anchor before
 * mainnet relies on it: testnet anchors by default against the shipped live set.
 * Mainnet stays off until the launch key fill, which flips it in the same change.
 * Regtest stays off: a rail venue runs no signing federation to anchor against.
 * An explicit VERIFY_CHECKPOINT_QUORUM env value overrides every key.
 */
const CHECKPOINT_QUORUM_DEFAULTS = Object.freeze({
    'BTC:mainnet':  false,  // ON AT LAUNCH FILL
    'BTC:testnet':  true,
    'BTC:regtest':  false,
    'LTC:mainnet':  false,  // ON AT LAUNCH FILL
    'LTC:testnet':  true,
    'LTC:regtest':  false,
    'DOGE:mainnet': false,  // ON AT LAUNCH FILL
    'DOGE:testnet': true,
    'DOGE:regtest': false,
});

// Whether the checkpoint-quorum anchor runs for (chain, network): the explicit env
// value config.js read (true or false) when the operator set one, else the shipped
// default for that key. An unknown key is off.
function checkpointQuorumEnabled(explicit, chain, network) {
    if (typeof explicit === 'boolean') return explicit;
    if (chain == null || network == null) return false;
    return CHECKPOINT_QUORUM_DEFAULTS[tickerOf(chain) + ':' + String(network).toLowerCase()] === true;
}

function envKey(chain, network) {
    return 'CHECKPOINT_VALIDATORS_' + String(chain).toUpperCase() + '_' + String(network).toUpperCase();
}

// Why entry i of an env-supplied set can never verify, or null when it can. The stake
// fields are judged by the quorum module itself (swq.totalStake throws on a blank source
// and on a missing, nonnumeric or negative weight), so this cannot drift from it.
function entryError(v, i) {
    if (!v || typeof v !== 'object' || Array.isArray(v)) return 'entry ' + i + ' is not an object';
    for (const f of ['pubkey', 'weight', 'source']) {
        if (typeof v[f] !== 'string') return 'entry ' + i + ' has no string `' + f + '`';
    }
    if (!HEX64.test(v.pubkey)) return 'entry ' + i + ' `pubkey` is not a 64-hex Ed25519 public key';
    try { swq.totalStake([v]); } catch (e) { return 'entry ' + i + ' has an unusable stake field (' + e.message + ')'; }
    return null;
}

// Parse and validate an env-supplied set into { set, error }: `set` when the value is
// usable, `error` naming WHY it is not. It refuses what the checkpoint verifier always
// rejects (a bad pubkey, stake field or zero total stake), so a typo is named at startup
// rather than surfacing later as a quorum halt that blames the source. The reason is what
// separates an absent override from an explicitly supplied invalid one, which the getters
// cannot express in their null and assertPinnedEnvOverrides refuses to start on.
function parseValidatorSetEnv(raw) {
    let arr;
    try { arr = JSON.parse(raw); } catch (e) { return { set: null, error: 'is not valid JSON (' + e.message + ')' }; }
    if (!Array.isArray(arr)) return { set: null, error: 'is not a JSON array' };
    if (arr.length === 0) return { set: null, error: 'is an empty array (an empty set verifies nothing)' };
    for (let i = 0; i < arr.length; i++) {
        const error = entryError(arr[i], i);
        if (error) return { set: null, error };
    }
    let total;
    try { total = swq.totalStake(arr); } catch (e) { return { set: null, error: 'has an unusable stake set (' + e.message + ')' }; }
    if (total.lte(0)) return { set: null, error: 'has zero total stake (the stake-weighted quorum can never be met)' };
    return { set: arr, error: null };
}

// Resolve the env-supplied set for (chain, network), or null when it is absent or
// unusable. Never throws: this is on the per-verify read path, and startup already
// refused an invalid explicit value.
function fromEnv(chain, network) {
    for (const name of envNames(envKey, chain, network)) {
        const raw = envConfig.envValueByName(name);
        if (!raw) continue;
        const { set, error } = parseValidatorSetEnv(raw);
        if (error) {
            logger.warn('[pinnedValidators] ' + name + ' override ' + error + '; no env trust root for this key');
            return null;
        }
        return set;
    }
    return null;
}

/**
 * The pinned validator set for (chain, network): an env override if present and
 * well-formed, else the baked-in entry, else null. Lookup is case-insensitive and
 * accepts the ticker or the full coin name.
 * @param {string} chain
 * @param {string} network
 * @returns {Array<{pubkey:string, weight:string, source:string}> | null}
 */
function getPinnedValidators(chain, network) {
    if (chain == null || network == null) return null;
    const env = fromEnv(chain, network);
    if (env) return env;
    const entry = PINNED[tickerOf(chain) + ':' + String(network).toLowerCase()];
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

function seedEnvKey(chain, network) {
    return 'CHECKPOINT_SEED_' + String(chain).toUpperCase() + '_' + String(network).toUpperCase();
}

// Parse + lightly validate an env-supplied seed checkpoint into { seed, error }, the
// same absent-versus-invalid split as parseValidatorSetEnv. state_root is the field
// the forward walk anchors successor-set proofs to, so it is required and a string.
function parseSeedEnv(raw) {
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
// unusable. Never throws, for the same reason fromEnv does not.
function seedFromEnv(chain, network) {
    for (const name of envNames(seedEnvKey, chain, network)) {
        const raw = envConfig.envValueByName(name);
        if (!raw) continue;
        const { seed, error } = parseSeedEnv(raw);
        if (error) {
            logger.warn('[pinnedValidators] ' + name + ' override ' + error + '; no env seed for this key');
            return null;
        }
        return seed;
    }
    return null;
}

/**
 * The pinned SEED checkpoint for (chain, network): an env override if present and
 * well-formed, else the baked-in entry, else null. Lookup is case-insensitive and
 * accepts the ticker or the full coin name.
 * @param {string} chain
 * @param {string} network
 * @returns {object | null}
 */
function getPinnedCheckpoint(chain, network) {
    if (chain == null || network == null) return null;
    const env = seedFromEnv(chain, network);
    if (env) return env;
    const entry = PINNED_CHECKPOINTS[tickerOf(chain) + ':' + String(network).toLowerCase()];
    return entry || null;
}

// The override families the startup scan checks: each prefix, the name builder and pin
// map its getter reads through, and how its value parses.
const _OVERRIDE_PREFIXES = [
    { prefix: 'CHECKPOINT_VALIDATORS_', keyFn: envKey,     pins: PINNED,             parse: parseValidatorSetEnv, what: 'pinned validator set' },
    { prefix: 'CHECKPOINT_SEED_',       keyFn: seedEnvKey, pins: PINNED_CHECKPOINTS, parse: parseSeedEnv,         what: 'pinned seed checkpoint' },
];

// A prefixed name with a CHAIN_NETWORK suffix, both halves non-empty. Mirrors config.js
// bootstrapDepthEnvKey's shape rule, except that the prefix matches in any case, so a
// mis-cased name reaches isReadOverrideName's upper-case check and is refused, not skipped.
function isChainNetworkShaped(prefix, envKey) {
    if (envKey.length < prefix.length || envKey.slice(0, prefix.length).toUpperCase() !== prefix) return false;
    const rest = envKey.slice(prefix.length);
    const sep  = rest.lastIndexOf('_');
    return sep > 0 && sep < rest.length - 1;
}

// Does a getter ever read this shaped name? Only when it is the upper-case name keyFn builds
// for its own halves and the chain folds onto a ticker that, with the network, keys `pins`.
function isReadOverrideName(family, name) {
    const rest = name.slice(family.prefix.length);
    const sep  = rest.lastIndexOf('_');
    const chain = rest.slice(0, sep), network = rest.slice(sep + 1);
    if (family.keyFn(chain, network) !== name) return false;
    return Object.prototype.hasOwnProperty.call(family.pins, tickerOf(chain) + ':' + network.toLowerCase());
}

/**
 * REFUSE to start when an explicitly supplied CHECKPOINT_VALIDATORS_* or
 * CHECKPOINT_SEED_* value is present but unusable, or sits under a CHAIN_NETWORK name no
 * getter reads (an unknown chain or network, or a name not in upper case).
 *
 * A malformed or misnamed override is not inert. The hot-path getters cannot throw;
 * they fall back to the baked-in pin or null. That would either ignore an intended
 * replacement or skip checkpoint authentication entirely. An unset variable stays
 * inert and is NOT an error; only a value the operator supplied and got wrong is.
 * Same rationale, and the same "Refusing to start" shape, as config.js
 * assertBootstrapDepthChains.
 *
 * @param {Record<string, string>} [env] Environment to scan; defaults to process.env.
 * @throws {Error} naming every offending variable and why it is unusable.
 */
function assertPinnedEnvOverrides(env) {
    const source = env || envConfig.envSource();
    const bad = [];
    for (const envKey of Object.keys(source)) {
        for (const family of _OVERRIDE_PREFIXES) {
            const { prefix, parse, what } = family;
            if (!isChainNetworkShaped(prefix, envKey)) continue;
            const raw = source[envKey];
            if (raw === undefined || raw === null || raw === '') break;   // absent: inert, not an error
            if (!isReadOverrideName(family, envKey)) {
                bad.push(envKey + ' (' + what + ') names no chain and network the pin getters read');
                break;
            }
            const { error } = parse(raw);
            if (error) bad.push(envKey + ' (' + what + ') ' + error);
            break;
        }
    }
    if (bad.length === 0) return;
    throw new Error(
        'Invalid checkpoint pin override: ' + bad.join('; ') +
        '. Refusing to start: an unusable or unread override resolves to the same null as an ABSENT one, ' +
        'which silently skips checkpoint-quorum verification instead of anchoring it. ' +
        'Fix the name or value, or unset the variable to run deliberately unanchored.'
    );
}

module.exports = { getPinnedValidators, getPinnedCheckpoint, assertPinnedEnvOverrides, checkpointQuorumEnabled, PINNED, PINNED_CHECKPOINTS, CHECKPOINT_QUORUM_DEFAULTS };
