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
 * XChain Indexer Sync - Frozen consensus constants
 *
 * A thin replica must agree with the source indexer on any value that gates
 * block-hashed state. These are now DERIVED from the canonical coin registry
 * (src/coins, the platform-wide source of truth) instead of being hand-mirrored,
 * so they cannot drift from the indexer. They are NOT hub-polled (live-polling
 * races the federation into a soft fork) and change only via a coordinated node
 * upgrade that ships a new canonical file.
 *
 ********************************************************************/

const coins = require('./coins');

// STAKING.ACTIVATION_DELAY_BLOCKS per coin (network-independent). Used by
// ClientRollback to mirror the source indexer's reorg deactivation_block resets.
const ACTIVATION_DELAY_BLOCKS_BY_COIN = {};
for(const tick of coins.ALLOWED_COINS)
    ACTIVATION_DELAY_BLOCKS_BY_COIN[tick] = coins.getCoinConfig(tick, 'mainnet').STAKING.ACTIVATION_DELAY_BLOCKS;

// The sync layer identifies a chain by `cfg.coin`, which may arrive as a ticker
// ('BTC') or a full name ('bitcoin'), in any case. Normalize to the ticker key.
const COIN_ALIASES = {};
for(const tick of coins.ALLOWED_COINS){
    COIN_ALIASES[tick.toLowerCase()]            = tick;
    COIN_ALIASES[coins.COIN_FULL_NAME[tick]]    = tick;
}

// Resolve a coin identifier to its frozen ACTIVATION_DELAY_BLOCKS.
//   - null/undefined coin            -> null  (legacy/no-op path; caller skips the mirror)
//   - recognized coin (ticker/name)  -> the frozen integer
//   - anything else                  -> undefined (caller treats as a hard misconfiguration)
function activationDelayBlocks(coin){
    if(coin === undefined || coin === null) return null;
    const ticker = COIN_ALIASES[String(coin).toLowerCase()];
    return ticker ? ACTIVATION_DELAY_BLOCKS_BY_COIN[ticker] : undefined;
}

// Normalize a chain identifier to its canonical TICKER for consensus / activation
// lookups. The sync layer identifies a chain by `cfg.coin`, which arrives as the FULL
// LOWERCASE NAME ('litecoin'), while every per-chain activation map is keyed
// '<TICKER>:<network>' ('LTC:mainnet') exactly as the SOURCE indexer writes it (its
// db.js passes config['COIN'], a ticker). Passing the full name silently missed the
// key, with two consequences: the state-hash classes resolved differently from the
// source, so the recompute diverged on every block at/after each chain's activation
// height and halted every replica; and isStateCommitmentActive fell through to "off",
// so the follower never computed the SMT roots and the light-client state-commitment
// check never ran at all (failing OPEN rather than halting). Callers MUST pass this
// ticker form anywhere the source passes config['COIN']. Unrecognized input is
// returned UNCHANGED so no caller regresses relative to the previous behaviour.
function coinTicker(coin){
    if(coin === undefined || coin === null) return coin;
    return COIN_ALIASES[String(coin).toLowerCase()] || coin;
}

// GAS token TICK: the genesis gas-asset symbol (xchain-indexer/src/config.js:
// `gas = 'XCHAIN'`). Network- and coin-independent (one symbol across all chains),
// so it is a platform constant, not a per-coin field. Never hub-polled.
const GAS_TICK = 'XCHAIN';
function gasTickSymbol(){ return GAS_TICK; }

// VALIDATOR_QUERY_LIMIT: CONSENSUS-CRITICAL cap on validator-set queries. Same
// value across coins; sourced from canonical BTC. The light-client stakes_root
// build must select the identical capped set as the indexer or the root forks.
const VALIDATOR_QUERY_LIMIT = coins.getCoinConfig('BTC', 'mainnet').VALIDATOR_QUERY_LIMIT;

// BTC STAKING.CAPABILITIES MIN_STAKE floors, sourced from canonical BTC. The
// BTC-only stakes_root commits one leaf per (pubkey, capability) whose aggregate
// active stake >= MIN_STAKE; the follower must use the SAME floors as the indexer
// or the stakes_root diverges. Derived so it can never drift from the coin file.
const BTC_STAKE_CAPABILITIES = {};
{
    const caps = coins.getCoinConfig('BTC', 'mainnet').STAKING.CAPABILITIES;
    for(const cap of Object.keys(caps))
        BTC_STAKE_CAPABILITIES[cap] = caps[cap].MIN_STAKE;
}
function btcStakeCapabilities(){ return BTC_STAKE_CAPABILITIES; }

module.exports = {
    ACTIVATION_DELAY_BLOCKS_BY_COIN, activationDelayBlocks, GAS_TICK, gasTickSymbol,
    coinTicker, VALIDATOR_QUERY_LIMIT, BTC_STAKE_CAPABILITIES, btcStakeCapabilities
};
