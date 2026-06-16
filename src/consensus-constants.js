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
 * A thin replica must agree with the source indexer on any value that
 * gates block-hashed state. These mirror the per-chain node-local
 * frozen defaults in xchain-indexer/src/configs/{BTC,LTC,DOGE}.js and
 * the golden in xchain-indexer/test/unit/consensus-params.test.js. They
 * are NOT hub-polled (the indexer's _mergeHubParams overlay is empty for
 * exactly this reason — live-polling races the federation into a soft
 * fork) and change only via a coordinated node upgrade. Any drift
 * between this map and the indexer config is a consensus divergence.
 *
 ********************************************************************/

// STAKING.ACTIVATION_DELAY_BLOCKS, set per-coin (network-independent — the
// indexer sets it before the per-network address switch) in
// xchain-indexer/src/configs/{COIN}.js. Used by ClientRollback to mirror the
// source indexer's reorg deactivation_block re-NULL resets, which key on
// orphanBlock + activationDelay.
const ACTIVATION_DELAY_BLOCKS_BY_COIN = {
    BTC:  6,    // ~60 min reorg protection at ~10 min/block
    LTC:  24,
    DOGE: 60
};

// The sync layer identifies a chain by `cfg.coin`, which may arrive as a ticker
// ('BTC') or a full name ('bitcoin'), in any case. Normalize to the ticker key.
const COIN_ALIASES = {
    btc: 'BTC',  bitcoin:  'BTC',
    ltc: 'LTC',  litecoin: 'LTC',
    doge: 'DOGE', dogecoin: 'DOGE'
};

// Resolve a coin identifier to its frozen ACTIVATION_DELAY_BLOCKS.
//   - null/undefined coin            → null  (legacy/no-op path; caller skips the mirror)
//   - recognized coin (ticker/name)  → the frozen integer
//   - anything else                  → undefined (caller treats as a hard misconfiguration)
function activationDelayBlocks(coin){
    if(coin === undefined || coin === null) return null;
    const ticker = COIN_ALIASES[String(coin).toLowerCase()];
    return ticker ? ACTIVATION_DELAY_BLOCKS_BY_COIN[ticker] : undefined;
}

// GAS token TICK — the genesis gas-asset symbol (xchain-indexer/src/config.js: `gas = 'XCHAIN'`,
// hardcoded). Network-independent and identical across all chains, like GAS_PRICE / GAS_SCHEDULE.
// Capability UNSTAKE cooldown refunds are paid in GAS, so ClientRollback's cooldown-maturity
// reversal needs it to byte-match the source indexer's GAS-tick refund-credit delete. Never
// hub-polled — a frozen consensus constant, changeable only via a coordinated node upgrade.
const GAS_TICK = 'XCHAIN';

// Resolve the GAS TICK. Coin-independent today (one frozen symbol across chains); kept as a
// function to mirror activationDelayBlocks() and to leave room for a future per-chain split.
function gasTickSymbol(){ return GAS_TICK; }

module.exports = { ACTIVATION_DELAY_BLOCKS_BY_COIN, activationDelayBlocks, GAS_TICK, gasTickSymbol };
