/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
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
 * XChain Sync activation registry entry. Shared rows are loaded from the
 * byte-twin part files, then the sync-only consensus constants are added.
 * Regtest arming is applied at read time from process.env.
 *
 ********************************************************************/

'use strict';

const coins = require('../coins');
const core = require('./gate_registry/core.js');
const { registerRows } = require('./gate_registry/shared_rows.js');

// The SHARED block, loaded for effect: each part queues its rows into
// shared_rows.js as it loads; registerRows() below replays them, in part
// order, into the one registry and installs the venue's regtest arming as
// its read overlay.
require('./gate_registry/shared_rows_1.js');
require('./gate_registry/shared_rows_2.js');
require('./gate_registry/shared_rows_3.js');
require('./gate_registry/shared_rows_4.js');
require('./gate_registry/shared_rows_5.js');

const { registry } = core;
registerRows(registry, process.env);

// SYNC-ONLY GATES
function activationDelays() {
    const table = {};
    for (const tick of coins.ALLOWED_COINS) {
        table[tick] = coins.getCoinConfig(tick, 'mainnet').STAKING.ACTIVATION_DELAY_BLOCKS;
    }
    return table;
}

function stakeCapabilities() {
    const table = {};
    const capabilities = coins.getCoinConfig('BTC', 'mainnet').STAKING.CAPABILITIES;
    for (const name of Object.keys(capabilities)) table[name] = capabilities[name].MIN_STAKE;
    return table;
}

registry.addGate('consensus-constants.ACTIVATION_DELAY_BLOCKS_BY_COIN', 'constant', activationDelays());
registry.addGate('consensus-constants.BTC_STAKE_CAPABILITIES', 'constant', stakeCapabilities());
registry.addGate('consensus-constants.GAS_TICK', 'constant', 'XCHAIN');
registry.addGate('consensus-constants.VALIDATOR_QUERY_LIMIT', 'constant', coins.getCoinConfig('BTC', 'mainnet').VALIDATOR_QUERY_LIMIT);

module.exports = {
    get: (key) => registry.get(key),
    copy: (key) => registry.copy(key),
    has: (key) => registry.has(key),
    keys: () => registry.keys(),
    rows: () => registry.rows(),
    activeAt: (key, network, coin, height, time) => registry.activeAt(key, network, coin, height, time),
    UNARMED: core.UNARMED,
    UNPINNED: core.UNPINNED,
    RegistryMissError: core.RegistryMissError,
};
