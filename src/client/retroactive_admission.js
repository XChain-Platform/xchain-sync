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

// Pure limits and quorum checks for admitting a retroactive rewind.
const RETROACTIVE_BLOCK_SECONDS = Object.freeze({
    BTC: 600,
    LTC: 150,
    DOGE: 60,
});

function retroactiveCapBlocks(coin, hours = 72){
    const seconds = RETROACTIVE_BLOCK_SECONDS[coin];
    if(seconds === undefined) throw new Error(`Unknown coin: ${coin}`);
    if(!Number.isFinite(hours) || hours <= 0) throw new Error('hours must be a finite positive number');
    return Math.floor(hours * 3600 / seconds);
}

function admitRetroactiveRewind({depth, capBlocks, retroactive, reports, quorum}){
    if(retroactive === null) return {admit: false, reason: 'not_retroactive'};
    if(depth > capBlocks) return {admit: false, reason: 'over_cap'};

    const sources = new Set(
        reports
            .filter((report) => report.target === retroactive.target && report.rules_to === retroactive.rules_to)
            .map((report) => report.source)
    );
    if(sources.size < quorum) return {admit: false, reason: 'no_quorum'};
    return {admit: true, reason: 'quorum'};
}

module.exports = {
    RETROACTIVE_BLOCK_SECONDS,
    retroactiveCapBlocks,
    admitRetroactiveRewind,
};
