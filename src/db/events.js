/*********************************************************************
 *
 * Copyright (c) 2025-2026 Dankest, LLC
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
 * Strict reads of source event records consumed by synchronization.
 *
 ********************************************************************/

'use strict';

const DIGEST_RE = /^[0-9a-f]{64}$/;
const REPLAY_STATES = new Set(['pending', 'replaying', 'replayed']);

function canonicalReplayMarker(marker){
    if(!marker || typeof marker !== 'object' || Array.isArray(marker))
        throw new TypeError('replay marker must be an object');
    if(!Number.isSafeInteger(marker.target) || marker.target < 0)
        throw new TypeError('target must be a non-negative safe integer');
    if(!Number.isInteger(marker.replay_to) || marker.replay_to < marker.target)
        throw new TypeError('replay_to must be an integer at or above target');
    if(typeof marker.rules_from !== 'string' || !DIGEST_RE.test(marker.rules_from))
        throw new TypeError('rules_from must be a 64-character lowercase hex digest');
    if(typeof marker.rules_to !== 'string' || !DIGEST_RE.test(marker.rules_to))
        throw new TypeError('rules_to must be a 64-character lowercase hex digest');
    if(!REPLAY_STATES.has(marker.state))
        throw new TypeError('state is not a replay state');

    return {
        target: marker.target,
        replay_to: marker.replay_to,
        rules_from: marker.rules_from,
        rules_to: marker.rules_to,
        state: marker.state,
    };
}

module.exports = {

    async getSourceReplayMarker(conn = null){
        const rows = await this.doQuery(
            "SELECT data FROM events WHERE code = 'CONSENSUS_RETRO_REPLAY' ORDER BY id DESC LIMIT 1",
            null,
            conn,
            { rethrow: true }
        );
        if(!rows || !rows.length) return null;
        return canonicalReplayMarker(JSON.parse(rows[0].data));
    },

};
