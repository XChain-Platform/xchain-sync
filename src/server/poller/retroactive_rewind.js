// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Pure helpers for deriving and attaching a rules-driven retroactive rewind.

'use strict';

function rulesFromMarker(marker){
    if(marker === null) return null;
    return {
        digest: marker.rules_to,
        replay: {
            state: marker.state,
            target: marker.target,
            replay_to: marker.replay_to
        }
    };
}

function retroactiveFromRules(rules){
    if(!rules || typeof rules.digest !== 'string' || !/^[0-9a-f]{64}$/i.test(rules.digest))
        return null;

    const replay = rules.replay;
    if(!replay || (replay.state !== 'pending' && replay.state !== 'replaying') ||
        !Number.isSafeInteger(replay.target) || replay.target < 0)
        return null;

    return { target: replay.target, rules_to: rules.digest.toLowerCase() };
}

function retroactiveForkPoint(retro, walkBackForkPoint){
    return retro === null ? walkBackForkPoint : retro.target;
}

function withRetroactive(event, retro){
    if(retro === null) return event;
    return {
        ...event,
        retroactive: { target: retro.target, rules_to: retro.rules_to }
    };
}

module.exports = { rulesFromMarker, retroactiveFromRules, retroactiveForkPoint, withRetroactive };
