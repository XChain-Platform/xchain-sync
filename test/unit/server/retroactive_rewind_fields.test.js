// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Rules-derived rewind fields stay validated, deterministic, and non-mutating.

'use strict';

const assert = require('assert');
const {
    retroactiveFromRules,
    retroactiveForkPoint,
    withRetroactive
} = require('../../../src/server/poller/retroactive_rewind');

const DIGEST = 'ABCDEF0123456789'.repeat(4);

function rules(state, target){
    return { digest: DIGEST, replay: { state, target, replay_to: 99 } };
}

describe('ServerPoller retroactive rewind fields @regression', function(){
    describe('retroactiveFromRules', function(){
        it('derives target and lowercase rules_to for pending or replaying rules', function(){
            assert.deepStrictEqual(retroactiveFromRules(rules('pending', 0)), {
                target: 0,
                rules_to: DIGEST.toLowerCase()
            });
            assert.deepStrictEqual(retroactiveFromRules(rules('replaying', 70)), {
                target: 70,
                rules_to: DIGEST.toLowerCase()
            });
        });

        it('returns null for replayed or malformed rules', function(){
            assert.strictEqual(retroactiveFromRules(rules('replayed', 70)), null);
            assert.strictEqual(retroactiveFromRules(rules('other', 70)), null);
            assert.strictEqual(retroactiveFromRules(rules('replaying', -1)), null);
            assert.strictEqual(retroactiveFromRules(rules('replaying', 1.5)), null);
            assert.strictEqual(retroactiveFromRules(rules('replaying', Number.MAX_SAFE_INTEGER + 1)), null);
            assert.strictEqual(retroactiveFromRules({ digest: 'not-a-digest', replay: {
                state: 'replaying', target: 70
            } }), null);
            assert.strictEqual(retroactiveFromRules({ digest: DIGEST, replay: null }), null);
            assert.strictEqual(retroactiveFromRules(null), null);
        });
    });

    it('retroactiveForkPoint prefers the rules target over the walk-back point', function(){
        const retro = retroactiveFromRules(rules('replaying', 70));

        assert.strictEqual(retroactiveForkPoint(retro, 95), 70);
        assert.strictEqual(retroactiveForkPoint(null, 95), 95);
    });

    it('withRetroactive returns the original event for null retroactive data', function(){
        const event = { type: 'reorg', forkPoint: 95 };

        assert.strictEqual(withRetroactive(event, null), event);
    });

    it('withRetroactive adds a copied retroactive field without mutating the event', function(){
        const event = { type: 'reorg', forkPoint: 95 };
        const retro = retroactiveFromRules(rules('replaying', 70));
        const result = withRetroactive(event, retro);

        assert.notStrictEqual(result, event);
        assert.deepStrictEqual(result, {
            type: 'reorg',
            forkPoint: 95,
            retroactive: { target: 70, rules_to: DIGEST.toLowerCase() }
        });
        assert.notStrictEqual(result.retroactive, retro);
        assert.deepStrictEqual(event, { type: 'reorg', forkPoint: 95 });
    });
});
