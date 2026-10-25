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
 * Source replay marker read validation and rewind rule composition.
 *
 ********************************************************************/

'use strict';

const assert = require('assert');
const { getSourceReplayMarker } = require('../../../src/db/events');
const {
    rulesFromMarker,
    retroactiveFromRules,
} = require('../../../src/server/poller/retroactive_rewind');

const RULES_FROM = 'ab'.repeat(32);
const RULES_TO = 'cd'.repeat(32);
const MARKER = {
    target: 70,
    replay_to: 99,
    rules_from: RULES_FROM,
    rules_to: RULES_TO,
    state: 'replaying',
};

function databaseWithRows(rows){
    const calls = [];
    return {
        calls,
        async doQuery(query, args, conn, options){
            calls.push({ query, args, conn, options });
            return rows;
        },
    };
}

async function rejectsMarker(marker){
    const database = databaseWithRows([{ data: marker }]);
    await assert.rejects(() => getSourceReplayMarker.call(database));
}

describe('Database source replay marker reader @regression', function(){
    it('queries the newest CONSENSUS_RETRO_REPLAY event once with a strict read', async function(){
        const database = databaseWithRows([]);

        assert.strictEqual(await getSourceReplayMarker.call(database), null);
        assert.strictEqual(database.calls.length, 1);
        assert.deepStrictEqual(database.calls[0], {
            query: "SELECT data FROM events WHERE code = 'CONSENSUS_RETRO_REPLAY' " +
                'ORDER BY id DESC LIMIT 1',
            args: null,
            conn: null,
            options: { rethrow: true },
        });
    });

    it('passes the connection through and returns only canonical marker fields', async function(){
        const conn = {};
        const database = databaseWithRows([{ data: JSON.stringify({ extra: true, ...MARKER }) }]);

        assert.deepStrictEqual(await getSourceReplayMarker.call(database, conn), MARKER);
        assert.strictEqual(database.calls[0].conn, conn);
    });

    it('rejects data that is not JSON', async function(){
        await rejectsMarker('not json');
    });
});

describe('Database source replay marker reader @regression', function(){
    it('rejects a target that is not a non-negative safe integer', async function(){
        await rejectsMarker(JSON.stringify({ ...MARKER, target: -1 }));
        await rejectsMarker(JSON.stringify({ ...MARKER, target: Number.MAX_SAFE_INTEGER + 1 }));
    });

    it('rejects replay_to below target or not an integer', async function(){
        await rejectsMarker(JSON.stringify({ ...MARKER, replay_to: 69 }));
        await rejectsMarker(JSON.stringify({ ...MARKER, replay_to: 70.5 }));
    });

    it('rejects rules_from that is not lowercase 64-character hex', async function(){
        await rejectsMarker(JSON.stringify({ ...MARKER, rules_from: 'x' }));
        await rejectsMarker(JSON.stringify({ ...MARKER, rules_from: 'AB'.repeat(32) }));
    });

    it('rejects rules_to that is not lowercase 64-character hex', async function(){
        await rejectsMarker(JSON.stringify({ ...MARKER, rules_to: 'x' }));
        await rejectsMarker(JSON.stringify({ ...MARKER, rules_to: 'CD'.repeat(32) }));
    });

    it('rejects a state outside pending, replaying, or replayed', async function(){
        await rejectsMarker(JSON.stringify({ ...MARKER, state: 'done' }));
    });

    it('propagates a rejected source read', async function(){
        const error = new Error('read failed');
        const database = { doQuery: async() => { throw error; } };

        await assert.rejects(() => getSourceReplayMarker.call(database), error);
    });
});

describe('Database source replay marker reader @regression', function(){
    describe('rulesFromMarker composition', function(){
        it('returns null for no marker', function(){
            assert.strictEqual(rulesFromMarker(null), null);
        });

        it('adapts an open marker for retroactiveFromRules', function(){
            const rules = rulesFromMarker(MARKER);

            assert.deepStrictEqual(rules, {
                digest: RULES_TO,
                replay: { state: 'replaying', target: 70, replay_to: 99 },
            });
            assert.deepStrictEqual(retroactiveFromRules(rules), {
                target: 70,
                rules_to: RULES_TO,
            });
        });

        it('composes a replayed marker to null', function(){
            assert.strictEqual(retroactiveFromRules(rulesFromMarker({
                ...MARKER,
                state: 'replayed',
            })), null);
        });
    });
});
