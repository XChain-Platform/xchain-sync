// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const {
    previousBlockHash, decoderLinkBroken, decoderLinkState
} = require('../../../src/client/decoder_link');

describe('previousBlockHash', function(){
    it('returns null without a block row or previous hash id', async function(){
        assert.strictEqual(await previousBlockHash(null, []), null);
        assert.strictEqual(await previousBlockHash({ previous_block_hash_id: null }, []), null);
        assert.strictEqual(await previousBlockHash({}, []), null);
    });

    it('finds the hash in supplied rows by string-equivalent id', async function(){
        let calls = 0;
        const db = { findIndexTransactionsByIds: async () => { calls++; return []; } };

        const hash = await previousBlockHash(
            { previous_block_hash_id: 7 }, [{ id: '7', hash: 'parent' }], db
        );

        assert.strictEqual(hash, 'parent');
        assert.strictEqual(calls, 0);
    });

    it('does not query the database when a supplied matching row has no hash', async function(){
        let calls = 0;
        const db = { findIndexTransactionsByIds: async () => {
            calls++;
            return [{ id: 7, hash: 'stored-parent' }];
        } };

        const hash = await previousBlockHash(
            { previous_block_hash_id: 7 }, [{ id: '7', hash: null }], db
        );

        assert.strictEqual(hash, null);
        assert.strictEqual(calls, 0);
    });

    it('falls back to the database when the supplied rows miss', async function(){
        let calls = 0;
        let requestedIds;
        const db = { findIndexTransactionsByIds: async ids => {
            calls++;
            requestedIds = ids;
            return [{ id: '7', hash: 'stored-parent' }];
        } };

        const hash = await previousBlockHash({ previous_block_hash_id: 7 }, [], db);

        assert.strictEqual(hash, 'stored-parent');
        assert.strictEqual(calls, 1);
        assert.deepStrictEqual(requestedIds, [7]);
    });

    it('returns null when the database has no lookup method', async function(){
        assert.strictEqual(await previousBlockHash(
            { previous_block_hash_id: 7 }, [], {}
        ), null);
    });

    it('returns null when no matching row carries a hash', async function(){
        const db = { findIndexTransactionsByIds: async () => [{ id: 8, hash: 'other' }] };
        const emptyDb = { findIndexTransactionsByIds: async () => undefined };

        assert.strictEqual(await previousBlockHash({ previous_block_hash_id: 7 }, [], db), null);
        assert.strictEqual(await previousBlockHash({ previous_block_hash_id: 7 }, undefined, emptyDb), null);
    });
});

describe('decoderLinkBroken', function(){
    it('is false without a tip hash or block row', async function(){
        assert.strictEqual(await decoderLinkBroken({ previous_block_hash_id: 7 }, [], null), false);
        assert.strictEqual(await decoderLinkBroken(null, [], 'tip'), false);
    });

    it('is false when the previous hash is unknown', async function(){
        assert.strictEqual(await decoderLinkBroken(
            { previous_block_hash_id: 7 }, [], 'tip', {}
        ), false);
    });

    it('is true when the previous hash differs from the tip', async function(){
        assert.strictEqual(await decoderLinkBroken(
            { previous_block_hash_id: 7 }, [{ id: 7, hash: 'parent' }], 'tip'
        ), true);
    });

    it('is false when the previous hash equals the tip', async function(){
        assert.strictEqual(await decoderLinkBroken(
            { previous_block_hash_id: 7 }, [{ id: 7, hash: 'tip' }], 'tip'
        ), false);
    });
});

describe('decoderLinkState', function(){
    it('is linked without a block row or previous hash id', async function(){
        assert.strictEqual(await decoderLinkState(null, [], 'tip'), 'linked');
        assert.strictEqual(await decoderLinkState({ previous_block_hash_id: null }, [], 'tip'), 'linked');
        assert.strictEqual(await decoderLinkState({}, [], 'tip'), 'linked');
    });

    it('is unresolved without a tip hash', async function(){
        assert.strictEqual(await decoderLinkState(
            { previous_block_hash_id: 7 }, [{ id: 7, hash: 'parent' }], null
        ), 'unresolved');
    });

    it('is unresolved when the previous row cannot be found', async function(){
        const db = { findIndexTransactionsByIds: async () => undefined };

        assert.strictEqual(await decoderLinkState(
            { previous_block_hash_id: 7 }, undefined, 'tip', db
        ), 'unresolved');
    });

    it('is linked when the previous row has no hash', async function(){
        assert.strictEqual(await decoderLinkState(
            { previous_block_hash_id: 7 }, [{ id: '7', hash: null }], 'tip'
        ), 'linked');
    });

    it('compares the previous hash with the tip', async function(){
        const block = { previous_block_hash_id: 7 };

        assert.strictEqual(await decoderLinkState(
            block, [{ id: 7, hash: 'tip' }], 'tip'
        ), 'linked');
        assert.strictEqual(await decoderLinkState(
            block, [{ id: 7, hash: 'parent' }], 'tip'
        ), 'broken');
    });
});
