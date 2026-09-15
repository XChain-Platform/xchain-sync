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
const validation = require('../../../src/util/validation');

describe('validation', function(){

    // ── validateWsEvent ──

    describe('validateWsEvent', function(){

        it('accepts valid block event', function(){
            let result = validation.validateWsEvent({ type: 'block', block_index: 100 });
            assert.strictEqual(result.valid, true);
            assert.strictEqual(result.type, 'block');
        });

        it('accepts valid reorg event', function(){
            let result = validation.validateWsEvent({ type: 'reorg', block_index: 50 });
            assert.strictEqual(result.valid, true);
            assert.strictEqual(result.type, 'reorg');
        });

        it('accepts valid status event with block_height', function(){
            let result = validation.validateWsEvent({ type: 'status', block_height: 100 });
            assert.strictEqual(result.valid, true);
            assert.strictEqual(result.type, 'status');
        });

        it('accepts status event with null block_height', function(){
            let result = validation.validateWsEvent({ type: 'status', block_height: null });
            assert.strictEqual(result.valid, true);
        });

        it('accepts status event without block_height', function(){
            let result = validation.validateWsEvent({ type: 'status' });
            assert.strictEqual(result.valid, true);
        });

        it('accepts block event with block_index 0', function(){
            let result = validation.validateWsEvent({ type: 'block', block_index: 0 });
            assert.strictEqual(result.valid, true);
        });
    });
});

describe('validation', function(){

    describe('validateWsEvent', function(){

        it('rejects null event', function(){
            let result = validation.validateWsEvent(null);
            assert.strictEqual(result.valid, false);
        });

        it('rejects undefined event', function(){
            let result = validation.validateWsEvent(undefined);
            assert.strictEqual(result.valid, false);
        });

        it('rejects string event', function(){
            let result = validation.validateWsEvent('block');
            assert.strictEqual(result.valid, false);
        });

        it('rejects array event', function(){
            let result = validation.validateWsEvent([{ type: 'block' }]);
            assert.strictEqual(result.valid, false);
        });

        it('rejects missing type field', function(){
            let result = validation.validateWsEvent({ block_index: 100 });
            assert.strictEqual(result.valid, false);
        });

        it('rejects unknown event type', function(){
            let result = validation.validateWsEvent({ type: 'inject' });
            assert.strictEqual(result.valid, false);
            assert.ok(result.reason.includes('inject'));
        });
    });
});

describe('validation', function(){

    describe('validateWsEvent', function(){

        it('rejects block event with non-numeric block_index', function(){
            let result = validation.validateWsEvent({ type: 'block', block_index: 'abc' });
            assert.strictEqual(result.valid, false);
        });

        it('rejects block event with missing block_index', function(){
            let result = validation.validateWsEvent({ type: 'block' });
            assert.strictEqual(result.valid, false);
        });

        it('rejects block event with negative block_index', function(){
            let result = validation.validateWsEvent({ type: 'block', block_index: -1 });
            assert.strictEqual(result.valid, false);
        });

        it('rejects reorg event with missing block_index', function(){
            let result = validation.validateWsEvent({ type: 'reorg' });
            assert.strictEqual(result.valid, false);
        });

        it('rejects block event with NaN block_index', function(){
            let result = validation.validateWsEvent({ type: 'block', block_index: NaN });
            assert.strictEqual(result.valid, false);
        });

        it('rejects block event with Infinity block_index', function(){
            let result = validation.validateWsEvent({ type: 'block', block_index: Infinity });
            assert.strictEqual(result.valid, false);
        });

        it('rejects status event with non-numeric block_height', function(){
            let result = validation.validateWsEvent({ type: 'status', block_height: 'abc' });
            assert.strictEqual(result.valid, false);
        });
    });
});
