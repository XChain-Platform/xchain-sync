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
 * XChain Sync - checkpoint supersession suffix byte contract
 *
 * Covers the pure suffix shared with checkpoint producers and verifiers.
 *
 ********************************************************************/

'use strict';

const assert = require('assert');
const { canonicalCheckpoint } = require('../../../src/checkpoint');
const {
    hasSupersedeFields,
    supersedeSuffix,
} = require('../../../src/client/supersede_suffix');

const digest = 'CD'.repeat(32);

describe('Supersede suffix bytes @regression', function(){
    it('returns an empty suffix when both supersession fields are absent or null', function(){
        assert.strictEqual(hasSupersedeFields({}), false);
        assert.strictEqual(hasSupersedeFields({ supersedes_from: null, rules_digest: null }), false);
        assert.strictEqual(supersedeSuffix({ block_index: 5 }), '');
        assert.strictEqual(supersedeSuffix({ supersedes_from: null, rules_digest: null }), '');
    });

    it('detects either non-null supersession field', function(){
        assert.strictEqual(hasSupersedeFields({ supersedes_from: 0 }), true);
        assert.strictEqual(hasSupersedeFields({ rules_digest: digest }), true);
    });

    it('serializes string supersedes_from and an uppercase rules_digest to exact bytes', function(){
        assert.strictEqual(
            supersedeSuffix({ supersedes_from: '12', rules_digest: digest }),
            '|SUPERSEDE|12|' + digest.toLowerCase()
        );
    });

    it('throws when exactly one supersession field is present', function(){
        assert.throws(() => supersedeSuffix({ supersedes_from: 3 }), /requires supersedes_from and rules_digest/);
        assert.throws(() => supersedeSuffix({ rules_digest: digest }), /requires supersedes_from and rules_digest/);
    });

    it('throws when supersedes_from is not a non-negative safe integer', function(){
        for(const value of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1, 'not-a-number'])
            assert.throws(
                () => supersedeSuffix({ supersedes_from: value, rules_digest: digest }),
                /non-negative safe integer/
            );
    });

    it('throws when rules_digest is not exactly 64 hexadecimal characters', function(){
        for(const value of ['cd', 'g'.repeat(64), null, 12])
            assert.throws(
                () => supersedeSuffix({ supersedes_from: 1, rules_digest: value }),
                value === null ? /requires supersedes_from and rules_digest/ : /64 hexadecimal characters/
            );
    });

    it('leaves a plain canonicalCheckpoint free of SUPERSEDE text with the helper loaded', function(){
        const cp = {
            chain: 'BTC',
            network: 'mainnet',
            block_index: 900123,
            block_hash: 'ab'.repeat(32),
            ledger_hash: 'cd'.repeat(32),
            actions_hash: 'ef'.repeat(32),
            contract_hash: '01'.repeat(32),
            checkpoint_seq: 417,
            snapshot_block: 900120,
        };

        assert.strictEqual(canonicalCheckpoint(cp).includes('SUPERSEDE'), false);
    });
});
