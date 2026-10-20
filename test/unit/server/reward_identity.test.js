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
 **********************************************************************/

// test/unit/server/reward_identity.test.js
//
// Pins the validator-reward dedup key to the replica's reward_unique key and to every
// payload merge that dedupes reward rows, so a column change cannot land in one place only.

'use strict';

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');
const { REWARD_IDENTITY_COLUMNS, validatorRewardKey } = require('../../../src/server/reward_identity.js');

const SRC = path.join(__dirname, '../../../src');

// The payload merges that dedupe validator_rewards rows by identity.
const KEY_SITES = ['server/poller.js', 'server/snapshot_builder.js', 'server/derived_rewards.js', 'server/recovery_rewards.js'];

// Drop // comments so prose that quotes the old key cannot satisfy or trip the scan.
function code(rel){
    return fs.readFileSync(path.join(SRC, rel), 'utf8').split('\n')
        .map(line => line.replace(/^\s*\/\/.*$/, '')).join('\n').replace(/\s+/g, ' ');
}

describe('validator-reward identity key', function(){
    it('names the reward_unique columns, in key order', function(){
        const ddl = fs.readFileSync(path.join(SRC, 'sql/migrations/2026-10-19-replica-keys.sql'), 'utf8');
        const m = ddl.match(/ADD UNIQUE INDEX `reward_unique` \(([^)]*)\)/);
        assert.ok(m, 'the reward_unique definition was not found in 2026-10-19-replica-keys.sql');
        const cols = m[1].split(',').map(c => c.trim().replace(/`/g, ''));
        assert.deepStrictEqual(REWARD_IDENTITY_COLUMNS, cols,
            'REWARD_IDENTITY_COLUMNS drifted from reward_unique; a narrower dedup key drops distinct rewards');
    });

    it('keeps the colon-joined key format, null included, and separates rows that differ only in round_qualifier', function(){
        const row = { source_id: 7, signing_pubkey_id: 3n, reward_type: 'anchor_archive', round_reference: 12, round_qualifier: null };
        assert.strictEqual(validatorRewardKey(row), '7:3:anchor_archive:12:null');
        assert.notStrictEqual(validatorRewardKey(Object.assign({}, row, { round_qualifier: 100 })),
            validatorRewardKey(Object.assign({}, row, { round_qualifier: 200 })));
    });

    it('is the only reward dedup key the payload merges build', function(){
        for(const rel of KEY_SITES){
            const src = code(rel);
            assert.ok(src.includes('validatorRewardKey('), rel + ' must key reward rows with validatorRewardKey');
            assert.doesNotMatch(src, /\.(source_id|signing_pubkey_id|reward_type|round_reference|round_qualifier) \+ ':'/,
                rel + ' hand-builds a reward key; use validatorRewardKey so a key change lands everywhere at once');
        }
    });
});
