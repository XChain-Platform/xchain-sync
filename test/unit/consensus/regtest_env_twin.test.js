'use strict';

// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

// The vendored regtest arming grammar must carry the time-override helper the
// indexer canonical exports, so a registry row that reads it resolves here too.

const assert = require('assert');
const path = require('path');

const regtestEnv = require(path.join(__dirname, '../../../src/consensus/gate_registry/regtest_env.js'));

const SCRATCH_VAR = 'XCHAIN_REGTEST_ENV_TWIN_SCRATCH_TIME';

describe('vendored regtest_env twin', function () {
    let saved;

    beforeEach(function () { saved = process.env[SCRATCH_VAR]; });

    afterEach(function () {
        if (saved === undefined) delete process.env[SCRATCH_VAR];
        else process.env[SCRATCH_VAR] = saved;
    });

    it('exports regtestTimeOverride', function () {
        assert.strictEqual(typeof regtestEnv.regtestTimeOverride, 'function');
    });

    it('reads the scratch variable when set and 0 when unset', function () {
        const read = regtestEnv.regtestTimeOverride(SCRATCH_VAR);
        process.env[SCRATCH_VAR] = '1790000000';
        assert.strictEqual(read(), 1790000000);
        delete process.env[SCRATCH_VAR];
        assert.strictEqual(read(), 0);
    });

    it('still exports regtestHeight', function () {
        assert.strictEqual(typeof regtestEnv.regtestHeight, 'function');
    });
});
