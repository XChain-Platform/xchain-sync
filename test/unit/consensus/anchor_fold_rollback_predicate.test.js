'use strict';

// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const ROLLBACK_PATH = path.join(__dirname, '../../../src/client/rollback.js');

describe('archive fold rollback predicate', function () {
    it('resets invalid archive heads with the fold row predicate', function () {
        const source = fs.readFileSync(ROLLBACK_PATH, 'utf8');

        assert.ok(source.includes(
            '"WHERE p.version " + ARCHIVE_HEAD_VERSIONS_SQL + " AND p.action_index < ? AND " + archiveHeadPredicate(\'p\')'));
        assert.ok(!source.includes(
            '"WHERE p.version " + ARCHIVE_HEAD_VERSIONS_SQL + " AND p.action_index < ?",'));
    });
});
