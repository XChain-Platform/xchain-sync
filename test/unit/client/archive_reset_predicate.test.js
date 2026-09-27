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
const { archiveResetWhereClause } = require('../../../src/client/archive_reset_predicate');

describe('Archive Reset Predicate', function(){
    it('builds the stamped archive-head rollback predicate', function(){
        assert.strictEqual(
            archiveResetWhereClause(),
            'WHERE p.match_batch_seq IS NOT NULL AND p.version <> 2 AND p.action_index < ?'
        );
    });

    it('does not leak the shared SQL constant name', function(){
        assert.strictEqual(archiveResetWhereClause().includes('ARCHIVE_HEAD_VERSIONS_SQL'), false);
    });
});
