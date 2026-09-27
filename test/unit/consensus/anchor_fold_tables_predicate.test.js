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
const TABLES = require('../../../src/db/tables.js');

describe('archive fold tables predicate', function () {
    it('selects archive heads with the fold row predicate', async function () {
        let captured;
        const connection = {
            async query(sql, args){
                captured = { sql, args };
                return [];
            }
        };
        const database = {
            doQuery(sql, args, conn){
                return conn.query(sql, args);
            }
        };

        await TABLES.findInvalidArchiveHeadRows.call(database, 40, 60, connection);

        assert.ok(captured.sql.includes('p.match_batch_seq IS NOT NULL AND p.version <> 2'));
        assert.deepStrictEqual(captured.args, [40, 60]);
    });
});
