// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// The recording database double the updated-rows suites share: the real db mixins
// over a doQuery stub that answers by SQL substring and records every (sql, args).

'use strict';

const sinon = require('sinon');
const { withDbMixins } = require('../../../helpers/db_mixins.js');

function fakeDb(routes){
    let calls = [];
    return withDbMixins({
        calls,
        dbType: 'indexer',
        doQuery: sinon.stub().callsFake(async (sql, args) => {
            calls.push({ sql, args });
            for(let r of routes || []){
                if(sql.indexOf(r.match) !== -1) return r.rows;
            }
            return [];
        }),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves(),
        getBlockHashRow: sinon.stub().resolves(null)
    });
}

module.exports = { fakeDb };
