// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers pagination fixtures and hooks. One part of transparency_page.test.js.
const sinon = require('sinon');
const { withDbMixins } = require('../../../../helpers/db_mixins.js');

function createMockDb(total){
    return withDbMixins({
        doQuery: sinon.stub().callsFake(async (query, args) => {
            if(query.includes('COUNT'))
                return [{ total: total || 0 }];
            return [];
        })
    });
}

function registerHooks(){
    afterEach(function(){ sinon.restore(); });
}

module.exports = { createMockDb, registerHooks };
