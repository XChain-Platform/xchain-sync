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
const fs     = require('fs');
const path   = require('path');

describe('Advisory table-content parity', function(){

    describe('wiring', function(){

        const src = fs.readFileSync(path.join(__dirname, '../../../src/client/sync.js'), 'utf8');
        const body = src.slice(src.indexOf('async verifyTableContentParity('),
                               src.indexOf('async recordTableContentMismatch('));

        it('case 7: the advisory check never halts', function(){
            assert.ok(body.length > 200, 'verifyTableContentParity body not found; update this guard');
            assert.ok(!/haltOnDivergence|this\.halted\s*=/.test(body),
                'table-content parity is advisory: it must never halt a follower');
        });

        it('runs only at the source\'s published height, behind the flag', function(){
            assert.ok(/TABLE_CONTENT_PARITY_CHECK/.test(body), 'the check must stay behind its flag');
            assert.ok(/Number\(remoteStatus\.block_height\) !== Number\(blockHeight\)/.test(body),
                'comparing across heights would compare different windows');
        });

        it('feeds the source\'s window and id ceilings back into the local recompute', function(){
            assert.ok(/window:\s*remoteParity\.window/.test(body), 'both sides must use the SOURCE window');
            assert.ok(/idBounds/.test(body), 'both sides must use the SOURCE id ceilings');
        });

        it('is wired into the decoder path too, which has no hashes at all', function(){
            let start = src.indexOf('async verifyDecoderCompleteness(');
            assert.ok(start !== -1, 'verifyDecoderCompleteness not found; update this guard');
            const decoderBody = src.slice(start, src.indexOf('\n    async ', start + 10));
            assert.ok(/verifyTableContentParity\(/.test(decoderBody),
                'the decoder DB has no ledger/actions/contract hash, so this is its only content signal');
        });

        it('the /status producer publishes the payload for both dbTypes, default null', function(){
            const api = fs.readFileSync(path.join(__dirname, '../../../src/api.js'), 'utf8');
            assert.ok(/row\.table_content_parity = null;/.test(api), 'default null so a follower skips');
            assert.ok(/cfg\['TABLE_CONTENT_PARITY_CHECK'\]/.test(api), 'publishing stays behind the flag');
            // It must sit OUTSIDE the indexer-only branch that owns index_map_checksum.
            let idx = api.indexOf('row.table_content_parity = null;');
            let mapIdx = api.indexOf('row.index_map_checksum = null;');
            assert.ok(mapIdx !== -1 && idx > mapIdx, 'unexpected api.js layout; update this guard');
            assert.ok(!/dbType === 'decoder'[\s\S]{0,200}table_content_parity/.test(api));
        });
    });
});
