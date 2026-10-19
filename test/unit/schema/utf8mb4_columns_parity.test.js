/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 *********************************************************************/

'use strict';

// src/schema/utf8mb4_columns.js is a synced copy of the canonical inventory in
// xchain-indexer/src/chain/utf8mb4_columns.js. It equals the canonical text with the
// provenance paragraph swapped for the SYNCED COPY paragraph and nothing else, so a
// widen-set edit made only here, or only in the indexer, fails this test.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');
const { siblingCheckout } = require('../../helpers/sibling_checkout.js');

const PARAGRAPH_START = ' * CANONICAL INVENTORY';
const COPY_PARAGRAPH = [
    ' * SYNCED COPY of the canonical inventory in xchain-indexer/src/chain/utf8mb4_columns.js.',
    ' * Never edit this file: change the canonical file, then run',
    ' * `node bin/sync-utf8mb4-columns.js --write` in xchain-indexer. xchain-sync has no',
    ' * dependency on that package by design, so it carries this copy.',
];

const COPY_PATH = path.resolve(__dirname, '../../../src/schema/utf8mb4_columns.js');

function indexerCanonicalPath(){
    const root = process.env.XCHAIN_INDEXER_SQL_PATH
        ? path.resolve(process.env.XCHAIN_INDEXER_SQL_PATH, '..', '..')
        : path.resolve(__dirname, '..', '..', '..', '..', 'xchain-indexer');
    return path.join(root, 'src', 'chain', 'utf8mb4_columns.js');
}

// The text the copy must hold for a given canonical text.
function renderCopy(canonicalText){
    const lines = canonicalText.split('\n');
    const start = lines.findIndex(l => l.startsWith(PARAGRAPH_START));
    assert.ok(start >= 0, 'canonical inventory has no "' + PARAGRAPH_START.trim() + '" paragraph');
    let end = start + 1;
    while(end < lines.length && lines[end] !== ' *') end++;
    assert.ok(end < lines.length, 'canonical provenance paragraph is not terminated');
    return lines.slice(0, start).concat(COPY_PARAGRAPH, lines.slice(end)).join('\n');
}

describe('utf8mb4 inventory synced copy', function(){
    it('carries the SYNCED COPY provenance paragraph', function(){
        const text = fs.readFileSync(COPY_PATH, 'utf8');
        assert.ok(text.includes(COPY_PARAGRAPH[0]), 'copy lost its SYNCED COPY paragraph');
        assert.ok(!text.includes(PARAGRAPH_START), 'copy still carries the canonical provenance paragraph');
    });

    it('equals the xchain-indexer canonical inventory rendered as a copy', function(){
        const canonical = indexerCanonicalPath();
        const verdict = siblingCheckout(__dirname, canonical);
        if(!verdict.usable){
            if(process.env.XCHAIN_REQUIRE_SIBLINGS === '1')
                throw new Error('utf8mb4 parity cannot run: ' + verdict.reason);
            return this.skip();
        }
        assert.strictEqual(fs.readFileSync(COPY_PATH, 'utf8'), renderCopy(fs.readFileSync(canonical, 'utf8')),
            'src/schema/utf8mb4_columns.js drifted from the xchain-indexer canonical inventory; ' +
            'run `node bin/sync-utf8mb4-columns.js --write` in xchain-indexer');
    });
});
