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
const fs = require('fs');
const path = require('path');

const FILES = [
    '../../../src/server/snapshot_builder/table_sets.js',
    '../../../src/server/snapshot_builder.js',
    '../../../src/client/rollback.js',
    '../../../src/client/applier.js',
    '../../../src/schema/replicated_tables.js',
];
const STALE = [
    /ride\s+along\s+in\s+the\s+full\s+snapshot\s+only/i,
    /next\s+full\s+snapshot\s+would\s+eventually\s+overwrite/i,
];

function flatten(source){
    return source.replace(/[\r\n]+[ \t]*(?:\/\/+|\*+)?[ \t]*/g, ' ');
}

describe('operator-local snapshot wording', function(){
    for(const rel of FILES){
        it(rel + ' does not assign operator-local tables to snapshots', function(){
            const source = flatten(fs.readFileSync(path.resolve(__dirname, rel), 'utf8'));
            assert.strictEqual(STALE.some(pattern => pattern.test(source)), false,
                'icons and price_snapshots are operator-local, never shipped in snapshots; ' +
                'price_snapshots reaches a node only through its own hub_db_sync');
        });
    }
});
