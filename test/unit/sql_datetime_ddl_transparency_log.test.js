// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const DECLARATIONS = [
    ['merkle_epochs.sql', 'created_at'],
    ['merkle_reorgs.sql', 'detected_at']
];

describe('transparency-log DDL datetime columns', function(){
    for(const [file, column] of DECLARATIONS){
        it(`${file} declares ${column} as DATETIME without TIMESTAMP columns`, function(){
            const sql = fs.readFileSync(path.join(__dirname, '../../src/sql', file), 'utf8');
            const body = sql.replace(/--[^\n]*/g, '')
                .split('\n').map(line => line.trim()).join('\n');

            assert.doesNotMatch(body, /^`?[A-Za-z_]\w*`?\s+TIMESTAMP\b/gim);
            assert.match(body, new RegExp(`^${column}\\s+DATETIME\\b`, 'mi'));
        });
    }
});
