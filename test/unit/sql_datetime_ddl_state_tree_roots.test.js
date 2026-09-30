// Copyright © 2025-2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');

const DDL_PATH = path.join(__dirname, '..', '..', 'src', 'sql', 'state_tree_roots.sql');
const ddl = fs.readFileSync(DDL_PATH, 'utf8')
    .replace(/--.*$/gm, '')
    .split('\n')
    .map(line => line.trim())
    .join('\n');

describe('state_tree_roots SQL datetime DDL', function () {

    it('declares no column with the TIMESTAMP type', function () {
        assert.doesNotMatch(ddl, /^`?[A-Za-z_][A-Za-z0-9_]*`?\s+TIMESTAMP\b/im);
    });

    it('declares computed_at as DATETIME with its existing default', function () {
        assert.match(ddl, /^computed_at\s+DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,/im);
    });

    it('keeps contract_state_root nullable', function () {
        assert.match(ddl, /^contract_state_root\s+CHAR\(64\) NULL,/im);
    });
});
