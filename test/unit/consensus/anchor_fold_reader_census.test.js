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

const ROOT = path.join(__dirname, '../../..');
const VERSION_SPLICE = '"WHERE p.version " + ARCHIVE_HEAD_VERSIONS_SQL';
const CHUNK_JOIN = 'c.version = 2 AND c.match_batch_seq = p.match_batch_seq';
const READER_SQL = 'anchor_actions p';
const FOLD_NOTICE = 'the fold row must update this census when it moves the reader';

function read(relativePath){
    return fs.readFileSync(path.join(ROOT, relativePath), 'utf8');
}

function count(source, text){
    return source.split(text).length - 1;
}

function failure(relativePath, expectation){
    return relativePath + ' must satisfy this census, ' + expectation + '; ' + FOLD_NOTICE;
}

describe('anchor fold reader census', function () {
    it('pins the rollback invalid-archive reader', function () {
        const relativePath = 'src/client/rollback.js';
        const source = read(relativePath);

        assert.strictEqual(count(source, VERSION_SPLICE), 1,
            failure(relativePath, 'expected exactly one archive-head version splice'));
        assert.strictEqual(count(source, CHUNK_JOIN), 1,
            failure(relativePath, 'expected exactly one invalid_archive v2 chunk join'));
    });

    it('pins the database updated-rows reader', function () {
        const relativePath = 'src/db/tables.js';
        const source = read(relativePath);
        const methodStart = source.indexOf('async findInvalidArchiveHeadRows(');
        const methodEnd = source.indexOf('\n    },', methodStart);
        const method = methodStart < 0 || methodEnd < 0 ? '' : source.slice(methodStart, methodEnd);

        assert.strictEqual(count(method, "archiveHeadPredicate('p')"), 1,
            failure(relativePath, 'expected exactly one fold row predicate inside findInvalidArchiveHeadRows'));
        assert.strictEqual(count(method, VERSION_SPLICE), 0,
            failure(relativePath, 'expected no archive-head version splice inside findInvalidArchiveHeadRows'));
    });

    it('keeps the updated-rows server layer free of the archive-head SQL reader', function () {
        const directory = 'src/server/updated_rows';
        const files = ['src/server/updated_rows.js'].concat(
            fs.readdirSync(path.join(ROOT, directory))
                .filter(file => file.endsWith('.js'))
                .map(file => path.join(directory, file)));

        for(const relativePath of files){
            assert.strictEqual(count(read(relativePath), READER_SQL), 0,
                failure(relativePath, 'expected no direct archive-head SQL reader'));
        }
    });
});
