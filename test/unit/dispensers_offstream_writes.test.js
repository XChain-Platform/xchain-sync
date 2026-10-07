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
// Drift guard for the decoder `dispensers` off-stream write list.
//
// The header of src/schema/replicated_tables.js names every decoder write that
// mutates dispensers off the per-block stream, and any channel replacing the
// full-table reconcile must carry each one. The list is hand-kept, so this guard
// counts the UPDATE/DELETE statements on dispensers in xchain-decoder/src and
// requires every writer to be named in that header.

'use strict';

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

// Read the decoder's runtime sources, not its schema, so a schema-only override cannot point here.
const DECODER_SRC_DIR = process.env.XCHAIN_DECODER_SRC_PATH
    || path.join(__dirname, '../../../xchain-decoder/src');
const DECODER_SQL_DIR = path.join(DECODER_SRC_DIR, 'sql');
const DECODER_MARKER  = path.join(DECODER_SRC_DIR, 'db', 'dispenser_queries.js');
const HEADER_FILE     = path.join(__dirname, '../../src/schema/replicated_tables.js');

// Fail on an absent sibling where the sibling is required; skip in a standalone checkout.
const SIBLING_REQUIRED = process.env.XCHAIN_REQUIRE_SIBLINGS === '1';
function requireSibling(ctx){
    if(fs.existsSync(DECODER_MARKER)) return true;
    if(SIBLING_REQUIRED)
        throw new Error('dispensers write-list guard cannot run: no decoder sources at ' +
            DECODER_SRC_DIR + ' (check out xchain-decoder beside this repo or set XCHAIN_DECODER_SRC_PATH)');
    ctx.skip();
    return false;
}

// Each decoder function that writes dispensers off the stream, with its statement count.
const PINNED_WRITERS = {
    deleteOpenDispensers:                   1,
    extendOpenDispenserExpirationBySource:  1,
    restoreDispenserExtensions:             1,
    deleteBlockRows:                        2,
    purgeExpiredDispensers:                 1
};
const PINNED_STATEMENTS = Object.values(PINNED_WRITERS).reduce((a, b) => a + b, 0);

// Match an in-place write on dispensers only (\b keeps dispenser_extension_undo out).
const WRITE_RE = /\b(?:UPDATE|DELETE\s+FROM)\s+dispensers\b/gi;

// List runtime .js sources, skipping src/sql (one-time migrations are not runtime writes).
function decoderSources(dir){
    let out = [];
    for(const ent of fs.readdirSync(dir, { withFileTypes: true })){
        const abs = path.join(dir, ent.name);
        if(ent.isDirectory()){
            if(abs === DECODER_SQL_DIR || ent.name === 'node_modules') continue;
            out = out.concat(decoderSources(abs));
        } else if(ent.name.endsWith('.js')){
            out.push(abs);
        }
    }
    return out;
}

function headerComment(){
    const src = fs.readFileSync(HEADER_FILE, 'utf8');
    const end = src.indexOf('*/');
    assert.ok(end > 0, 'no header comment found in ' + HEADER_FILE);
    return src.slice(0, end);
}

describe('decoder dispensers off-stream write list @regression', function(){

    it('counts exactly the pinned UPDATE/DELETE statements on dispensers in xchain-decoder/src', function(){
        if(!requireSibling(this)) return;
        const files = decoderSources(DECODER_SRC_DIR);
        assert.ok(files.length > 0, 'decoder source walk found no .js files under ' + DECODER_SRC_DIR);

        const hits = [];
        for(const file of files){
            const text = fs.readFileSync(file, 'utf8');
            for(const m of text.matchAll(WRITE_RE))
                hits.push(path.relative(DECODER_SRC_DIR, file) + ': ' + m[0].replace(/\s+/g, ' '));
        }
        assert.ok(hits.length > 0, 'found no dispensers writes at all; the walk or the pattern is broken');
        assert.strictEqual(hits.length, PINNED_STATEMENTS,
            'xchain-decoder/src has ' + hits.length + ' UPDATE/DELETE statements on dispensers, ' +
            'the pin expects ' + PINNED_STATEMENTS + ':\n  ' + hits.join('\n  ') +
            '\nName any new writer in the dispensers entry of the src/schema/replicated_tables.js ' +
            'header, since a channel replacing the full-table reconcile must carry it, then update ' +
            'PINNED_WRITERS here.');
    });

    it('finds every pinned writer in xchain-decoder/src', function(){
        if(!requireSibling(this)) return;
        const corpus = decoderSources(DECODER_SRC_DIR).map(f => fs.readFileSync(f, 'utf8')).join('\n');
        const missing = Object.keys(PINNED_WRITERS).filter(name => !corpus.includes(name));
        assert.deepStrictEqual(missing, [],
            'pinned dispensers writer(s) no longer exist in xchain-decoder/src: ' + missing.join(', ') +
            '. Update PINNED_WRITERS and the replicated_tables.js header together.');
    });

    it('names every pinned writer in the replicated_tables.js header', function(){
        const header = headerComment();
        const missing = Object.keys(PINNED_WRITERS).filter(name => !header.includes(name));
        assert.deepStrictEqual(missing, [],
            'the dispensers entry in the src/schema/replicated_tables.js header does not name: ' +
            missing.join(', '));
    });
});
