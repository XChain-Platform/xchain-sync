/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 * Every service config key read under src/ is declared by getConfig().
 *
 * The live config object is exactly what getConfig() builds, so a key the
 * code reads but the builder never assigns is always undefined in production
 * and its env var is silently inert. Unit tests hand-build partial config
 * objects, so nothing else in the suite can see that gap.
 ********************************************************************/

'use strict';

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');
const acorn  = require('acorn');
const config = require('../../../src/config');

const SRC_DIR = path.resolve(__dirname, '..', '..', '..', 'src');

// Names the service config object is held under at its read sites.
const CONFIG_NAMES = new Set(['config', 'cfg']);

// Keys read but deliberately not declared yet. Each must still be read and still
// be undeclared, so this list can only shrink.
const PENDING = {
    // Selects which release manifest the train-activation gate enforces; its env
    // wiring is decided together with the indexer's train gate, not here alone.
    RELEASE_MANIFEST_PATH: 'src/client/sync.js'
};

// List every .js file under a directory, recursively.
function jsFiles(dir, out){
    for(const entry of fs.readdirSync(dir, { withFileTypes: true })){
        const full = path.join(dir, entry.name);
        if(entry.isDirectory()) jsFiles(full, out);
        else if(entry.name.endsWith('.js')) out.push(full);
    }
    return out;
}

// Collect `config['KEY']` / `cfg['KEY']` reads from code tokens (comments excluded).
function collectReads(file, reads){
    const source = fs.readFileSync(file, 'utf8');
    const tokens = [...acorn.tokenizer(source, { ecmaVersion: 'latest', allowHashBang: true, locations: true })];
    for(let i = 0; i + 3 < tokens.length; i++){
        const [name, open, key, close] = tokens.slice(i, i + 4);
        if(name.type.label !== 'name' || !CONFIG_NAMES.has(name.value)) continue;
        if(open.type.label !== '[' || close.type.label !== ']') continue;
        if(key.type.label !== 'string' || !/^[A-Z][A-Z0-9_]*$/.test(key.value)) continue;
        const site = path.relative(path.dirname(SRC_DIR), file) + ':' + key.loc.start.line;
        if(!reads.has(key.value)) reads.set(key.value, []);
        reads.get(key.value).push(site);
    }
    return reads;
}

function allReads(){
    const reads = new Map();
    for(const file of jsFiles(SRC_DIR, [])) collectReads(file, reads);
    return reads;
}

describe('config key coverage: every config key src/ reads is declared by getConfig()', function(){
    let reads;
    let declared;
    before(function(){
        reads = allReads();
        declared = new Set(Object.keys(config.getConfig()));
    });

    it('finds the known reads, so an empty scan cannot pass', function(){
        for(const key of ['SNAPSHOT_MAX_CONTENT', 'DISPENSERS_RECONCILE_EVERY', 'DISPENSERS_RECONCILE_MAX_INTERVAL_MS']){
            assert.ok(reads.has(key), 'the scan no longer sees the read of ' + key);
        }
    });

    it('declares every key that is read', function(){
        const missing = [];
        for(const [key, sites] of reads){
            if(declared.has(key) || Object.prototype.hasOwnProperty.call(PENDING, key)) continue;
            missing.push(key + ' (read at ' + sites.join(', ') + ')');
        }
        assert.deepStrictEqual(missing, [],
            'read off the service config but never assigned in getConfig(), so the env var is inert:\n  ' +
            missing.join('\n  '));
    });

    it('keeps every pending key both read and undeclared', function(){
        for(const [key, file] of Object.entries(PENDING)){
            assert.ok((reads.get(key) || []).some(site => site.startsWith(file + ':')),
                key + ' is no longer read in ' + file + '; drop it from PENDING');
            assert.ok(!declared.has(key), key + ' is now declared; drop it from PENDING');
        }
    });
});
