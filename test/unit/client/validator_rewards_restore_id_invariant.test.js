// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// validator_rewards streams with its source AUTO_INCREMENT id and is applied with
// INSERT IGNORE, so a carried id that lands on a locally minted one is dropped on the
// PRIMARY KEY. The only statement that lets the replica mint an id is the RB-ANCHOR
// restore, and it is safe because the source runs the identical statement over the
// same replicated log rows. Full snapshot replacement also resets the replica
// allocator to MAX(id)+1, keeping its counter at or below the source's. These
// guards fail when either half of that argument is broken.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');
const { siblingCheckout, skipOrFail } = require('../../helpers/sibling_checkout.js');

const SRC = path.resolve(__dirname, '..', '..', '..', 'src');
const SYNC_RESTORE = path.join(SRC, 'client', 'rollback.js');
const APPLIER = path.join(SRC, 'client', 'applier.js');
const INDEXER_PURGE = path.resolve(__dirname, '..', '..', '..', '..', 'xchain-indexer', 'src', 'db', 'rollback', 'purge.js');

// Quotes, backticks and string-concatenation joins are not part of the statement.
const normalize = (s) => s.replace(/[`"']/g, ' ').replace(/\s+\+\s+/g, ' ').replace(/\s+/g, ' ');

// The restore statement, from its INSERT through the end of its derive-height predicate.
function restoreStatement(text){
    const m = normalize(text).match(/INSERT IGNORE INTO validator_rewards.*?reward_derive_block_index < \?\)/);
    return m ? m[0] : null;
}

function codeFilesUnder(dir){
    let out = [];
    for(const e of fs.readdirSync(dir, { withFileTypes: true })){
        const p = path.join(dir, e.name);
        if(e.isDirectory()) out = out.concat(codeFilesUnder(p));
        else if(e.name.endsWith('.js')) out.push(p);
    }
    return out;
}

describe('validator_rewards restore id invariant', function(){
    it('the replica RB-ANCHOR restore matches the source restore statement', function(){
        const verdict = siblingCheckout(__dirname, INDEXER_PURGE);
        if(!skipOrFail(this, verdict, 'the validator_rewards restore parity guard')) return;
        const replica = restoreStatement(fs.readFileSync(SYNC_RESTORE, 'utf8'));
        const source  = restoreStatement(fs.readFileSync(INDEXER_PURGE, 'utf8'));
        assert.ok(replica, 'xchain-sync src/client/rollback.js must keep the RB-ANCHOR validator_rewards restore');
        assert.ok(source, 'xchain-indexer src/db/rollback/purge.js must keep the RB-ANCHOR validator_rewards restore');
        assert.strictEqual(replica, source,
            'the two restores must insert the same row set, or replica-assigned ids can overtake the source ids ' +
            'and a later streamed reward is dropped on the PRIMARY KEY under INSERT IGNORE');
    });

    it('the restore names no id column, so both sides mint theirs from the same log rows', function(){
        const replica = restoreStatement(fs.readFileSync(SYNC_RESTORE, 'utf8'));
        assert.ok(replica, 'the restore statement must be present');
        const columns = replica.match(/validator_rewards \(([^)]*)\)/);
        assert.ok(columns, 'the restore must list its insert columns');
        assert.ok(!/\bid\b/.test(columns[1]), 'an explicit id in the restore changes the counting argument; see ClientApplier.ignoreTables');
    });

    it('the RB-ANCHOR restore is the only replica-side SQL inserting into validator_rewards', function(){
        const writers = [];
        for(const file of codeFilesUnder(SRC)){
            const code = fs.readFileSync(file, 'utf8').split('\n')
                .filter(l => !/^\s*(\/\/|\*|\/\*)/.test(l)).join('\n');
            if(/\b(INSERT|REPLACE)\s+(IGNORE\s+)?INTO\s+validator_rewards\b/i.test(code)) writers.push(path.relative(SRC, file));
        }
        assert.deepStrictEqual(writers, [path.join('client', 'rollback.js')],
            'another statement inserts into validator_rewards; if it lets AUTO_INCREMENT assign the id, ' +
            'a later streamed reward can be dropped on the PRIMARY KEY, so carry the source id instead');
    });

    it('full snapshots rebase the validator_rewards allocator after commit', function(){
        const code = fs.readFileSync(APPLIER, 'utf8');
        assert.match(code, /POST_SNAPSHOT_AUTO_INCREMENT_TABLES\s*=\s*Object\.freeze\(\['validator_rewards'\]\)/);
        const commit = code.indexOf('commitTransaction()');
        const reset = code.indexOf('resetAutoIncrementToMax(table)');
        assert.ok(commit >= 0 && reset > commit,
            'allocator reset must remain after the full snapshot commit because ALTER TABLE implicitly commits');
    });

    it('the restore extractor fails on text that lacks the statement', function(){
        assert.strictEqual(restoreStatement('INSERT IGNORE INTO validator_rewards (amount) VALUES (?)'), null);
    });
});
