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
 *
 * Deployed-process armed-map fingerprint.
 *
 * Services install from release tarballs with no .git and stale package
 * versions, so nothing a process reports says WHICH consensus-gate build it
 * actually runs. Before a flag-day height the operator must confirm every
 * deployed process carries the same armed map or a straggler forks or halts at
 * the boundary (this exact mode halted LTC:testnet at 4805000 on 2026-07-10).
 * Hashing the byte content of the running build's gate files, and exposing that
 * on the health endpoint, lets a fleet sweep compare one string per process
 * instead of copying files out of containers.
 *
 * Per-file sha256 hashes are exposed too, because gate files that are twins
 * across repos can then be cross-compared service-to-service, while the
 * combined fingerprint only compares like-for-like processes of the SAME
 * service. This module is itself a twin in xchain-indexer (FIXED_GATE_FILES
 * simply matches whichever carriers exist in each repo's src/); keep them in step.
 *
 ********************************************************************/

const fs     = require('fs');
const path   = require('path');
const crypto = require('crypto');

// Every *_activation.js in this directory (the per-gate arming twins) plus the
// fixed gate carriers. Enumerated at call time so a gate file added later can
// never be silently excluded by a stale hand-list.
const FIXED_GATE_FILES = ['protocol_changes.js', 'stateHash.js'];

let cached = null;

function computeArmedMapFingerprint(){
    if(cached) return cached;
    const dir = __dirname;
    const names = fs.readdirSync(dir)
        .filter(f => f.endsWith('_activation.js') || FIXED_GATE_FILES.includes(f))
        .sort();
    const files = {};
    const outer = crypto.createHash('sha256');
    for(const name of names){
        let fileHash;
        try {
            fileHash = crypto.createHash('sha256').update(fs.readFileSync(path.join(dir, name))).digest('hex');
        } catch(e){
            // A listed-but-unreadable gate file must poison the fingerprint
            // visibly rather than silently matching a healthy process.
            fileHash = 'UNREADABLE';
        }
        files[name] = fileHash;
        outer.update(name).update('\0').update(fileHash).update('\0');
    }
    cached = { fingerprint: outer.digest('hex'), files: files };
    return cached;
}

module.exports = { computeArmedMapFingerprint };
