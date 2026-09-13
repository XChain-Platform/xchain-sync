/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
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
 * Give a fake database the REAL query methods, without overriding its stubs.
 *
 * WHY THIS EXISTS. Many suites stand a database in with an object that has
 * doQuery and a handful of hand-stubbed methods. Once a query moves out of a
 * caller and into a named db method, every such fake that lacks the new name
 * throws, although nothing about the behaviour under test changed. Stubbing the
 * new names would make those fakes pass, but it would also stop the query
 * reaching doQuery, and several suites assert exactly what doQuery receives and
 * how many times it is called. So the real mixin methods are installed instead:
 * each one still hands its SQL to the fake's doQuery, and the suite sees what it
 * always saw.
 *
 * A name the fake already defines is left alone, so a suite that stubs a
 * method on purpose keeps its stub.
 *
 ********************************************************************/

'use strict';

const fs   = require('fs');
const path = require('path');

const DB_DIR = path.resolve(__dirname, '..', '..', 'src', 'db');

// Read from the registry the database class itself installs from, rather than
// restated here, so a mixin added to the class is available to every fake the
// moment it lands.
function mixinFiles() {
    const declared = fs.readFileSync(path.join(DB_DIR, 'index.js'), 'utf8');
    const block = /const MIXIN_FILES = \[([\s\S]*?)\];/.exec(declared);
    if (!block) throw new Error('src/db/index.js no longer declares MIXIN_FILES');
    return Array.from(block[1].matchAll(/(['"])([^'"]+)\1/g)).map((m) => path.join(DB_DIR, m[2]));
}

/**
 * Install every real query method onto `fake` that `fake` does not already have.
 *
 * @param {object} fake a stand-in database, typically carrying a doQuery stub
 * @returns {object} the same object, for use in a return statement
 */
function withDbMixins(fake) {
    for (const file of mixinFiles()) {
        const mixin = require(file);
        for (const name of Object.keys(mixin)) {
            if (!(name in fake)) fake[name] = mixin[name];
        }
    }
    return fake;
}

module.exports = { withDbMixins };
