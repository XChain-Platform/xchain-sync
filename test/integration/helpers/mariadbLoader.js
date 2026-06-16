// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Wrapper to load ESM-only mariadb package from CommonJS
// Caches the module after first load
let _mariadb = null;

async function getMariadb() {
    if (!_mariadb) {
        _mariadb = await import('mariadb');
        // The default export contains createPool, createConnection, etc.
        if (_mariadb.default) _mariadb = _mariadb.default;
    }
    return _mariadb;
}

module.exports = { getMariadb };
