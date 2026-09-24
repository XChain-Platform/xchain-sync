// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers environment hooks. One part of config_parsing.test.js.
const ENV_KEYS = [
    'SYNC_MODE', 'SYNC_API_PORT', 'HUB_API_HOST', 'HUB_PORT',
    'CORS_ORIGIN', 'BLOCK_POLL_INTERVAL', 'WS_MAX_PER_IP',
    'SNAPSHOT_RATE_FULL', 'SNAPSHOT_RATE_INCR', 'SYNC_SOURCES',
    'VERIFY_HASHES', 'REPLICA_DB_HOST', 'REPLICA_DB_PORT',
    'REPLICA_DB_USER', 'REPLICA_DB_PASS'
];

function registerHooks(){
    let savedEnv = {};
    beforeEach(function(){
        for(let key of ENV_KEYS){
            savedEnv[key] = process.env[key];
            delete process.env[key];
        }
    });
    afterEach(function(){
        for(let key of ENV_KEYS){
            if(savedEnv[key] !== undefined)
                process.env[key] = savedEnv[key];
            else
                delete process.env[key];
        }
    });
}

module.exports = { registerHooks };
