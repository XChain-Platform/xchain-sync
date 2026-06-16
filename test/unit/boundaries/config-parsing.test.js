// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const config = require('../../../src/config');

describe('Boundary: Config Parsing', function(){

    const ENV_KEYS = [
        'SYNC_MODE', 'SYNC_API_PORT', 'HUB_API_HOST', 'HUB_PORT',
        'CORS_ORIGIN', 'BLOCK_POLL_INTERVAL', 'WS_MAX_PER_IP',
        'SNAPSHOT_RATE_FULL', 'SNAPSHOT_RATE_INCR', 'SYNC_SOURCES',
        'VERIFY_HASHES', 'REPLICA_DB_HOST', 'REPLICA_DB_PORT',
        'REPLICA_DB_USER', 'REPLICA_DB_PASS'
    ];
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

    describe('zero values (falsy-zero)', function(){
        it('preserves SYNC_API_PORT=0', function(){
            process.env.SYNC_API_PORT = '0';
            assert.strictEqual(config.getConfig().SYNC_API_PORT, 0);
        });

        it('preserves BLOCK_POLL_INTERVAL=0', function(){
            process.env.BLOCK_POLL_INTERVAL = '0';
            assert.strictEqual(config.getConfig().BLOCK_POLL_INTERVAL, 0);
        });

        it('preserves SNAPSHOT_RATE_FULL=0', function(){
            process.env.SNAPSHOT_RATE_FULL = '0';
            assert.strictEqual(config.getConfig().SNAPSHOT_RATE_FULL, 0);
        });

        it('preserves SNAPSHOT_RATE_INCR=0', function(){
            process.env.SNAPSHOT_RATE_INCR = '0';
            assert.strictEqual(config.getConfig().SNAPSHOT_RATE_INCR, 0);
        });
    });

    describe('negative values clamped', function(){
        it('clamps WS_MAX_PER_IP=-1 to 1', function(){
            process.env.WS_MAX_PER_IP = '-1';
            assert.strictEqual(config.getConfig().WS_MAX_PER_IP, 1);
        });

        it('clamps HUB_PORT=-1 to 1', function(){
            process.env.HUB_PORT = '-1';
            assert.strictEqual(config.getConfig().HUB_PORT, 1);
        });

        it('clamps REPLICA_DB_PORT=-1 to 1', function(){
            process.env.REPLICA_DB_PORT = '-1';
            assert.strictEqual(config.getConfig().REPLICA_DB_PORT, 1);
        });

        it('clamps BLOCK_POLL_INTERVAL=-100 to 0', function(){
            process.env.BLOCK_POLL_INTERVAL = '-100';
            assert.strictEqual(config.getConfig().BLOCK_POLL_INTERVAL, 0);
        });

        it('clamps SNAPSHOT_RATE_FULL=-5 to 0', function(){
            process.env.SNAPSHOT_RATE_FULL = '-5';
            assert.strictEqual(config.getConfig().SNAPSHOT_RATE_FULL, 0);
        });
    });

    describe('NaN inputs default gracefully', function(){
        it('SYNC_API_PORT=abc defaults to 3006', function(){
            process.env.SYNC_API_PORT = 'abc';
            assert.strictEqual(config.getConfig().SYNC_API_PORT, 3006);
        });

        it('WS_MAX_PER_IP=xyz defaults to 100', function(){
            process.env.WS_MAX_PER_IP = 'xyz';
            assert.strictEqual(config.getConfig().WS_MAX_PER_IP, 100);
        });

        it('HUB_PORT="" defaults to 10000', function(){
            process.env.HUB_PORT = '';
            assert.strictEqual(config.getConfig().HUB_PORT, 10000);
        });

        it('BLOCK_POLL_INTERVAL=undefined defaults to 3000', function(){
            delete process.env.BLOCK_POLL_INTERVAL;
            assert.strictEqual(config.getConfig().BLOCK_POLL_INTERVAL, 3000);
        });
    });

    describe('float strings truncated', function(){
        it('WS_MAX_PER_IP=3.7 becomes 3', function(){
            process.env.WS_MAX_PER_IP = '3.7';
            assert.strictEqual(config.getConfig().WS_MAX_PER_IP, 3);
        });

        it('SYNC_API_PORT=3006.5 becomes 3006', function(){
            process.env.SYNC_API_PORT = '3006.5';
            assert.strictEqual(config.getConfig().SYNC_API_PORT, 3006);
        });
    });

    describe('large values', function(){
        it('SNAPSHOT_RATE_FULL=999999 preserved', function(){
            process.env.SNAPSHOT_RATE_FULL = '999999';
            assert.strictEqual(config.getConfig().SNAPSHOT_RATE_FULL, 999999);
        });

        it('BLOCK_POLL_INTERVAL=2147483647 preserved', function(){
            process.env.BLOCK_POLL_INTERVAL = '2147483647';
            assert.strictEqual(config.getConfig().BLOCK_POLL_INTERVAL, 2147483647);
        });
    });

    describe('VERIFY_HASHES case insensitivity', function(){
        it('"false" disables', function(){
            process.env.VERIFY_HASHES = 'false';
            assert.strictEqual(config.getConfig().VERIFY_HASHES, false);
        });

        it('"FALSE" disables', function(){
            process.env.VERIFY_HASHES = 'FALSE';
            assert.strictEqual(config.getConfig().VERIFY_HASHES, false);
        });

        it('"False" disables', function(){
            process.env.VERIFY_HASHES = 'False';
            assert.strictEqual(config.getConfig().VERIFY_HASHES, false);
        });

        it('"0" does NOT disable (not the word false)', function(){
            process.env.VERIFY_HASHES = '0';
            assert.strictEqual(config.getConfig().VERIFY_HASHES, true);
        });

        it('unset defaults to true', function(){
            delete process.env.VERIFY_HASHES;
            assert.strictEqual(config.getConfig().VERIFY_HASHES, true);
        });
    });

    describe('SYNC_SOURCES parsing boundaries', function(){
        it('empty string yields empty after getConfig', function(){
            process.env.SYNC_SOURCES = '';
            assert.strictEqual(config.getConfig().SYNC_SOURCES, '');
        });

        it('single URL preserved', function(){
            process.env.SYNC_SOURCES = 'http://server1';
            assert.strictEqual(config.getConfig().SYNC_SOURCES, 'http://server1');
        });

        it('trailing comma preserved in raw config (parsed by ClientSync)', function(){
            process.env.SYNC_SOURCES = 'http://s1,';
            assert.strictEqual(config.getConfig().SYNC_SOURCES, 'http://s1,');
        });

        it('whitespace preserved in raw config (parsed by ClientSync)', function(){
            process.env.SYNC_SOURCES = ' http://s1 , http://s2 ';
            assert.strictEqual(config.getConfig().SYNC_SOURCES, ' http://s1 , http://s2 ');
        });
    });

    describe('minimum-one clamping for ports', function(){
        it('WS_MAX_PER_IP=0 clamps to 1', function(){
            process.env.WS_MAX_PER_IP = '0';
            assert.strictEqual(config.getConfig().WS_MAX_PER_IP, 1);
        });

        it('WS_MAX_PER_IP=1 stays 1', function(){
            process.env.WS_MAX_PER_IP = '1';
            assert.strictEqual(config.getConfig().WS_MAX_PER_IP, 1);
        });

        it('HUB_PORT=0 clamps to 1', function(){
            process.env.HUB_PORT = '0';
            assert.strictEqual(config.getConfig().HUB_PORT, 1);
        });

        it('REPLICA_DB_PORT=0 clamps to 1', function(){
            process.env.REPLICA_DB_PORT = '0';
            assert.strictEqual(config.getConfig().REPLICA_DB_PORT, 1);
        });
    });
});
