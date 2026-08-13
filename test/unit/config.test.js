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
const config = require('../../src/config');

describe('config', function(){

    const ENV_KEYS = [
        'SYNC_MODE', 'SYNC_API_PORT', 'HUB_API_HOST', 'HUB_PORT',
        'CORS_ORIGIN', 'BLOCK_POLL_INTERVAL', 'WS_MAX_PER_IP',
        'SNAPSHOT_RATE_FULL', 'SNAPSHOT_RATE_INCR', 'SYNC_SOURCES',
        'VERIFY_HASHES', 'REPLICA_DB_HOST', 'REPLICA_DB_PORT',
        'REPLICA_DB_USER', 'REPLICA_DB_PASS', 'REPLICA_DB_READONLY', 'SYNC_EXCLUDE',
        'SYNC_META_RETENTION_BLOCKS',
        'SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET', 'SYNC_BOOTSTRAP_DEPTH_BTC_MAINNET',
        'SYNC_BOOTSTRAP_DEPTH_BADKEY', 'SYNC_BOOTSTRAP_DEPTH_LTC_TESTNET',
        'SYNC_BOOTSTRAP_DEPTH_DOGECOIN_TESTNET', 'SYNC_BOOTSTRAP_DEPTH_NOTACOIN_TESTNET'
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

    describe('defaults', function(){
        it('returns correct defaults when no env vars set', function(){
            let cfg = config.getConfig();
            assert.strictEqual(cfg.SYNC_MODE, 'server');
            assert.strictEqual(cfg.SYNC_API_PORT, 3006);
            assert.strictEqual(cfg.HUB_PORT, 10000);
            assert.strictEqual(cfg.CORS_ORIGIN, false);
            assert.strictEqual(cfg.BLOCK_POLL_INTERVAL, 3000);
            assert.strictEqual(cfg.WS_MAX_PER_IP, 100);
            assert.strictEqual(cfg.SNAPSHOT_RATE_FULL, 12);
            assert.strictEqual(cfg.SNAPSHOT_RATE_INCR, 600);
            assert.strictEqual(cfg.SYNC_SOURCES, '');
            assert.strictEqual(cfg.VERIFY_HASHES, true);
            assert.strictEqual(cfg.REPLICA_DB_PORT, 3306);
        });
    });

    describe('numeric coercion', function(){
        it('parses SYNC_API_PORT as integer', function(){
            process.env.SYNC_API_PORT = '4000';
            assert.strictEqual(config.getConfig().SYNC_API_PORT, 4000);
        });
        it('parses HUB_PORT as integer', function(){
            process.env.HUB_PORT = '9999';
            assert.strictEqual(config.getConfig().HUB_PORT, 9999);
        });
        it('parses BLOCK_POLL_INTERVAL as integer', function(){
            process.env.BLOCK_POLL_INTERVAL = '5000';
            assert.strictEqual(config.getConfig().BLOCK_POLL_INTERVAL, 5000);
        });
        it('falls back to default for non-numeric SYNC_API_PORT', function(){
            process.env.SYNC_API_PORT = 'abc';
            assert.strictEqual(config.getConfig().SYNC_API_PORT, 3006);
        });
    });

    describe('VERIFY_HASHES boolean', function(){
        it('returns false when set to "false"', function(){
            process.env.VERIFY_HASHES = 'false';
            assert.strictEqual(config.getConfig().VERIFY_HASHES, false);
        });
        it('returns false when set to "FALSE" (case-insensitive)', function(){
            process.env.VERIFY_HASHES = 'FALSE';
            assert.strictEqual(config.getConfig().VERIFY_HASHES, false);
        });
        it('returns false when set to "False"', function(){
            process.env.VERIFY_HASHES = 'False';
            assert.strictEqual(config.getConfig().VERIFY_HASHES, false);
        });
        it('returns true when set to "true"', function(){
            process.env.VERIFY_HASHES = 'true';
            assert.strictEqual(config.getConfig().VERIFY_HASHES, true);
        });
        it('returns true when not set', function(){
            assert.strictEqual(config.getConfig().VERIFY_HASHES, true);
        });
        it('returns true for any value other than "false"', function(){
            process.env.VERIFY_HASHES = '0';
            assert.strictEqual(config.getConfig().VERIFY_HASHES, true);
        });
    });

    describe('SYNC_MODE passthrough', function(){
        it('passes through the env value', function(){
            process.env.SYNC_MODE = 'client';
            assert.strictEqual(config.getConfig().SYNC_MODE, 'client');
        });
    });

    describe('hardcoded values', function(){
        it('includes HUB_REPOLL_INTERVAL', function(){
            assert.strictEqual(config.getConfig().HUB_REPOLL_INTERVAL, 300000);
        });
        // WS_BACKPRESSURE_LIMIT (count-based) is retired (item 5410); the byte/stall
        // pair below replaced it and the env var is ignored with a console notice.
        it('does not expose the retired WS_BACKPRESSURE_LIMIT', function(){
            assert.strictEqual(config.getConfig().WS_BACKPRESSURE_LIMIT, undefined);
        });
        it('includes WS_BACKPRESSURE_MAX_BYTES default', function(){
            assert.strictEqual(config.getConfig().WS_BACKPRESSURE_MAX_BYTES, 16777216);
        });
        it('includes WS_BACKPRESSURE_STALL_MS default', function(){
            assert.strictEqual(config.getConfig().WS_BACKPRESSURE_STALL_MS, 30000);
        });
        it('includes WS_STATUS_INTERVAL', function(){
            assert.strictEqual(config.getConfig().WS_STATUS_INTERVAL, 60000);
        });
        it('includes WS_PING_INTERVAL', function(){
            assert.strictEqual(config.getConfig().WS_PING_INTERVAL, 30000);
        });
        it('includes CLIENT_RECONNECT_DELAY', function(){
            assert.strictEqual(config.getConfig().CLIENT_RECONNECT_DELAY, 5000);
        });
        it('includes HASH_CONFIRM_TIMEOUT', function(){
            assert.strictEqual(config.getConfig().HASH_CONFIRM_TIMEOUT, 5000);
        });
    });

    describe('string passthrough', function(){
        it('passes HUB_API_HOST', function(){
            process.env.HUB_API_HOST = 'myhost';
            assert.strictEqual(config.getConfig().HUB_API_HOST, 'myhost');
        });
        it('passes REPLICA_DB_HOST', function(){
            process.env.REPLICA_DB_HOST = 'dbhost';
            assert.strictEqual(config.getConfig().REPLICA_DB_HOST, 'dbhost');
        });
    });

    describe('SYNC_EXCLUDE', function(){
        it('defaults to an empty array', function(){
            assert.deepStrictEqual(config.getConfig().SYNC_EXCLUDE, []);
        });
        it('parses, trims, and deduplicates a comma list', function(){
            process.env.SYNC_EXCLUDE = ' DOGE:testnet:indexer , LTC:mainnet:decoder ,DOGE:testnet:indexer';
            assert.deepStrictEqual(config.getConfig().SYNC_EXCLUDE,
                ['DOGE:testnet:indexer', 'LTC:mainnet:decoder']);
        });
        it('drops empty segments', function(){
            process.env.SYNC_EXCLUDE = ',,DOGE:testnet:indexer,,';
            assert.deepStrictEqual(config.getConfig().SYNC_EXCLUDE, ['DOGE:testnet:indexer']);
        });
    });

    describe('SYNC_BOOTSTRAP_DEPTH', function(){
        it('defaults to an empty map', function(){
            assert.deepStrictEqual(config.getConfig().SYNC_BOOTSTRAP_DEPTH, {});
        });
        it('parses SYNC_BOOTSTRAP_DEPTH_<CHAIN>_<NETWORK> into an uppercased CHAIN:NETWORK map', function(){
            process.env.SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET = '50000';
            process.env.SYNC_BOOTSTRAP_DEPTH_LTC_TESTNET = '1000';
            assert.deepStrictEqual(config.getConfig().SYNC_BOOTSTRAP_DEPTH,
                { 'DOGE:TESTNET': 50000, 'LTC:TESTNET': 1000 });
        });
        it('ignores non-positive / non-numeric depths', function(){
            process.env.SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET = '0';
            process.env.SYNC_BOOTSTRAP_DEPTH_BTC_MAINNET = 'abc';
            assert.deepStrictEqual(config.getConfig().SYNC_BOOTSTRAP_DEPTH, {});
        });
        it('ignores a malformed key with no CHAIN_NETWORK split', function(){
            process.env.SYNC_BOOTSTRAP_DEPTH_BADKEY = '500';
            assert.deepStrictEqual(config.getConfig().SYNC_BOOTSTRAP_DEPTH, {});
        });

        // The documented DOGE_TESTNET spelling produced 'DOGE:TESTNET' while ClientSync
        // looked up the hub's `cfg.coin` ('dogecoin'), so the documented key never matched
        // and the miss fell through to depth 0 (the FULL-snapshot branch). Both spellings
        // must now land on the ticker key ClientSync asks for.
        it('folds the full coin name onto the same ticker key as the ticker spelling', function(){
            process.env.SYNC_BOOTSTRAP_DEPTH_DOGECOIN_TESTNET = '50000';
            assert.deepStrictEqual(config.getConfig().SYNC_BOOTSTRAP_DEPTH, { 'DOGE:TESTNET': 50000 });
        });
        it('resolves the documented DOGE_TESTNET key to the key ClientSync looks up', function(){
            process.env.SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET = '50000';
            let cfg = config.getConfig();
            // exactly what ClientSync computes from the hub-supplied chain identifier
            assert.strictEqual(cfg.SYNC_BOOTSTRAP_DEPTH[config.bootstrapDepthKey('dogecoin', 'testnet')], 50000);
        });
        it('records every raw env key it saw, whatever the value', function(){
            process.env.SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET = '0';
            process.env.SYNC_BOOTSTRAP_DEPTH_BADKEY = '500';
            assert.deepStrictEqual(config.getConfig().SYNC_BOOTSTRAP_DEPTH_ENV_KEYS.sort(),
                ['SYNC_BOOTSTRAP_DEPTH_BADKEY', 'SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET']);
        });
    });

    // An unmatched depth key is NOT inert. The lookup falls through to 0, which is
    // the full-history snapshot branch, so a key that names no discovered chain
    // silently starts the unbounded bootstrap it was set to prevent.
    describe('assertBootstrapDepthChains', function(){
        const CHAINS = [
            { coin: 'dogecoin', network: 'testnet', dbType: 'indexer' },
            { coin: 'bitcoin',  network: 'mainnet', dbType: 'indexer' }
        ];

        it('accepts a key naming a discovered chain (ticker spelling)', function(){
            process.env.SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET = '50000';
            config.assertBootstrapDepthChains(config.getConfig(), CHAINS);
        });
        it('accepts a key naming a discovered chain (full-name spelling)', function(){
            process.env.SYNC_BOOTSTRAP_DEPTH_DOGECOIN_TESTNET = '50000';
            config.assertBootstrapDepthChains(config.getConfig(), CHAINS);
        });
        it('accepts a config with no depth keys at all', function(){
            config.assertBootstrapDepthChains(config.getConfig(), CHAINS);
        });
        it('REFUSES a key whose chain was never discovered', function(){
            process.env.SYNC_BOOTSTRAP_DEPTH_LTC_TESTNET = '50000';
            assert.throws(
                () => config.assertBootstrapDepthChains(config.getConfig(), CHAINS),
                /SYNC_BOOTSTRAP_DEPTH_LTC_TESTNET.*LTC:TESTNET/s
            );
        });
        it('REFUSES a key whose network was never discovered', function(){
            process.env.SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET = '50000';
            assert.throws(
                () => config.assertBootstrapDepthChains(config.getConfig(),
                    [{ coin: 'dogecoin', network: 'mainnet', dbType: 'indexer' }]),
                /SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET/
            );
        });
        it('REFUSES an unknown coin rather than defaulting it to depth 0', function(){
            process.env.SYNC_BOOTSTRAP_DEPTH_NOTACOIN_TESTNET = '50000';
            assert.throws(
                () => config.assertBootstrapDepthChains(config.getConfig(), CHAINS),
                /SYNC_BOOTSTRAP_DEPTH_NOTACOIN_TESTNET/
            );
        });
        it('REFUSES a malformed key with no CHAIN_NETWORK split', function(){
            process.env.SYNC_BOOTSTRAP_DEPTH_BADKEY = '500';
            assert.throws(
                () => config.assertBootstrapDepthChains(config.getConfig(), CHAINS),
                /SYNC_BOOTSTRAP_DEPTH_BADKEY.*not CHAIN_NETWORK shaped/s
            );
        });
        it('REFUSES a depth-0 key naming no discovered chain (0 is the full-snapshot branch)', function(){
            process.env.SYNC_BOOTSTRAP_DEPTH_LTC_TESTNET = '0';
            assert.throws(
                () => config.assertBootstrapDepthChains(config.getConfig(), CHAINS),
                /SYNC_BOOTSTRAP_DEPTH_LTC_TESTNET/
            );
        });
    });

    // Opt-in only: a tier that can write must never be silently downgraded to
    // serve-only by a stray value, and a read_only replica must not be flipped
    // writable by one either.
    describe('REPLICA_DB_READONLY', function(){
        it('defaults to false when unset', function(){
            assert.strictEqual(config.getConfig().REPLICA_DB_READONLY, false);
        });
        it('is true for "1"', function(){
            process.env.REPLICA_DB_READONLY = '1';
            assert.strictEqual(config.getConfig().REPLICA_DB_READONLY, true);
        });
        it('is true for "true" in any case', function(){
            process.env.REPLICA_DB_READONLY = 'TRUE';
            assert.strictEqual(config.getConfig().REPLICA_DB_READONLY, true);
        });
        it('is false for "false", "0" and an empty value', function(){
            for(let value of ['false', '0', '']){
                process.env.REPLICA_DB_READONLY = value;
                assert.strictEqual(config.getConfig().REPLICA_DB_READONLY, false, 'value: ' + value);
            }
        });
    });

    // Transparency-log retention is opt-in and default-off: unset means keep full
    // history, so every historical inclusion proof stays serveable. A garbage value
    // must read as off, never as an accidental prune window.
    describe('SYNC_META_RETENTION_BLOCKS', function(){
        it('defaults to 0 (retention disabled) when unset', function(){
            assert.strictEqual(config.getConfig().SYNC_META_RETENTION_BLOCKS, 0);
        });
        it('reads a positive window', function(){
            process.env.SYNC_META_RETENTION_BLOCKS = '50000';
            assert.strictEqual(config.getConfig().SYNC_META_RETENTION_BLOCKS, 50000);
        });
        it('is 0 for a non-numeric, empty or negative value', function(){
            for(let value of ['', 'lots', '-1']){
                process.env.SYNC_META_RETENTION_BLOCKS = value;
                assert.strictEqual(config.getConfig().SYNC_META_RETENTION_BLOCKS, 0, 'value: ' + value);
            }
        });
    });
});
