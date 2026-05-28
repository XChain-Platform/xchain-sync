const assert = require('assert');
const config = require('../../src/config');

describe('config', function(){

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

    describe('defaults', function(){
        it('returns correct defaults when no env vars set', function(){
            let cfg = config.getConfig();
            assert.strictEqual(cfg.SYNC_MODE, 'server');
            assert.strictEqual(cfg.SYNC_API_PORT, 3006);
            assert.strictEqual(cfg.HUB_PORT, 10000);
            assert.strictEqual(cfg.CORS_ORIGIN, false);
            assert.strictEqual(cfg.BLOCK_POLL_INTERVAL, 3000);
            assert.strictEqual(cfg.WS_MAX_PER_IP, 3);
            assert.strictEqual(cfg.SNAPSHOT_RATE_FULL, 1);
            assert.strictEqual(cfg.SNAPSHOT_RATE_INCR, 10);
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
        it('includes WS_BACKPRESSURE_LIMIT', function(){
            assert.strictEqual(config.getConfig().WS_BACKPRESSURE_LIMIT, 50);
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
});
