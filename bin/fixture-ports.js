#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

const REPO = path.join(__dirname, '..');
const SPECS = Object.freeze({
    'test/e2e/docker-compose.e2e.yml': Object.freeze({
        E2E_DB_PORT: 23306,
        E2E_REPLICA_DB_PORT: 23307,
        E2E_SOURCE2_DB_PORT: 23308
    }),
    'test/chaos/fixtures/docker-compose.chaos.yml': Object.freeze({
        SOURCE_DIRECT_PORT: 33065,
        TOXIPROXY_PORT: 8474,
        SOURCE_PROXY_PORT: 33060,
        REPLICA_PROXY_PORT: 33061,
        WS_PROXY_PORT: 33062
    })
});

function integer(value, label) {
    const text = String(value);
    if (!/^(0|[1-9][0-9]*)$/.test(text)) {
        throw new Error(`${label} must be a non-negative integer, got ${JSON.stringify(text)}`);
    }
    return Number(text);
}

function offset(env) {
    const source = env || process.env;
    return source.CI_PORT_OFFSET === undefined || source.CI_PORT_OFFSET === ''
        ? 0
        : integer(source.CI_PORT_OFFSET, 'CI_PORT_OFFSET');
}

function basePort(name) {
    for (const spec of Object.values(SPECS)) {
        if (Object.prototype.hasOwnProperty.call(spec, name)) return spec[name];
    }
    throw new Error(`unknown fixture port ${name}`);
}

function port(name, env) {
    const source = env || process.env;
    const value = source[name] === undefined || source[name] === ''
        ? basePort(name) + offset(source)
        : integer(source[name], name);
    if (value < 1 || value > 65535) throw new Error(`${name} resolves outside the TCP port range: ${value}`);
    return value;
}

function composeEnvironment(file, env) {
    const spec = SPECS[file];
    if (!spec) throw new Error(`unknown fixture compose file ${file}`);
    const result = { ...(env || process.env) };
    for (const name of Object.keys(spec)) result[name] = String(port(name, result));
    return result;
}

function render(file, env) {
    const spec = SPECS[file];
    if (!spec) throw new Error(`unknown fixture compose file ${file}`);
    let source = fs.readFileSync(path.join(REPO, file), 'utf8');
    for (const [name, base] of Object.entries(spec)) {
        source = source.split(`\${${name}:-${base}}`).join(String(port(name, env)));
    }
    return source;
}

function compose(file, args, env) {
    const result = spawnSync('docker', ['compose', '-f', file, ...args], {
        cwd: REPO,
        env: composeEnvironment(file, env),
        stdio: 'inherit'
    });
    if (result.error) throw result.error;
    return result.status === null ? 1 : result.status;
}

function main(argv) {
    const [command, name, ...args] = argv;
    if (command === 'render' && name && args.length === 0) {
        process.stdout.write(render(name));
        return 0;
    }
    if (command === 'port' && name && args.length === 0) {
        process.stdout.write(String(port(name)) + '\n');
        return 0;
    }
    if (command === 'compose' && name && args.length > 0) return compose(name, args);
    throw new Error('usage: fixture-ports.js <render FILE|port NAME|compose FILE ARGS...>');
}

if (require.main === module) {
    try {
        process.exitCode = main(process.argv.slice(2));
    } catch (err) {
        console.error(err.message);
        process.exitCode = 1;
    }
}

module.exports = { SPECS, offset, port, composeEnvironment, render };
