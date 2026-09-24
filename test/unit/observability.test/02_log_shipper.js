'use strict';

// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

function registerLogShipperTestsPart1({ expect, createLogShipper, readLogEnv, redactFields, scrubMessage, REDACTED, fakeConsole }) {
    it('is inert by default: text output, no buffering, no shipping', function () {
        const sink = fakeConsole();
        const log = createLogShipper({ service: 'svc', env: {}, console: sink });
        log.info('hello world');
        expect(log.config.shipEnabled).to.equal(false);
        expect(log.buffer.length).to.equal(0);
        expect(log.timer).to.equal(null);
        expect(sink.lines.log).to.have.lengthOf(1);
        expect(sink.lines.log[0]).to.match(/^\S+Z info \[svc\] hello world$/);
    });

    it('emits NDJSON with the envelope keys when LOG_FORMAT=json', function () {
        const sink = fakeConsole();
        const log = createLogShipper({ service: 'xchain-sync', version: '9.9.9', env: { LOG_FORMAT: 'json' }, console: sink });
        log.warn('block stalled', { height: 42 });
        const rec = JSON.parse(sink.lines.warn[0]);
        expect(rec.level).to.equal('warn');
        expect(rec.service).to.equal('xchain-sync');
        expect(rec.msg).to.equal('block stalled');
        expect(rec.height).to.equal(42);
        expect(rec.version).to.equal('9.9.9');
        expect(new Date(rec.ts).toISOString()).to.equal(rec.ts);
    });

    it('honours LOG_LEVEL and drops quieter levels', function () {
        const sink = fakeConsole();
        const log = createLogShipper({ service: 'svc', env: { LOG_LEVEL: 'warn' }, console: sink });
        expect(log.info('quiet')).to.equal(null);
        expect(log.error('loud')).to.not.equal(null);
        expect(sink.lines.log.length).to.equal(0);
    });

    it('redacts credential-shaped field keys and inline key=value pairs', function () {
        const sink = fakeConsole();
        const log = createLogShipper({ service: 'svc', env: { LOG_FORMAT: 'json' }, console: sink });
        const rec = log.info('connect password=hunter2 then api_key: abc123', {
            db: { user: 'app', password: 'hunter2' },
            HUB_API_KEY: 'zzz',
            height: 7
        });
        expect(rec.db.password).to.equal(REDACTED);
        expect(rec.db.user).to.equal('app');
        expect(rec.HUB_API_KEY).to.equal(REDACTED);
        expect(rec.height).to.equal(7);
        expect(rec.msg).to.not.include('hunter2');
        expect(rec.msg).to.not.include('abc123');
        expect(JSON.stringify(rec)).to.not.include('hunter2');
    });

}

function registerLogShipperTestsPart2({
    expect, createLogShipper, readLogEnv, redactFields, scrubMessage, REDACTED, fakeConsole
}) {
    it('never lets a caller field forge the record envelope', function () {
        const log = createLogShipper({ service: 'real-svc', env: {}, console: fakeConsole() });
        const rec = log.info('m', { service: 'spoofed', level: 'debug', msg: 'spoofed' });
        expect(rec.service).to.equal('real-svc');
        expect(rec.level).to.equal('info');
        expect(rec.msg).to.equal('m');
    });

    it('handles cyclic and deep field graphs without throwing', function () {
        const a = { name: 'a' };
        a.self = a;
        expect(redactFields(a).self).to.equal('[circular]');
        expect(redactFields({ a: { b: { c: { d: { e: 1 } } } } }).a.b.c.d).to.equal('[truncated]');
        expect(scrubMessage('token=abc')).to.equal(`token=${REDACTED}`);
    });

    it('serializes an Error field with a scrubbed message', function () {
        const out = redactFields({ err: new Error('login failed for password=hunter2') });
        expect(out.err.message).to.include(REDACTED);
        expect(out.err.message).to.not.include('hunter2');
    });

    it('requires BOTH the flag and a valid URL before shipping', function () {
        expect(readLogEnv({ LOG_SHIP_ENABLED: '1' }).shipEnabled).to.equal(false);
        expect(readLogEnv({ LOG_SHIP_URL: 'https://c/logs' }).shipEnabled).to.equal(false);
        expect(readLogEnv({ LOG_SHIP_ENABLED: '1', LOG_SHIP_URL: 'ftp://c/logs' }).shipEnabled).to.equal(false);
        expect(readLogEnv({ LOG_SHIP_ENABLED: 'true', LOG_SHIP_URL: 'https://c/logs' }).shipEnabled).to.equal(true);
    });

    it('batches NDJSON to the transport once the batch size is reached', async function () {
        const bodies = [];
        const log = createLogShipper({
            service: 'svc',
            env: { LOG_SHIP_ENABLED: '1', LOG_SHIP_URL: 'https://collector.invalid/logs', LOG_SHIP_BATCH_SIZE: '2' },
            console: fakeConsole(),
            transport: (body) => { bodies.push(body); return Promise.resolve(); }
        });
        log.info('one');
        log.info('two');
        await new Promise((r) => setImmediate(r));
        await log.stop();

        expect(bodies.length).to.equal(1);
        const lines = bodies[0].trim().split('\n').map((l) => JSON.parse(l));
        expect(lines.map((l) => l.msg)).to.deep.equal(['one', 'two']);
        expect(log.stats.shipped).to.equal(2);
    });

}

function registerLogShipperTestsPart3({ expect, Registry, createLogShipper, fakeConsole }) {
    it('drops the oldest lines when the buffer is full and counts the loss', function () {
        const log = createLogShipper({
            service: 'svc',
            env: {
                LOG_SHIP_ENABLED: '1', LOG_SHIP_URL: 'https://collector.invalid/logs',
                LOG_SHIP_BATCH_SIZE: '1000', LOG_SHIP_MAX_BUFFER: '3'
            },
            console: fakeConsole(),
            transport: () => new Promise(() => {})   // never settles: buffer fills
        });
        for (let i = 0; i < 6; i++) log.info(`line-${i}`);
        expect(log.buffer.length).to.equal(3);
        expect(log.stats.dropped).to.equal(3);
        expect(log.buffer.map((r) => r.msg)).to.deep.equal(['line-3', 'line-4', 'line-5']);
    });

    it('survives a failing collector, re-queues the batch and rate-limits the stderr note', async function () {
        const sink = fakeConsole();
        const log = createLogShipper({
            service: 'svc',
            env: { LOG_SHIP_ENABLED: '1', LOG_SHIP_URL: 'https://collector.invalid/logs', LOG_SHIP_BATCH_SIZE: '1' },
            console: sink,
            transport: () => Promise.reject(new Error('ECONNREFUSED'))
        });
        log.info('a');
        await log.flush();
        log.info('b');
        await log.flush();

        expect(log.stats.failures).to.be.greaterThan(0);
        expect(log.stats.shipped).to.equal(0);
        expect(log.buffer.length).to.be.greaterThan(0);
        // One note per minute, so the second failure adds no line.
        expect(sink.lines.error.filter((l) => l.includes('[log-ship]')).length).to.equal(1);
        await log.stop();
    });

    it('exposes shipper counters on a registry when one is supplied', function () {
        const reg = new Registry();
        const log = createLogShipper({ service: 'svc', env: {}, console: fakeConsole(), registry: reg });
        log.info('x');
        log.error('y');
        const out = reg.render();
        expect(out).to.include('log_lines_emitted_total{level="info"} 1');
        expect(out).to.include('log_lines_emitted_total{level="error"} 1');
        expect(out).to.include('log_ship_buffer_lines 0');
    });

}

function registerLogShipperTestsPart4({ expect, http, createLogShipper, fakeConsole }) {
    it('stop() clears the flush timer so the process can exit', async function () {
        const log = createLogShipper({
            service: 'svc',
            env: { LOG_SHIP_ENABLED: '1', LOG_SHIP_URL: 'https://collector.invalid/logs' },
            console: fakeConsole(),
            transport: () => Promise.resolve()
        });
        expect(log.timer).to.not.equal(null);
        await log.stop();
        expect(log.timer).to.equal(null);
    });

    // Exercises the real postBatch/fetch path. Every other test here injects a
    // transport, which is why the unreleased response body below went unseen.
    it('releases the response body so a stalled collector cannot pin the socket', async function () {
        this.timeout(5000);
        let closed = false;
        const sockets = new Set();
        const server = http.createServer((req, res) => {
            req.resume();
            // Answer with headers and a first chunk, then never end the body.
            req.on('end', () => { res.writeHead(200); res.write('ack'); });
            res.socket.on('close', () => { closed = true; });
        });
        server.on('connection', (s) => { sockets.add(s); s.on('close', () => sockets.delete(s)); });
        await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
        const { port } = server.address();

        const log = createLogShipper({
            service: 'svc',
            env: {
                LOG_SHIP_ENABLED: '1',
                LOG_SHIP_URL: `http://127.0.0.1:${port}/logs`,
                LOG_SHIP_BATCH_SIZE: '1',
                LOG_SHIP_TIMEOUT_MS: '400'
            },
            console: fakeConsole()
        });

        try {
            log.info('one');
            await log.flush();
            // fetch() resolves on headers and the abort timer is cleared with it,
            // so an unreleased body leaves nothing that will ever close this
            // socket. Measured: released in under 2ms, unreleased still open at 3s.
            for (let i = 0; i < 100 && !closed; i++) {
                await new Promise((resolve) => setTimeout(resolve, 10));
            }
            expect(closed).to.equal(true);
        } finally {
            await log.stop();
            for (const s of sockets) s.destroy();
            await new Promise((resolve) => server.close(resolve));
        }
    });
}

function registerLogShipperTests(context) {
    registerLogShipperTestsPart1(context);
    registerLogShipperTestsPart2(context);
    registerLogShipperTestsPart3(context);
    registerLogShipperTestsPart4(context);
}

function registerTextFormatTestsPart1({ expect, createLogShipper, REDACTED, fakeConsole }) {
    it('renders ts, lowercase level, service tag, message, then key=value', function () {
        const sink = fakeConsole();
        const log = createLogShipper({ service: 'xchain-sync', env: {}, console: sink });
        log.warn('PBFT_DROP', { reason: 'digest_mismatch', phase: 'prepare', round: 42 });
        expect(sink.lines.warn).to.have.lengthOf(1);
        expect(sink.lines.warn[0]).to.match(
            /^\d{4}-\d{2}-\d{2}T[\d:.]+Z warn \[xchain-sync\] PBFT_DROP reason=digest_mismatch phase=prepare round=42$/
        );
    });

    it('keeps the level token lowercase so the server-monitor ERROR|FATAL grep does not match it', function () {
        const sink = fakeConsole();
        const log = createLogShipper({ service: 'svc', env: {}, console: sink });
        log.error('boom');
        // collect-snapshot.sh counts `grep -cE 'ERROR|FATAL'`. An uppercase
        // token would make every console.error line count and trip the crit
        // threshold fleet-wide on first deploy.
        expect(sink.lines.error[0]).to.not.match(/ERROR|FATAL/);
        expect(sink.lines.error[0]).to.include(' error [svc] boom');
    });

    it('puts the message immediately after the service tag so existing substring greps still match', function () {
        const sink = fakeConsole();
        const log = createLogShipper({ service: 'xchain-sync', env: {}, console: sink });
        log.info('Oracle: Round 12 finalized');
        expect(sink.lines.log[0]).to.include('Oracle: Round 12 finalized');
    });

    it('quotes a value carrying whitespace, = or a quote, and leaves plain tokens bare', function () {
        const sink = fakeConsole();
        const log = createLogShipper({ service: 'svc', env: {}, console: sink });
        log.info('m', { plain: 'abc', spaced: 'a b', eq: 'k=v', num: 3, flag: true, nil: null });
        const line = sink.lines.log[0];
        expect(line).to.include('plain=abc');
        expect(line).to.include('spaced="a b"');
        expect(line).to.include('eq="k=v"');
        expect(line).to.include('num=3');
        expect(line).to.include('flag=true');
        expect(line).to.include('nil=null');
    });

}

function registerTextFormatTestsPart2({ expect, createLogShipper, REDACTED, fakeConsole }) {
    it('redacts a credential-shaped field and an inline credential in the message', function () {
        const sink = fakeConsole();
        const log = createLogShipper({ service: 'svc', env: {}, console: sink });
        log.warn('connect failed password=hunter2', { db_password: 'hunter2', host: 'db1' });
        const line = sink.lines.warn[0];
        expect(line).to.not.include('hunter2');
        expect(line).to.include(REDACTED);
        expect(line).to.include('host=db1');
    });

    it('emits one NDJSON record per line under LOG_FORMAT=json', function () {
        const sink = fakeConsole();
        const log = createLogShipper({ service: 'svc', env: { LOG_FORMAT: 'json' }, console: sink });
        log.info('hello', { a: 1 });
        const parsed = JSON.parse(sink.lines.log[0]);
        expect(parsed).to.include({ level: 'info', service: 'svc', msg: 'hello', a: 1 });
        expect(parsed.ts).to.be.a('string');
    });

    it('silences info under LOG_LEVEL=warn while still emitting warn', function () {
        const sink = fakeConsole();
        const log = createLogShipper({ service: 'svc', env: { LOG_LEVEL: 'warn' }, console: sink });
        log.info('quiet');
        log.warn('loud');
        expect(sink.lines.log).to.have.lengthOf(0);
        expect(sink.lines.warn).to.have.lengthOf(1);
    });
}

function registerTextFormatTests(context) {
    registerTextFormatTestsPart1(context);
    registerTextFormatTestsPart2(context);
}

function registerMessageRedactionTests({ expect, scrubMessage, REDACTED }) {
    // An env-validation failure prints the variable NAME and its value, and the
    // names the services use are all prefixed (HUB_DB_SECRET, INDEXER_DB_PASS,
    // db_password). A `\b`-anchored key never matches those, because `_` is a
    // word character and `\b` does not fire between two word characters. With
    // LOG_SHIP_* configured, an unscrubbed line goes off-box in the clear.
    const leaky = [
        ['prefixed env secret',   'Missing required environment variable: HUB_DB_SECRET=hunter2swordfish'],
        ['prefixed db pass',      'connect failed db_password=hunter2swordfish'],
        ['screaming env pass',    'INDEXER_DB_PASS=hunter2swordfish'],
        ['api key',               'HUB_API_KEY=hunter2swordfish'],
        ['keyed bearer',          'Authorization: Bearer eyJhbGciOi.SECRETPAYLOAD.sig'],
        ['bare bearer',           'sending Bearer eyJhbGciOi.SECRETPAYLOAD.sig upstream'],
        ['quoted mnemonic',       'mnemonic="correct horse battery staple"'],
    ];
    for (const [name, line] of leaky) {
        it(`scrubs a ${name}`, function () {
            const out = scrubMessage(line);
            expect(out).to.not.match(/hunter2swordfish|SECRETPAYLOAD|correct horse/);
            expect(out).to.include(REDACTED);
        });
    }

    it('redacts the token, not the word Bearer', function () {
        // The value group would otherwise capture "Bearer" and stop, leaving the
        // token itself in the clear immediately after a [redacted] marker that
        // makes the line look handled.
        const out = scrubMessage('Authorization: Bearer eyJhbGciOi.SECRETPAYLOAD.sig');
        expect(out).to.not.include('SECRETPAYLOAD');
    });

    it('leaves real operational lines untouched, hex identifiers included', function () {
        // Hub and indexer lines are full of legitimate 64-char hex (txids, block
        // hashes, state roots). A hex sweep here would gut the logs this work
        // exists to make readable.
        const keep = [
            'Oracle: Round 12 finalized with 4 of 5 votes',
            'StateAnchorPublisher: anchored bundle regtest @ 100 (txid a3f9bc21de)',
            'P2P: Invalid signature from xc1qexampleaddr; dropping message',
            'seed block=5 imported',
            'PBFT_DROP reason=digest_mismatch phase=prepare round=42'
        ];
        for (const line of keep) expect(scrubMessage(line)).to.equal(line);
    });
}

module.exports = {
    registerLogShipperTests,
    registerTextFormatTests,
    registerMessageRedactionTests
};
