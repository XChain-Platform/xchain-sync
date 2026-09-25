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

function registerFlushAndHealthTestsPart1({ expect, patchConsole, unpatchConsole }) {
    it('routes console.* through the shim, mapping log to info', function () {
        const handle = patchConsole({ service: 'xchain-sync', env: {} });
        const seen = [];
        // Read the shipper's own output by swapping its sink after the patch,
        // which is the only place the bound originals are reachable from.
        handle.logger.console = { log: (m) => seen.push(m), warn: (m) => seen.push(m), error: (m) => seen.push(m) };
        console.log('plain');
        console.warn('careful');
        console.error('bad');
        unpatchConsole();
        expect(seen[0]).to.match(/ info \[xchain-sync\] plain$/);
        expect(seen[1]).to.match(/ warn \[xchain-sync\] careful$/);
        expect(seen[2]).to.match(/ error \[xchain-sync\] bad$/);
    });

    it('resolves printf format strings and keeps an Error stack, via util.format', function () {
        const handle = patchConsole({ service: 'svc', env: {} });
        const seen = [];
        handle.logger.console = { log: (m) => seen.push(m), warn: (m) => seen.push(m), error: (m) => seen.push(m) };
        console.log('round %d of %s', 7, 'oracle');
        console.error('crashed:', new Error('kaboom'));
        unpatchConsole();
        expect(seen[0]).to.include('round 7 of oracle');
        expect(seen[1]).to.include('kaboom');
        expect(seen[1]).to.include('Error');
    });

    it('does not recurse: the sink holds bound originals captured BEFORE the patch', function () {
        // `const orig = console` would hand the logger the very object about to
        // be replaced, so every line would re-enter the wrapper forever. The
        // proof is simply that a line completes and arrives once.
        const realLog = console.log;
        let depth = 0;
        let maxDepth = 0;
        console.log = (...a) => { depth += 1; maxDepth = Math.max(maxDepth, depth); depth -= 1; return realLog.apply(console, a); };
        const captured = console.log;
        try {
            patchConsole({ service: 'svc', env: {} });
            expect(console.log).to.not.equal(captured);
            console.log('one line');
            unpatchConsole();
        } finally {
            console.log = realLog;
        }
        expect(maxDepth).to.equal(1);
    });

}

function registerFlushAndHealthTestsPart2({ expect, patchConsole, unpatchConsole, getLogger, createLogShipper, fakeConsole }) {
    it('no-ops under XCHAIN_LOG_PATCH=0 so test bootstraps see stock console', function () {
        const before = console.log;
        const handle = patchConsole({ service: 'svc', env: { XCHAIN_LOG_PATCH: '0' } });
        expect(handle.patched).to.equal(false);
        expect(console.log).to.equal(before);
    });

    it('is idempotent and restores the exact original functions on unpatch', function () {
        const before = { log: console.log, warn: console.warn, error: console.error };
        const first = patchConsole({ service: 'svc', env: {} });
        const second = patchConsole({ service: 'other', env: {} });
        expect(second).to.equal(first);
        unpatchConsole();
        expect(console.log).to.equal(before.log);
        expect(console.warn).to.equal(before.warn);
        expect(console.error).to.equal(before.error);
    });

    it('getLogger works before any install and reaches the real shipper after', function () {
        // A module that logs while being required must not be able to crash the
        // process just because it loaded before the wiring.
        const log = getLogger();
        expect(() => log.info('early', { a: 1 })).to.not.throw();
        const handle = patchConsole({ service: 'svc', env: {} });
        const seen = [];
        handle.logger.console = { log: (m) => seen.push(m), warn: (m) => seen.push(m), error: (m) => seen.push(m) };
        log.warn('LATE_EVENT', { reason: 'x' });
        unpatchConsole();
        expect(seen[0]).to.match(/ warn \[svc\] LATE_EVENT reason=x$/);
    });

    // A trailing Error argument expands across lines under util.inspect, and only
    // the first line carries the prefix. Measured on the live fleet as orphaned
    // fragments like "  fatal: true," from a pretty-printed mariadb SqlError:
    // no operation, no error, no coin, and unparseable by anything keying on the
    // prefix. One console call must be one line.
    it('renders a multi-line message as ONE line with the breaks escaped', () => {
        const sink = fakeConsole();
        const log = createLogShipper({ service: 'xchain-sync', env: {}, console: sink });
        log.error('DB write failed: SqlError: connect ECONNREFUSED\n  fatal: true,\n  errno: -111');
        expect(sink.lines.error).to.have.lengthOf(1);
        expect(sink.lines.error[0]).to.not.match(/\n/);
        expect(sink.lines.error[0]).to.include('\\n  fatal: true,');
        expect(sink.lines.error[0]).to.match(/^\d{4}-\d{2}-\d{2}T[\d:.]+Z error \[xchain-sync\] DB write failed:/);
    });

}

function registerFlushAndHealthTestsPart3({ expect, patchConsole, unpatchConsole, getRegistry, createLogShipper, installObservability, fakeConsole }) {
    it('escapes a bare carriage return too, so a progress writer cannot split a record', () => {
        const sink = fakeConsole();
        const log = createLogShipper({ service: 'xchain-sync', env: {}, console: sink });
        log.warn('rewriting\rline');
        expect(sink.lines.warn).to.have.lengthOf(1);
        expect(sink.lines.warn[0]).to.include('rewriting\\nline');
    });

    // JSON mode needs no escaping of its own: JSON.stringify already emits one
    // physical line and keeps the true characters, which is better fidelity for a
    // machine reader than a lossy substitution would be.
    it('JSON mode keeps the real newlines and still emits one physical line', () => {
        const sink = fakeConsole();
        const log = createLogShipper({ service: 'xchain-sync', env: { LOG_FORMAT: 'json' }, console: sink });
        log.error('line one\nline two');
        expect(sink.lines.error).to.have.lengthOf(1);
        expect(sink.lines.error[0]).to.not.match(/\n/);
        expect(JSON.parse(sink.lines.error[0]).msg).to.equal('line one\nline two');
    });

    it('does not double-format: a shipper built AFTER the patch writes to the pre-patch sink', function () {
        // The shim's default sink is the global console by reference. A shipper
        // taking that default once console is patched emits its formatted line
        // INTO the wrapper and gets it formatted again, so the line reads
        // `<ts> warn [svc] <ts> warn [svc] msg`. Caught by driving the real hub
        // suite, not by reading the diff.
        const seen = [];
        const realWarn = console.warn;
        console.warn = (m) => seen.push(m);
        try {
            patchConsole({ service: 'svc', env: {} });
            // A custom transport means this handle does NOT adopt the process
            // shipper, so it builds a second one: the path where the global
            // console would otherwise be taken as the default sink.
            const second = installObservability(null, { service: 'svc', env: {}, logTransport: () => Promise.resolve() });
            second.logger.warn('once only');
        } finally {
            unpatchConsole();
            console.warn = realWarn;
        }
        expect(seen).to.have.lengthOf(1);
        expect(seen[0]).to.match(/^\S+Z warn \[svc\] once only$/);
    });

    it('hands out one registry, always constructed, before any install call', function () {
        const reg = getRegistry({ service: 'svc' });
        expect(reg).to.equal(getRegistry());
        const c = reg.counter({ name: 'xchain_probe_total', help: 'probe' });
        c.inc({}, 1);
        expect(reg.render()).to.include('xchain_probe_total');
    });
}

function registerFlushAndHealthTests(context) {
    registerFlushAndHealthTestsPart1(context);
    registerFlushAndHealthTestsPart2(context);
    registerFlushAndHealthTestsPart3(context);
}

module.exports = { registerFlushAndHealthTests };
