'use strict';

const assert = require('node:assert');
const fs = require('node:fs');
const { execFileSync } = require('node:child_process');
const { resolveBase, selectFastTests } = require('../ci_fast_select');

function gitLines(args, allowNoMatch = false) {
    try {
        return execFileSync('git', args, { encoding: 'utf8' }).split(/\r?\n/).filter(Boolean);
    } catch (error) {
        if (allowNoMatch && error.status === 1) return [];
        throw error;
    }
}

const dependencies = {
    listTests: () => gitLines(['ls-files', 'test']),
    findRequirers: (term) => gitLines(['grep', '-l', '-F', '--', term, '--', 'src', 'test'], true),
};

function plan(changedFiles) {
    return selectFastTests(changedFiles, dependencies);
}

function files(result) {
    return result.tests.map((test) => test.file);
}

function guardedBlock(script, guard) {
    const lines = script.split(/\r?\n/);
    const start = lines.indexOf(guard);
    assert.notStrictEqual(start, -1);

    let depth = 0;
    for (let index = start; index < lines.length; index += 1) {
        const line = lines[index].trim();
        if (/^if\b.*; then$/.test(line)) depth += 1;
        if (line !== 'fi') continue;
        depth -= 1;
        if (depth === 0) return lines.slice(start, index + 1).join('\n');
    }
    assert.fail(`unclosed guard: ${guard}`);
}

describe('ci fast selector', function () {
    it('maps the CORS helper to its unit test without widening', function () {
        const result = plan(['src/http/cors_origin.js']);

        assert.strictEqual(result.consensus, false);
        assert.ok(files(result).includes('test/unit/cors_origin.test.js'));
    });

    it('maps a nested health helper to the same test directory', function () {
        const result = plan(['src/health/carrier_logic.js']);

        assert.strictEqual(result.consensus, false);
        assert.ok(files(result).includes('test/unit/health/carrier_logic.test.js'));
    });

    it('selects a changed test in a runner group directly', function () {
        const changed = 'test/unit/cors_origin.test.js';
        const result = plan([changed]);

        assert.strictEqual(result.consensus, false);
        assert.deepStrictEqual(result.tests, [{ group: 'unit', file: changed }]);
    });

    it('widens a source module required by a consensus path', function () {
        const result = plan(['src/db/balance_helpers.js']);

        assert.strictEqual(result.consensus, true);
        assert.ok(result.reasons.some((reason) => reason.includes('src/client/applier.js')));
    });

    for (const changed of ['src/consensus/gate_registry.js', 'src/client/sync.js']) {
        it(`widens a change to ${changed}`, function () {
            const result = plan([changed]);

            assert.strictEqual(result.consensus, true);
            assert.ok(result.reasons.some((reason) => reason.includes(changed)));
        });
    }

    it('selects no tests for a documentation-only change', function () {
        const result = plan(['README.md']);

        assert.strictEqual(result.consensus, false);
        assert.deepStrictEqual(result.tests, []);
    });

    it('widens a package manifest change', function () {
        const result = plan(['package.json']);

        assert.strictEqual(result.consensus, true);
        assert.ok(result.reasons.some((reason) => reason.includes('package.json')));
    });

    it('defers a tracked integration test outside the runner groups', function () {
        const integration = gitLines(['ls-files', 'test/integration']).find((file) => file.endsWith('.test.js'));
        assert.ok(integration);

        const result = plan([integration]);

        assert.strictEqual(result.consensus, false);
        assert.deepStrictEqual(result.tests, []);
        assert.ok(result.reasons.includes(`deferred: ${integration}`));
    });

    it('returns null when neither candidate base resolves', function () {
        const git = () => { throw new Error('unknown revision'); };

        assert.strictEqual(resolveBase({ env: { PROM_CI_BASE_SHA: 'unknown' }, git }), null);
    });

    it('uses an available pushed base without asking for a merge-base', function () {
        const sha = '0123456789abcdef';
        const calls = [];
        const git = (args) => {
            calls.push(args);
            if (args[0] !== 'cat-file') throw new Error('unexpected fallback');
            return '';
        };

        assert.strictEqual(resolveBase({ env: { PROM_CI_BASE_SHA: sha }, git }), sha);
        assert.deepStrictEqual(calls, [['cat-file', '-e', `${sha}^{commit}`]]);
    });

    it('keeps the fast selector guarded without removing the e2e tier', function () {
        const script = fs.readFileSync('bin/ci-full.sh', 'utf8');
        const invocation = 'node bin/ci_fast_select.js --plan';
        const guard = 'if [ "${CI_TIER:-full}" = "fast" ]; then';
        const selectorBlock = guardedBlock(script.slice(script.indexOf('FAST_SELECTOR_READY=0')), guard);

        assert.strictEqual(script.split(invocation).length - 1, 1);
        assert.ok(selectorBlock.includes(invocation));
        assert.ok(script.includes('run_tier "e2e: e2e tier (test:e2e:ci)"'));
    });
});
