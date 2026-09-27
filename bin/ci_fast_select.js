#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const GROUPS = [
    { name: 'unit', pattern: /^test\/unit\/.*\.test\.js$/ },
    { name: 'security', pattern: /^test\/security\/.*\.test\.js$/ },
];
const GROUP_ARGS = [
    '--require', './test/setup/index.js', '--timeout', '10000', '--recursive', '--exit',
];
const CONSENSUS = [
    'src/consensus/',
    'src/consensus-constants.js',
    'src/checkpoint.js',
    'src/merkle.js',
    'src/merkle/',
    'src/table_lifecycle.js',
    'src/table_lifecycle/',
    'src/state_commitment/',
    'src/contract_state_subtree.js',
    'src/escrow_leaf_subtree.js',
    'src/coins/',
    'src/client/',
    'src/sql/',
    'src/schema/',
    'bin/pins/',
    'bin/lib/',
    'bin/pin-identity.js',
];
const WIDEN = ['test/setup/', 'test/helpers/', 'test/fixtures/'];
const ALWAYS = [];

function resolveBase({ env, git }) {
    const promised = env.PROM_CI_BASE_SHA;
    if (promised) {
        let promisedExists = false;
        try {
            git(['cat-file', '-e', `${promised}^{commit}`]);
            promisedExists = true;
        } catch (_) {
            promisedExists = false;
        }
        if (promisedExists) return promised;
    }
    try {
        const base = String(git(['merge-base', 'HEAD', 'origin/develop'])).trim();
        return base || null;
    } catch (_) {
        return null;
    }
}

function groupFor(file) {
    const group = GROUPS.find((candidate) => candidate.pattern.test(file));
    return group && group.name;
}

function directConsensusReason(file) {
    if (file === 'package.json') return `dependency surface: ${file}`;
    if (CONSENSUS.some((prefix) => file.startsWith(prefix))) return `consensus path: ${file}`;
    if (WIDEN.some((prefix) => file.startsWith(prefix))) return `widen path: ${file}`;
    return null;
}

function resolvedRequires(file) {
    let source;
    try {
        source = fs.readFileSync(file, 'utf8');
    } catch (_) {
        return [];
    }
    const resolved = [];
    const requirePattern = /require\(\s*['"](\.[^'"]+)['"]\s*\)/g;
    for (const match of source.matchAll(requirePattern)) {
        const target = path.resolve(path.dirname(file), match[1]);
        resolved.push(target, `${target}.js`, path.join(target, 'index.js'));
    }
    return resolved.map((target) => path.relative(process.cwd(), target).split(path.sep).join('/'));
}

function importerReasons(changedFiles, findRequirers) {
    const changedSrc = new Set(changedFiles.filter((file) => file.startsWith('src/')));
    const reasons = new Set();
    for (const changed of changedSrc) {
        const basename = path.basename(changed, path.extname(changed));
        for (const requirer of findRequirers(basename)) {
            if (!CONSENSUS.some((prefix) => requirer.startsWith(prefix))) continue;
            if (resolvedRequires(requirer).some((target) => changedSrc.has(target))) {
                reasons.add(`consensus importer: ${requirer}`);
            }
        }
    }
    return [...reasons];
}

function moduleTests(sourceFile, tests, findRequirers) {
    if (!sourceFile.startsWith('src/')) return [];
    const relative = sourceFile.slice(4).replace(/\.js$/, '');
    const basename = path.basename(relative);
    const relativeDir = path.dirname(relative) === '.' ? '' : path.dirname(relative);
    const named = basename === 'index' ? null : `${basename}.test.js`;
    const requiring = new Set(findRequirers(`src/${relative}`));
    return tests.filter((test) => {
        const group = groupFor(test);
        if (!group) return false;
        const tierRoot = `test/${group}`;
        const sameDir = relativeDir ? `${tierRoot}/${relativeDir}` : tierRoot;
        return (named && path.basename(test) === named)
            || (named && test.includes(`/${basename}.test/`))
            || path.dirname(test) === sameDir
            || requiring.has(test);
    });
}

function selectedTestRecords(files) {
    return [...new Set(files)].sort().map((file) => ({ group: groupFor(file), file }));
}

function selectFastTests(changedFiles, { listTests, findRequirers }) {
    const changed = [...new Set(changedFiles)].sort();
    const directReasons = changed.map(directConsensusReason).filter(Boolean);
    const indirectReasons = importerReasons(changed, findRequirers);
    const reasons = [...new Set([...directReasons, ...indirectReasons])].sort();
    if (reasons.length) return { consensus: true, reasons, tests: selectedTestRecords(ALWAYS) };

    const tests = listTests().filter((file) => groupFor(file) && fs.existsSync(file));
    const existing = new Set(tests);
    const selected = ALWAYS.filter((file) => existing.has(file));
    for (const file of changed) {
        const group = groupFor(file);
        if (group && existing.has(file)) selected.push(file);
        else if (file.startsWith('test/')) reasons.push(`deferred: ${file}`);
        selected.push(...moduleTests(file, tests, findRequirers));
    }
    return { consensus: false, reasons: [...new Set(reasons)].sort(), tests: selectedTestRecords(selected) };
}

function git(args) {
    const result = spawnSync('git', args, { encoding: 'utf8' });
    if (result.status === 0) return result.stdout;
    const error = new Error((result.stderr || `git ${args[0]} failed`).trim());
    error.status = result.status;
    throw error;
}

function gitLines(args, noMatchIsEmpty = false) {
    try {
        return git(args).split(/\r?\n/).filter(Boolean);
    } catch (error) {
        if (noMatchIsEmpty && error.status === 1) return [];
        throw error;
    }
}

function makeDependencies() {
    return {
        listTests: () => gitLines(['ls-files', 'test']),
        findRequirers: (term) => gitLines(['grep', '-l', '-F', '--', term, '--', 'src', 'test'], true),
    };
}

function computePlan() {
    const base = resolveBase({ env: process.env, git });
    if (!base) return { error: 'no valid push base and no merge-base with origin/develop' };
    const changed = gitLines(['diff', '--name-only', `${base}...HEAD`]);
    return { plan: selectFastTests(changed, makeDependencies()) };
}

function printPlan(plan) {
    console.log(`consensus ${plan.consensus ? 1 : 0}`);
    for (const reason of plan.reasons) console.log(`reason ${reason}`);
    for (const test of plan.tests) console.log(`test ${test.group} ${test.file}`);
}

function runPlan(plan) {
    if (!plan.tests.length) {
        console.log('ci:fast: no test maps to this push');
        return 0;
    }
    let failed = false;
    for (const group of GROUPS) {
        const files = plan.tests.filter((test) => test.group === group.name).map((test) => test.file);
        if (!files.length) continue;
        const result = spawnSync('./node_modules/.bin/mocha', ['--no-config', ...GROUP_ARGS, ...files], {
            stdio: 'inherit',
        });
        if (result.status !== 0) failed = true;
    }
    return failed ? 1 : 0;
}

function main() {
    if (!['--plan', '--run'].includes(process.argv[2])) {
        console.error('usage: node bin/ci_fast_select.js --plan|--run');
        return 2;
    }
    let computed;
    try {
        computed = computePlan();
    } catch (error) {
        console.log(`no-base ${error.message}`);
        return 3;
    }
    if (computed.error) {
        console.log(`no-base ${computed.error}`);
        return 3;
    }
    if (process.argv[2] === '--plan') printPlan(computed.plan);
    return process.argv[2] === '--run' ? runPlan(computed.plan) : 0;
}

module.exports = { resolveBase, selectFastTests };

if (require.main === module) process.exitCode = main();
