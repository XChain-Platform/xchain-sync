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

function directConsensusReason(file, consensusPrefixes) {
    if (file === 'package.json') return `dependency surface: ${file}`;
    if (consensusPrefixes.some((prefix) => file.startsWith(prefix))) {
        return `consensus path: ${file}`;
    }
    if (WIDEN.some((prefix) => file.startsWith(prefix))) return `widen path: ${file}`;
    return null;
}

function resolvedRequiresFromSource(file, source) {
    const resolved = [];
    const requirePattern = /require\(\s*['"](\.[^'"]+)['"]\s*\)/g;
    for (const match of source.matchAll(requirePattern)) {
        const target = path.resolve(path.dirname(file), match[1]);
        resolved.push(target, `${target}.js`, path.join(target, 'index.js'));
    }
    return resolved.map((target) => path.relative(process.cwd(), target).split(path.sep).join('/'));
}

function resolvedRequires(file) {
    let source;
    try {
        source = fs.readFileSync(file, 'utf8');
    } catch (_) {
        return [];
    }
    return resolvedRequiresFromSource(file, source);
}

function importerReasons(changedFiles, findRequirers, consensusPrefixes) {
    const changedSrc = new Set(changedFiles.filter((file) => file.startsWith('src/')));
    const reasons = new Set();
    for (const changed of changedSrc) {
        const basename = path.basename(changed, path.extname(changed));
        for (const requirer of findRequirers(basename)) {
            if (!consensusPrefixes.some((prefix) => requirer.startsWith(prefix))) continue;
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

function selectFastTests(
    changedFiles,
    { listTests, findRequirers },
    { consensusPrefixes = CONSENSUS } = {},
) {
    const changed = [...new Set(changedFiles)].sort();
    const directReasons = changed
        .map((file) => directConsensusReason(file, consensusPrefixes))
        .filter(Boolean);
    const indirectReasons = importerReasons(changed, findRequirers, consensusPrefixes);
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

function selectionDependencies({ indexed = false } = {}) {
    if (!indexed) return makeDependencies();
    const tests = gitLines(['ls-files', 'test']);
    const files = gitLines(['ls-files', 'src', 'test']).filter((file) => fs.existsSync(file));
    const sources = new Map(files.map((file) => [file, fs.readFileSync(file, 'utf8')]));
    const cache = new Map();
    return {
        listTests: () => tests,
        findRequirers: (term) => {
            if (!cache.has(term)) {
                cache.set(term, [...sources]
                    .filter(([, source]) => source.includes(term))
                    .map(([file]) => file));
            }
            return cache.get(term);
        },
    };
}

function withoutConsensusPrefixes(prefixes) {
    const removed = new Set(prefixes.flatMap((prefix) => {
        const trimmed = prefix.trim();
        if (!trimmed) return [];
        return [trimmed, trimmed.endsWith('/') ? trimmed.slice(0, -1) : `${trimmed}/`];
    }));
    return CONSENSUS.filter((prefix) => !removed.has(prefix));
}

function changedFilesForCommit(commit) {
    const revision = gitLines(['rev-list', '--parents', '-n', '1', commit])[0];
    const [, parent] = revision.split(' ');
    if (parent) return gitLines(['diff', '--name-only', `${parent}..${commit}`]);
    return gitLines(['diff-tree', '--root', '--no-commit-id', '--name-only', '-r', commit]);
}

function emptyReplayCounts() {
    return { wholeUnit: 0, changedTests: 0, testOnly: 0, noTests: 0 };
}

function countReplayPlan(counts, changed, plan) {
    if (plan.consensus) {
        counts.wholeUnit++;
    } else if (plan.tests.length && changed.every((file) => file.startsWith('test/'))) {
        counts.testOnly++;
    } else if (plan.tests.length) {
        counts.changedTests++;
    } else {
        counts.noTests++;
    }
}

function replayPlans(limit, narrowPrefixes) {
    const commits = gitLines([
        'log', '--first-parent', '-n', String(limit), '--format=%H', 'origin/develop',
    ]);
    const current = emptyReplayCounts();
    const narrowed = emptyReplayCounts();
    const consensusPrefixes = withoutConsensusPrefixes(narrowPrefixes);
    const dependencies = selectionDependencies({ indexed: true });
    for (const commit of commits) {
        const changed = changedFilesForCommit(commit);
        countReplayPlan(current, changed, selectFastTests(changed, dependencies));
        countReplayPlan(narrowed, changed, selectFastTests(changed, dependencies, {
            consensusPrefixes,
        }));
    }
    return { commits, current, narrowed, consensusPrefixes, dependencies };
}

function fraction(value, total) {
    return `${value}/${total}`;
}

function printReplayRow(name, total, counts) {
    console.log([
        name,
        total,
        fraction(counts.wholeUnit, total),
        fraction(counts.changedTests, total),
        fraction(counts.testOnly, total),
        fraction(counts.noTests, total),
    ].join(' '));
}

function parseList(value) {
    return value.split(',').map((item) => item.trim()).filter(Boolean);
}

function parseMustSelect(value) {
    return parseList(value).map((pair) => {
        const separator = pair.indexOf(':');
        if (separator <= 0 || separator === pair.length - 1) {
            throw new Error(`invalid --must-select pair: ${pair}`);
        }
        return { source: pair.slice(0, separator), test: pair.slice(separator + 1) };
    });
}

function replayOptions(args) {
    const limit = Number(args[0]);
    if (!Number.isSafeInteger(limit) || limit < 1) {
        throw new Error('--replay requires a positive integer');
    }
    const options = { limit, narrowPrefixes: [], mustSelect: [] };
    for (let index = 1; index < args.length; index += 2) {
        const flag = args[index];
        const value = args[index + 1];
        if (!value || (flag !== '--narrow' && flag !== '--must-select')) {
            throw new Error(`invalid replay option: ${flag || ''}`.trim());
        }
        if (flag === '--narrow') options.narrowPrefixes.push(...parseList(value));
        else options.mustSelect.push(...parseMustSelect(value));
    }
    return options;
}

function runReplay(args) {
    try {
        const options = replayOptions(args);
        const result = replayPlans(options.limit, options.narrowPrefixes);
        console.log('plan commits consensus-1 changed-tests test-only no-tests');
        printReplayRow('current', result.commits.length, result.current);
        if (options.narrowPrefixes.length) {
            printReplayRow('narrowed', result.commits.length, result.narrowed);
        }
        let failed = false;
        for (const pair of options.mustSelect) {
            const plan = selectFastTests([pair.source], result.dependencies, {
                consensusPrefixes: result.consensusPrefixes,
            });
            const selected = plan.tests.some((test) => test.file === pair.test);
            console.log(`must-select ${selected ? 'PASS' : 'FAIL'} ${pair.source}:${pair.test}`);
            if (!selected) failed = true;
        }
        return failed ? 1 : 0;
    } catch (error) {
        console.error(`replay-error ${error.message}`);
        return 2;
    }
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
    const mode = process.argv[2];
    if (mode === '--replay') return runReplay(process.argv.slice(3));
    if (!['--plan', '--run'].includes(mode)) {
        console.error('usage: node bin/ci_fast_select.js --plan|--run|--replay N ' +
            '[--narrow prefix,...] [--must-select file:testfile,...]');
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
    if (mode === '--plan') printPlan(computed.plan);
    return mode === '--run' ? runPlan(computed.plan) : 0;
}

module.exports = { replayPlans, resolveBase, selectFastTests };

if (require.main === module) process.exitCode = main();
