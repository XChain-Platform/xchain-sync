#!/usr/bin/env node
/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 **********************************************************************
 *
 * Put back the comment runs a cleanup pass deleted, above the code they
 * described, in the file where that code lives today.
 *
 * WHY THIS IS NOT `git revert`. The passes this recovers from deleted
 * explanation AND rewrote surviving lines to take internal references out.
 * Reverting restores the second along with the first, which reintroduces text
 * this repo may not publish, and it discards every comment written since. So
 * this works run by run: it takes only the runs that no longer exist in any
 * form, drops a run carrying an internal reference whole, and places each one
 * above the code it sat above, found in today's file rather than at yesterday's
 * line number.
 *
 * WHERE EACH FILE WAS. The input is a list of files with the path each one had
 * when its comments were deleted, the path it has now, and the commit to read
 * the deleted text from. Resolving renames is the coverage checker's job and it
 * already does it exactly, so this reads its answer rather than guessing from
 * a basename, which is how a restore ends up writing prose into the wrong file.
 *
 * THE ANCHOR IS THE CODE, NOT THE LINE NUMBER. A run is placed only where the
 * first three lines of code beneath it appear exactly once in today's file. A
 * run whose code has changed, or matches twice, is reported for a human.
 *
 * WHAT IS NOT PUT BACK, even when it was deleted. Decorative dividers, step
 * banners and narration that restates the next line are the style rules' cut
 * list, and restoring them would re-add noise the rules exist to remove. A run
 * that reads as history ("used to", "previously") is placed but REPORTED, so a
 * human rewrites it as a present-tense explanation that keeps its substance.
 *
 * THE ONE GUARANTEE. Not one byte of executable code changes. Each file is
 * checked with every comment and blank line stripped, before and after, and a
 * file that would differ is left exactly as it was.
 *
 * USAGE
 *   node bin/restore_comments.js --short <list.json>            report only
 *   node bin/restore_comments.js --short <list.json> --write    place them
 *
 *   The list is an array of { currentPath, histPath, restoreFrom }.
 *
 ********************************************************************/

'use strict';

const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

const REPO_ROOT = path.resolve(__dirname, '..');

// Files this repo carries but does not author. A consensus carrier is hashed by
// name and bytes, so a restored comment moves the armed-map fingerprint; a twin
// is compared with its canonical copy; a vendored file is refreshed by a script.
// Their deleted lines are the canonical repo's to put back, never this one's.
const FROZEN = [
    /^src\/coins\//, /^src\/observability\//,
    /^src\/[a-z_]+_activation\.js$/,
    /^src\/(stateHash|equivocation_header|stake_weighted_quorum|consensus-constants)\.js$/,
    /^src\/(merkle|contractStateSubtree|escrowLeafSubtree|tableLifecycle|stateCommitment|checkpoint|armedMapFingerprint)\.js$/,
    /^test\/unit\/(stateSubtreeActivation|contractStateSubtree|escrowLeafSubtree)\.test\.js$/,
];

// Text that may never go back into a public repo. The shapes are described
// rather than spelled out: a tool that lists the private names it refuses has
// published them.
const INTERNAL_REFERENCE = [
    /(?:^|[\s(])#\d{3,5}\b/,              // a review id
    /\/(?:Users|home)\//,                  // an operator's home directory
    /\b[a-z]+\/(?:bin|reports|specs)\//,   // a path into a private tooling tree
];
// A tracker id is letters, a dash and a number, which is also the shape of open
// standards a comment is right to cite, so those prefixes are let through.
const PUBLIC_PREFIXES = new Set(['SHA', 'BIP', 'RFC', 'UTF', 'ISO', 'AES', 'ECMA', 'EIP', 'SLIP', 'CVE', 'GHSA', 'SMT', 'SPV', 'UTXO', 'WS', 'HTTP', 'API', 'JSON']);
const TRACKER_ID = /\b([A-Z]{2,5})-\d{1,5}\b/g;

// The style rules' cut list: noise, not explanation.
const CUT = [
    /^[-=_*#~\s]{4,}$/,                                    // a bare divider
    /^(?:[-=_*#~]{3,}\s*.{0,40}\s*[-=_*#~]{3,})$/,         // a titled divider
    /^step\s+\d+\b/i,                                      // a step banner
    /^(?:now\s+we|next,?\s+we|then\s+we|here\s+we)\b/i,    // narration
];

// Markers that a run tells a story about the past rather than explaining now.
const HISTORY = /\b(?:used to|previously|formerly|no longer|at first|originally|this session|before this (?:change|fix|commit)|was changed|we changed|we used)\b/i;

const COMMENT_LINE = /^\s*(?:\/\/|\*|\/\*)/;

// A deleted run that shares this much vocabulary with a comment still in the file
// was REWRITTEN, not removed. Putting it back verbatim would sit a near-copy
// beside its own rewrite, so it is left to the rewrite pass, which merges the
// lost words into the surviving run instead. The same floor that pass pairs at.
const REWRITE_FLOOR = 0.3;

function tokens(text) {
    return new Set(String(text).toLowerCase().match(/[a-z0-9_]{3,}/g) || []);
}

function jaccard(a, b) {
    if (!a.size && !b.size) return 1;
    let shared = 0;
    for (const t of a) if (b.has(t)) shared += 1;
    return shared / (a.size + b.size - shared);
}

function git(args) {
    return execFileSync('git', args, { cwd: REPO_ROOT, encoding: 'utf8', maxBuffer: 256 * 1024 * 1024 });
}

/** Contiguous runs of comment lines, each with the window of code beneath it. */
function commentRuns(lines) {
    const runs = [];
    let cur = null;
    for (let i = 0; i < lines.length; i += 1) {
        if (COMMENT_LINE.test(lines[i])) {
            if (!cur) cur = { start: i, lines: [] };
            cur.lines.push(lines[i]);
            continue;
        }
        if (cur) {
            let j = i;
            while (j < lines.length && !lines[j].trim()) j += 1;
            // Three lines of code, not one: `});` is the commonest line in a
            // suite and anchors nothing on its own.
            cur.window = [];
            for (let k = j; k < lines.length && cur.window.length < 3; k += 1) {
                if (!lines[k].trim() || COMMENT_LINE.test(lines[k])) continue;
                cur.window.push(lines[k].trim());
            }
            runs.push(cur);
            cur = null;
        }
    }
    if (cur) { cur.window = []; runs.push(cur); }
    return runs;
}

/** A `*` continuation fragment cannot stand alone; a whole block or `//` run can. */
function isSelfContained(run) {
    const body = run.lines.map((l) => l.trim());
    if (body.every((l) => l.startsWith('//'))) return true;
    return body[0].startsWith('/*') && body[body.length - 1].endsWith('*/');
}

/** The file with every comment and blank line removed: what must not change. */
function codeOnly(text) {
    const out = [];
    let inBlock = false;
    for (const raw of text.split('\n')) {
        let line = raw;
        if (inBlock) {
            const end = line.indexOf('*/');
            if (end === -1) continue;
            line = line.slice(end + 2);
            inBlock = false;
        }
        const trimmed = line.trim();
        if (trimmed.startsWith('//')) continue;
        if (trimmed.startsWith('/*')) {
            if (trimmed.includes('*/')) {
                const after = trimmed.slice(trimmed.indexOf('*/') + 2);
                if (after.trim()) out.push(after.trim());
                continue;
            }
            inBlock = true;
            continue;
        }
        if (trimmed.startsWith('*') && !trimmed.startsWith('*/')) continue;
        if (!trimmed) continue;
        out.push(trimmed);
    }
    return out.join('\n');
}

/** A comment line with its marker stripped, for comparison and for the filters. */
function bodyOf(line) {
    return line.replace(/^\s*(?:\/\/+|\*+|\/\*+)\s?/, '').replace(/\*\/\s*$/, '').trim();
}

function carriesInternalReference(line) {
    if (INTERNAL_REFERENCE.some((re) => re.test(line))) return true;
    for (const m of String(line).matchAll(TRACKER_ID)) if (!PUBLIC_PREFIXES.has(m[1])) return true;
    return false;
}

function isCut(run) {
    const bodies = run.lines.map(bodyOf).filter(Boolean);
    if (!bodies.length) return false;
    if (bodies.every((b) => CUT.some((re) => re.test(b)))) return true;
    // A TITLED banner: a label between two rule lines. Checking every line alone
    // misses it, because the label itself is ordinary text, and that is exactly
    // how a restore puts back a section banner the cleanup was right to remove.
    const rule = CUT[0];
    const first = bodies[0];
    const last = bodies[bodies.length - 1];
    return bodies.length >= 3 && rule.test(first) && rule.test(last);
}

/** What one file lost, and where each run goes back. */
function analyse(entry) {
    const target = entry.currentPath;
    const abs = path.join(REPO_ROOT, target);
    if (!fs.existsSync(abs)) return { target, skipped: 'no such file today' };
    if (FROZEN.some((re) => re.test(target))) return { target, skipped: 'frozen: its lines are the canonical repo\'s to restore' };

    let oldText;
    try { oldText = git(['show', `${entry.restoreFrom}:${entry.histPath}`]); } catch (e) {
        return { target, skipped: `no such file at ${String(entry.restoreFrom).slice(0, 8)}` };
    }
    const nowLines = fs.readFileSync(abs, 'utf8').split('\n');
    const nowBodies = new Set();
    const nowRunTokens = [];
    for (const r of commentRuns(nowLines)) {
        for (const l of r.lines) nowBodies.add(bodyOf(l));
        nowRunTokens.push(tokens(r.lines.map(bodyOf).join(' ')));
    }

    const placed = [];
    const unplaced = [];
    for (const run of commentRuns(oldText.split('\n'))) {
        const bodies = run.lines.map(bodyOf).filter(Boolean);
        if (!bodies.length) continue;
        // Any surviving line means the run was REWRITTEN, not removed: that is
        // for the rewrite merge to fold in, never a duplicate to append here.
        if (bodies.some((b) => nowBodies.has(b))) continue;
        const runTokens = tokens(bodies.join(' '));
        if (nowRunTokens.some((t) => jaccard(runTokens, t) >= REWRITE_FLOOR)) {
            unplaced.push({ run: run.lines, why: 'rewritten rather than removed: the rewrite pass merges it' });
            continue;
        }
        if (isCut(run)) { unplaced.push({ run: run.lines, why: 'cut list: a divider, banner or narration' }); continue; }
        if (!isSelfContained(run)) { unplaced.push({ run: run.lines, why: 'a block fragment whose opener or closer is elsewhere' }); continue; }
        if (run.lines.some(carriesInternalReference)) { unplaced.push({ run: run.lines, why: 'carries an internal reference' }); continue; }
        if (!run.window.length) { unplaced.push({ run: run.lines, why: 'no code beneath it' }); continue; }

        let at = [];
        for (let width = run.window.length; width >= 1; width -= 1) {
            const want = run.window.slice(0, width);
            at = [];
            for (let i = 0; i < nowLines.length; i += 1) {
                if (nowLines[i].trim() !== want[0]) continue;
                let k = i;
                let ok = true;
                for (const w of want) {
                    while (k < nowLines.length && (!nowLines[k].trim() || COMMENT_LINE.test(nowLines[k]))) k += 1;
                    if (k >= nowLines.length || nowLines[k].trim() !== w) { ok = false; break; }
                    k += 1;
                }
                if (ok) at.push(i);
            }
            if (at.length === 1) break;
        }
        if (at.length !== 1) {
            unplaced.push({ run: run.lines, why: at.length ? `its code appears ${at.length} times` : 'its code is gone' });
            continue;
        }
        placed.push({ at: at[0], run: run.lines, history: run.lines.some((l) => HISTORY.test(bodyOf(l))) });
    }
    return { target, placed, unplaced };
}

function main() {
    const argv = process.argv.slice(2);
    const arg = (n) => { const i = argv.indexOf(n); return i === -1 ? null : argv[i + 1]; };
    const listPath = arg('--short');
    if (!listPath) { console.error('usage: restore_comments.js --short <list.json> [--write]'); process.exitCode = 2; return; }
    const write = argv.includes('--write');
    const list = JSON.parse(fs.readFileSync(path.resolve(listPath), 'utf8'));

    const totals = { files: 0, placed: 0, unplaced: 0, history: 0, refused: 0 };
    const report = [];
    for (const entry of list) {
        const res = analyse(entry);
        if (res.skipped) { report.push({ file: res.target, skipped: res.skipped }); continue; }
        const lines = res.placed.reduce((a, p) => a + p.run.length, 0);
        totals.unplaced += res.unplaced.reduce((a, u) => a + u.run.length, 0);
        const history = res.placed.filter((p) => p.history);
        totals.history += history.length;
        report.push({ file: res.target, placedLines: lines, unplaced: res.unplaced.map((u) => ({ why: u.why, lines: u.run.length, first: bodyOf(u.run[0]).slice(0, 90) })), historyRuns: history.map((h) => bodyOf(h.run[0]).slice(0, 90)) });
        if (!res.placed.length) continue;
        totals.files += 1;
        if (!write) { totals.placed += lines; continue; }

        const abs = path.join(REPO_ROOT, res.target);
        const before = fs.readFileSync(abs, 'utf8');
        const out = before.split('\n');
        // Bottom up, so an earlier insertion does not move a later anchor.
        for (const p of res.placed.slice().sort((a, b) => b.at - a.at)) {
            const indent = (out[p.at].match(/^\s*/) || [''])[0];
            // Re-base the run onto the code's indentation but keep its own shape,
            // so a JSDoc block keeps the one space that aligns each `*` under the
            // opener's, rather than every line being flattened to the same column.
            const base = Math.min(...p.run.filter((l) => l.trim()).map((l) => (l.match(/^\s*/) || [''])[0].length));
            out.splice(p.at, 0, ...p.run.map((l) => indent + l.slice(Math.min(base, (l.match(/^\s*/) || [''])[0].length))));
        }
        const after = out.join('\n');
        if (codeOnly(before) !== codeOnly(after)) {
            totals.refused += 1;
            report.push({ file: res.target, refused: 'the insertion would have changed executable code' });
            continue;
        }
        fs.writeFileSync(abs, after);
        totals.placed += lines;
    }
    process.stdout.write(`${JSON.stringify({ totals, report }, null, 1)}\n`);
}

if (require.main === module) main();

module.exports = { commentRuns, isSelfContained, codeOnly, bodyOf, carriesInternalReference, isCut, analyse };
