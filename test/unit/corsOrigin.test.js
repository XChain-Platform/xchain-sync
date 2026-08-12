// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// CORS_ORIGIN is an ALLOWLIST, and the load-bearing assertions are the ones that
// drive the real `cors` middleware rather than reading parseCorsOrigin's return
// value.
//
// The defect this guards against is not a crash, it is a header that LOOKS
// configured: handed a String, `cors` echoes it verbatim to every caller, so
// `CORS_ORIGIN="a,b"` answers `Access-Control-Allow-Origin: a,b` to a, to b, and
// to a hostile origin alike. No browser accepts a multi-value ACAO, so every
// listed shell is blocked while `curl -D -` shows a populated header.
//
// sync differs from its five sibling services in WHERE the parse happens: api.js
// only ever reads cfg, so the fix lives at the config seam and these tests drive
// getConfig() rather than process.env directly. A test that called
// parseCorsOrigin itself would still pass if config.js went back to the raw var.
// Twin of the encoder, hub, indexer, tracker and sdk suites .

const assert  = require('assert');
const express = require('express');
const cors    = require('cors');
const config  = require('../../src/config');
const { parseCorsOrigin } = require('../../src/corsOrigin');

// Resolve CORS_ORIGIN the way the service does: env -> getConfig() -> cors().
// api.js also pins `methods`, which is reproduced so the mount matches.
async function acaoFor(rawEnv, origins){
    const saved = process.env.CORS_ORIGIN;
    if(rawEnv === undefined) delete process.env.CORS_ORIGIN;
    else process.env.CORS_ORIGIN = rawEnv;

    let cfg;
    try { cfg = config.getConfig(); }
    finally {
        if(saved !== undefined) process.env.CORS_ORIGIN = saved;
        else delete process.env.CORS_ORIGIN;
    }

    const app = express();
    app.use(cors({ origin: cfg['CORS_ORIGIN'], methods: ['GET', 'POST'] }));
    app.get('/probe', (req, res) => res.json({ ok: true }));

    const server = await new Promise(resolve => {
        const s = app.listen(0, () => resolve(s));
    });
    try {
        const port = server.address().port;
        const out = {};
        for(const origin of origins){
            const res = await fetch(`http://127.0.0.1:${port}/probe`, { headers: { Origin: origin } });
            out[origin] = res.headers.get('access-control-allow-origin');
        }
        return out;
    } finally {
        await new Promise(resolve => server.close(resolve));
    }
}

const IOS      = 'capacitor://localhost';
const ANDROID  = 'https://localhost';
const EXPLORER = 'https://explorer.xchain.io';
const HOSTILE  = 'https://evil.example';

describe('CORS_ORIGIN allowlist parsing', function(){

    describe('parseCorsOrigin', function(){

        it('disables CORS when the var is unset, empty, blank, or only separators', function(){
            assert.strictEqual(parseCorsOrigin(undefined), false);
            assert.strictEqual(parseCorsOrigin(null), false);
            assert.strictEqual(parseCorsOrigin(''), false);
            assert.strictEqual(parseCorsOrigin('   '), false);
            assert.strictEqual(parseCorsOrigin(',,'), false);
            assert.strictEqual(parseCorsOrigin(' , , '), false);
        });

        it('passes a lone value straight through, so `*` and a single origin behave as before', function(){
            assert.strictEqual(parseCorsOrigin('*'), '*');
            assert.strictEqual(parseCorsOrigin(EXPLORER), EXPLORER);
            assert.strictEqual(parseCorsOrigin(`  ${EXPLORER}  `), EXPLORER);
        });

        it('splits a comma-separated value into an array and trims each entry', function(){
            assert.deepStrictEqual(parseCorsOrigin(`${IOS},${ANDROID},${EXPLORER}`), [IOS, ANDROID, EXPLORER]);
            assert.deepStrictEqual(parseCorsOrigin(` ${IOS} , ${ANDROID} `), [IOS, ANDROID]);
            assert.deepStrictEqual(parseCorsOrigin(`${IOS},,${EXPLORER}`), [IOS, EXPLORER]);
        });
    });

    // The seam that actually ships: config.js must hand cors a parsed value.
    describe('getConfig() resolves CORS_ORIGIN, not the raw env string', function(){

        it('keeps the documented unset default of false (CORS disabled)', function(){
            const saved = process.env.CORS_ORIGIN;
            delete process.env.CORS_ORIGIN;
            try { assert.strictEqual(config.getConfig()['CORS_ORIGIN'], false); }
            finally { if(saved !== undefined) process.env.CORS_ORIGIN = saved; }
        });

        it('turns a comma-separated value into an array before it can reach cors', function(){
            const saved = process.env.CORS_ORIGIN;
            process.env.CORS_ORIGIN = `${IOS},${EXPLORER}`;
            try { assert.deepStrictEqual(config.getConfig()['CORS_ORIGIN'], [IOS, EXPLORER]); }
            finally {
                if(saved !== undefined) process.env.CORS_ORIGIN = saved;
                else delete process.env.CORS_ORIGIN;
            }
        });
    });

    describe('what a caller actually receives', function(){

        it('sends no ACAO at all when CORS is disabled, the sync default', async function(){
            const acao = await acaoFor(undefined, [IOS, EXPLORER, HOSTILE]);
            assert.strictEqual(acao[IOS], null);
            assert.strictEqual(acao[EXPLORER], null);
            assert.strictEqual(acao[HOSTILE], null);
        });

        it('sends `*` to everyone when CORS_ORIGIN is `*`', async function(){
            const acao = await acaoFor('*', [IOS, EXPLORER, HOSTILE]);
            assert.strictEqual(acao[IOS], '*');
            assert.strictEqual(acao[EXPLORER], '*');
            assert.strictEqual(acao[HOSTILE], '*');
        });

        // Measured, not assumed: given a String, `cors` does no matching at all -
        // it names that origin to every caller, and the BROWSER is what refuses a
        // mismatch. That is safe for one origin and is exactly why a comma list is
        // not: the same unconditional echo produces a header nobody can accept.
        it('names the single configured origin to every caller, leaving the browser to refuse', async function(){
            const acao = await acaoFor(EXPLORER, [EXPLORER, IOS, HOSTILE]);
            assert.strictEqual(acao[EXPLORER], EXPLORER);
            assert.strictEqual(acao[IOS], EXPLORER);
            assert.strictEqual(acao[HOSTILE], EXPLORER);
        });

        it('refuses an unlisted origin server-side once the value is a list', async function(){
            const acao = await acaoFor(`${IOS},${EXPLORER}`, [HOSTILE]);
            assert.strictEqual(acao[HOSTILE], null);
        });

        // THE REGRESSION. Before parseCorsOrigin every one of these read back the
        // raw "a,b,c" string, including for HOSTILE.
        it('echoes each allowlisted origin BACK TO ITSELF, never the raw list', async function(){
            const raw  = `${IOS},${ANDROID},${EXPLORER}`;
            const acao = await acaoFor(raw, [IOS, ANDROID, EXPLORER, HOSTILE]);

            assert.strictEqual(acao[IOS], IOS);
            assert.strictEqual(acao[ANDROID], ANDROID);
            assert.strictEqual(acao[EXPLORER], EXPLORER);
            assert.strictEqual(acao[HOSTILE], null);

            // Stated separately because this is the exact shape of the old bug:
            // a header that is present and populated and accepted by nothing.
            for(const origin of [IOS, ANDROID, EXPLORER]){
                assert.notStrictEqual(acao[origin], raw,
                    'a multi-value ACAO is rejected by every browser; the header must name one origin');
                assert.ok(!String(acao[origin]).includes(','),
                    'ACAO must never contain a comma');
            }
        });

        it('fails CLOSED on `*` mixed with real origins rather than silently opening up', async function(){
            const acao = await acaoFor(`*,${EXPLORER}`, [EXPLORER, HOSTILE]);
            assert.strictEqual(acao[EXPLORER], EXPLORER);
            assert.strictEqual(acao[HOSTILE], null,
                'a stray `*` in a list must not widen the grant to every origin');
        });
    });

    // The parser is only reached if config.js actually calls it. Asserting the
    // source keeps a later edit from reverting to the raw env var while the
    // behavioural tests still pass against the helper in isolation.
    describe('src/config.js wiring', function(){

        it('resolves CORS_ORIGIN through parseCorsOrigin, never the raw env var', function(){
            const src = require('fs').readFileSync(require('path').join(__dirname, '../../src/config.js'), 'utf8');
            assert.ok(/config\['CORS_ORIGIN'\]\s*=\s*parseCorsOrigin\(process\.env\.CORS_ORIGIN\)/.test(src),
                "config.js must set CORS_ORIGIN via parseCorsOrigin(process.env.CORS_ORIGIN)");
            assert.ok(!/config\['CORS_ORIGIN'\]\s*=\s*process\.env\.CORS_ORIGIN/.test(src),
                'config.js must not store the raw CORS_ORIGIN string');
        });
    });
});
