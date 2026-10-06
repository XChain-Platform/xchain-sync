// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const zlib = require('zlib');
const { assert, sinon, axios, ClientSync } = require('./support');
const { SCHEMA_VERSION } = require('../../../src/schema/version');

const SOURCE = 'http://source1:3006';

// Gzipped page body as the /snapshot-dispensers route serves it.
function pageBuffer(page){
    return zlib.gzipSync(Buffer.from(JSON.stringify(page)));
}

function decoderCtx(){
    return {
        dbType: 'decoder', chain: 'bitcoin', network: 'mainnet', config: {},
        _lastDispenserReconcileAt: null,
        upstreamHeaders: () => ({}),
        withApplyLock: (fn) => fn(),
        applier: { applyDispensersReplace: sinon.stub().resolves() },
    };
}

function stubPages(pages){
    let get = sinon.stub(axios, 'get');
    pages.forEach((p, i) => get.onCall(i).resolves({ data: pageBuffer(p) }));
    return get;
}

function reconcile(ctx){
    return ClientSync.prototype.reconcileDispensers.call(ctx, SOURCE);
}

describe('ClientSync.reconcileDispensers (schema_version gate)', function(){
    afterEach(function(){ sinon.restore(); });

    it('replaces the table from a page carrying the client schema version', async function(){
        let ctx = decoderCtx();
        stubPages([{ schema_version: SCHEMA_VERSION.decoder, has_more: false, rows: [{ tx_index: 1 }] }]);
        await reconcile(ctx);
        assert.strictEqual(ctx.applier.applyDispensersReplace.calledOnce, true);
        assert.deepStrictEqual(ctx.applier.applyDispensersReplace.firstCall.args[0], [{ tx_index: 1 }]);
        assert.ok(ctx._lastDispenserReconcileAt > 0);
    });

    it('leaves the table intact when the page schema version differs', async function(){
        let ctx = decoderCtx();
        stubPages([{ schema_version: SCHEMA_VERSION.decoder + 1, has_more: false, rows: [{ tx_index: 1 }] }]);
        await reconcile(ctx);
        assert.strictEqual(ctx.applier.applyDispensersReplace.called, false);
        assert.strictEqual(ctx._lastDispenserReconcileAt, null);
    });

    it('fails closed on a page with no schema_version field', async function(){
        let ctx = decoderCtx();
        stubPages([{ has_more: false, rows: [{ tx_index: 1 }] }]);
        await reconcile(ctx);
        assert.strictEqual(ctx.applier.applyDispensersReplace.called, false);
        assert.strictEqual(ctx._lastDispenserReconcileAt, null);
    });

    it('applies nothing when a later page mismatches after an accepted first page', async function(){
        let ctx = decoderCtx();
        let get = stubPages([
            { schema_version: SCHEMA_VERSION.decoder, has_more: true, max_tx: 5, max_addr: 'a', rows: [{ tx_index: 1 }] },
            { schema_version: SCHEMA_VERSION.decoder - 1, has_more: false, rows: [{ tx_index: 6 }] },
        ]);
        await reconcile(ctx);
        assert.strictEqual(get.callCount, 2);
        assert.strictEqual(ctx.applier.applyDispensersReplace.called, false);
        assert.strictEqual(ctx._lastDispenserReconcileAt, null);
    });
});

// reconcileDispensers reports whether the replace committed, so its callers can keep
// the dispensers count check to the post-replace equality it is documented to be.
describe('ClientSync.reconcileDispensers (return value)', function(){
    afterEach(function(){ sinon.restore(); });

    it('returns true only after a committed replace', async function(){
        let ctx = decoderCtx();
        stubPages([{ schema_version: SCHEMA_VERSION.decoder, has_more: false, rows: [{ tx_index: 1 }] }]);
        assert.strictEqual(await reconcile(ctx), true);
    });

    it('returns false on a schema mismatch, a fetch failure and a failed replace', async function(){
        let ctx = decoderCtx();
        stubPages([{ schema_version: SCHEMA_VERSION.decoder + 1, has_more: false, rows: [] }]);
        assert.strictEqual(await reconcile(ctx), false);
        sinon.restore();

        ctx = decoderCtx();
        sinon.stub(axios, 'get').rejects(new Error('ECONNRESET'));
        assert.strictEqual(await reconcile(ctx), false);
        sinon.restore();

        ctx = decoderCtx();
        ctx.applier.applyDispensersReplace = sinon.stub().rejects(new Error('lock wait'));
        stubPages([{ schema_version: SCHEMA_VERSION.decoder, has_more: false, rows: [{ tx_index: 1 }] }]);
        assert.strictEqual(await reconcile(ctx), false);
    });

    it('returns false without fetching for a missing source or a non-decoder replica', async function(){
        let get = sinon.stub(axios, 'get');
        assert.strictEqual(await ClientSync.prototype.reconcileDispensers.call(decoderCtx(), null), false);
        assert.strictEqual(await reconcile(Object.assign(decoderCtx(), { dbType: 'indexer' })), false);
        assert.strictEqual(get.called, false);
    });
});

// Both callers leave dispensers out of the count check unless the reconcile committed:
// a failed one leaves the drifted (or, after a truncated bootstrap, empty) table behind.
describe('ClientSync dispensers count check gated on reconcile success', function(){
    function callerCtx(shouldReconcile, reconciled){
        return {
            lastAppliedBlock: 900,
            shouldReconcileDispensers: sinon.stub().returns(shouldReconcile),
            reconcileDispensers: sinon.stub().resolves(reconciled),
            verifyDecoderCompleteness: sinon.stub().resolves(null),
        };
    }
    function excludeOf(ctx){ return ctx.verifyDecoderCompleteness.firstCall.args[2]; }
    function catchUp(ctx){ return ClientSync.prototype.verifyIncrementalCatchUpDecoder.call(ctx, SOURCE); }
    function bootstrap(ctx){ return ClientSync.prototype.reconcileBootstrapDecoder.call(ctx, SOURCE); }

    it('catch-up off cadence: no reconcile, dispensers excluded', async function(){
        let ctx = callerCtx(false, true);
        await catchUp(ctx);
        assert.strictEqual(ctx.reconcileDispensers.called, false);
        assert.ok(excludeOf(ctx) instanceof Set && excludeOf(ctx).has('dispensers'));
    });

    it('catch-up after a committed reconcile: dispensers checked', async function(){
        let ctx = callerCtx(true, true);
        await catchUp(ctx);
        assert.strictEqual(ctx.reconcileDispensers.calledOnceWith(SOURCE), true);
        assert.strictEqual(excludeOf(ctx), null);
    });

    it('catch-up after a failed reconcile: dispensers still excluded', async function(){
        let ctx = callerCtx(true, false);
        await catchUp(ctx);
        assert.ok(excludeOf(ctx) instanceof Set && excludeOf(ctx).has('dispensers'));
    });

    it('bootstrap: no exclusion after a committed reconcile, dispensers excluded after a failed one', async function(){
        let ok = callerCtx(true, true);
        await bootstrap(ok);
        assert.strictEqual(excludeOf(ok), undefined);
        assert.strictEqual(ok.verifyDecoderCompleteness.firstCall.args[1], 900);

        let failed = callerCtx(true, false);
        await bootstrap(failed);
        assert.ok(excludeOf(failed) instanceof Set && excludeOf(failed).has('dispensers'));
    });
});
