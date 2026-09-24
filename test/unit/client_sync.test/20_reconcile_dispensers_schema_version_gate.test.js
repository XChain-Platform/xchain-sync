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
