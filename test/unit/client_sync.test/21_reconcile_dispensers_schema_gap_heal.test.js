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
// A source decoder can add a nullable dispensers column at startup. The full re-dump
// then carries a column the replica lacks and the replace fails with errno 1054, so
// the reconcile must heal the schema or every later reconcile fails the same way.

const zlib = require('zlib');
const { assert, sinon, axios, ClientSync } = require('./support');
const { SCHEMA_VERSION } = require('../../../src/schema/version');

const SOURCE = 'http://source1:3006';

function stubPage(page){
    return sinon.stub(axios, 'get').resolves({ data: zlib.gzipSync(Buffer.from(JSON.stringify(page))) });
}

// The real healSchemaIfStale over a stubbed schema fetch, so its errno filter runs too.
function decoderCtx(replaceError){
    let ctx = {
        dbType: 'decoder', chain: 'bitcoin', network: 'mainnet', config: {},
        sources: [SOURCE], _lastSchemaHeal: null, _lastDispenserReconcileAt: null,
        upstreamHeaders: () => ({}),
        withApplyLock: (fn) => fn(),
        fetchAndApplySchema: sinon.stub().resolves(),
        applier: { applyDispensersReplace: replaceError ? sinon.stub().rejects(replaceError)
                                                        : sinon.stub().resolves() },
    };
    ctx.healSchemaIfStale = ClientSync.prototype.healSchemaIfStale.bind(ctx);
    return ctx;
}

function reconcile(ctx){
    return ClientSync.prototype.reconcileDispensers.call(ctx, SOURCE);
}

function errnoError(errno){
    return Object.assign(new Error('replace failed'), { errno });
}

const CURRENT_PAGE = { schema_version: SCHEMA_VERSION.decoder, has_more: false, rows: [{ tx_index: 1, new_col: 7 }] };

describe('ClientSync.reconcileDispensers (schema-gap heal)', function(){
    afterEach(function(){ sinon.restore(); });

    it('re-applies the source schema when the replace fails on a missing column (errno 1054)', async function(){
        let ctx = decoderCtx(errnoError(1054));
        stubPage(CURRENT_PAGE);
        assert.strictEqual(await reconcile(ctx), false);
        assert.strictEqual(ctx.fetchAndApplySchema.calledOnceWith(SOURCE), true);
        assert.strictEqual(ctx._lastDispenserReconcileAt, null);
    });

    it('re-applies the source schema when the replace fails on a missing table (errno 1146)', async function(){
        let ctx = decoderCtx(errnoError(1146));
        stubPage(CURRENT_PAGE);
        assert.strictEqual(await reconcile(ctx), false);
        assert.strictEqual(ctx.fetchAndApplySchema.calledOnce, true);
    });

    it('still returns false when the schema heal itself throws', async function(){
        let ctx = decoderCtx(errnoError(1054));
        ctx.fetchAndApplySchema = sinon.stub().rejects(new Error('schema fetch failed'));
        stubPage(CURRENT_PAGE);
        assert.strictEqual(await reconcile(ctx), false);
        assert.strictEqual(ctx.fetchAndApplySchema.calledOnce, true);
    });

    it('leaves the schema alone on a non-schema replace fault and on a schema_version mismatch', async function(){
        let ctx = decoderCtx(errnoError(1213));
        stubPage(CURRENT_PAGE);
        assert.strictEqual(await reconcile(ctx), false);
        assert.strictEqual(ctx.fetchAndApplySchema.called, false);
        sinon.restore();

        ctx = decoderCtx(null);
        stubPage(Object.assign({}, CURRENT_PAGE, { schema_version: SCHEMA_VERSION.decoder + 1 }));
        assert.strictEqual(await reconcile(ctx), false);
        assert.strictEqual(ctx.applier.applyDispensersReplace.called, false);
        assert.strictEqual(ctx.fetchAndApplySchema.called, false);
    });
});
