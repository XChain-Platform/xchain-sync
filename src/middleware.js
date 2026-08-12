/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * XChain Indexer Sync - Middleware
 *
 * Express middleware factories for authentication and security.
 *
 ********************************************************************/

const crypto = require('crypto');

// Constant-time equality for auth secrets: a plain `===` short-circuits at the
// first mismatching character and leaks the key byte-by-byte through response
// timing. timingSafeEqual needs equal-length buffers, so length is guarded
// first, which is safe because a length mismatch is not itself the secret.
// Used by every Bearer-key check, here and in api.js.
function safeEqual(a, b){
    let ab = Buffer.from(String(a == null ? '' : a));
    let bb = Buffer.from(String(b == null ? '' : b));
    if(ab.length !== bb.length) return false;
    return crypto.timingSafeEqual(ab, bb);
}

// With a key configured this fails closed: no "Authorization: Bearer <apiKey>"
// header means 401. A falsy apiKey deliberately passes everything through, for
// single-host, regtest and managed deployments that provision no key (xchain-node
// injects none); api.js warns at startup in that mode and the destructive
// /halt/clear route still refuses to run without a key.
function createApiKeyMiddleware(apiKey){
    return function(req, res, next){
        if(!apiKey) return next();
        let header = req.headers['authorization'];
        if(!header || !safeEqual(header, 'Bearer ' + apiKey))
            return res.status(401).json({ error: 'Unauthorized' });
        next();
    };
}

module.exports = {
    createApiKeyMiddleware,
    safeEqual
};
