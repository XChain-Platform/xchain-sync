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

// Constant-time equality for auth secrets. A plain `===`/`!==` on the key
// leaks it byte-by-byte through response-time differences (the comparison
// short-circuits at the first mismatching character). crypto.timingSafeEqual
// requires equal-length buffers, so the length is guarded first; a length
// mismatch is not itself the secret. Used by every Bearer-key check (REST
// middleware here, plus the /halt/clear route and WS upgrade in api.js).
function safeEqual(a, b){
    let ab = Buffer.from(String(a == null ? '' : a));
    let bb = Buffer.from(String(b == null ? '' : b));
    if(ab.length !== bb.length) return false;
    return crypto.timingSafeEqual(ab, bb);
}

// Create an API key authentication middleware.
// When a key is configured, requests fail closed: they must include
// "Authorization: Bearer <apiKey>" or they are rejected (401).
// When apiKey is falsy the middleware passes requests through (open access;
// single-host / regtest / managed deployments where no key is provisioned;
// xchain-node injects none). api.js logs a startup warning in that mode, and
// the destructive /halt/clear route refuses to run at all without a key.
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
