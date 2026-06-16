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

// Create an API key authentication middleware.
// When a key is configured, requests fail closed: they must include
// "Authorization: Bearer <apiKey>" or they are rejected (401).
// When apiKey is falsy the middleware passes requests through (open access —
// single-host / regtest / managed deployments where no key is provisioned;
// xchain-node injects none). api.js logs a startup warning in that mode, and
// the destructive /halt/clear route refuses to run at all without a key.
function createApiKeyMiddleware(apiKey){
    return function(req, res, next){
        if(!apiKey) return next();
        let header = req.headers['authorization'];
        if(!header || header !== 'Bearer ' + apiKey)
            return res.status(401).json({ error: 'Unauthorized' });
        next();
    };
}

module.exports = {
    createApiKeyMiddleware
};
