/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available —
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
// When apiKey is falsy, all requests pass through (open access).
// When set, requests must include: Authorization: Bearer <apiKey>
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
