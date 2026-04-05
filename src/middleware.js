/*********************************************************************
 *
 * Copyright © 2025 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * Licensed under the Dankest Community License (Apache License 2.0 + Additional Terms).
 * You may not use this file except in compliance with that License.
 *
 * A copy of the License is available at:
 *     https://dankest.llc/license
 *
 * This software is provided "AS IS", without warranties or conditions of any kind.
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
