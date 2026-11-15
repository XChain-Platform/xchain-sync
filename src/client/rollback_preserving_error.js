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
 **********************************************************************/
'use strict';

const { getLogger } = require('../observability');

async function rollbackPreservingError(db, cause, context){
    try {
        await db.rollbackTransaction();
    } catch(rollbackError){
        const causeText = (cause && cause.message) || cause;
        const rollbackText = (rollbackError && rollbackError.message) || rollbackError;
        try {
            getLogger().error(context + ' rollback failed while unwinding after: ' + causeText +
                '; rollback error: ' + rollbackText);
        } catch(_logError){
            // Error preservation must not depend on the logging sink.
        }
    }
}

module.exports = { rollbackPreservingError };
