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
