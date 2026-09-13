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
 * XChain Indexer Sync - Utility Class
 *
 * This file provides utility functions used throughout the sync service
 *
 ********************************************************************/

const crypto = require('crypto');

class Utility {

    constructor(){}

    sleep(ms) {
        return new Promise((resolve) => setTimeout(resolve, ms));
    }

    // Empty string counts as null here; callers rely on that for DB columns.
    isNull(val){
        return (val === null || val === undefined || val === '');
    }

    throwError(error){
        console.error('throwError:', error);
        throw new Error(error);
    }

    logError(error, info){
        console.error('logError: ' + error, info);
    }

    // JSON.stringify with BigInt support. The replacer reads the RAW pre-toJSON value
    // via this[key] rather than the post-toJSON `value`, so a global
    // BigInt.prototype.toJSON patch (one a loaded SDK installs, say) cannot flip a
    // bigint's serialized form and desync the two hashers. Consensus pair with
    // xchain-indexer/src/utility.js jsonStringify(); the two MUST stay byte-identical.
    jsonStringify(obj){
        return JSON.stringify(obj, function(key, value){
            const raw = this[key];
            return typeof raw === 'bigint' ? raw.toString() : value;
        });
    }

    // Must produce identical output to xchain-indexer/src/utility.js getDataHash().
    getDataHash(data){
        let obj  = Object.assign({}, data);
        let json = this.jsonStringify(obj);
        let hash = crypto.createHash('sha256').update(json).digest('hex');
        return hash;
    }

    startTimer(){
        return Date.now();
    }

    // Formats elapsed time as ms / s / m+s for log lines, not for arithmetic.
    getTimer(timer){
        let ms = Date.now() - timer;
        if(ms < 1000) return ms + 'ms';
        let seconds = Math.floor(ms / 1000);
        if(seconds < 60) return seconds + 's';
        let minutes = Math.floor(seconds / 60);
        return minutes + 'm ' + (seconds % 60) + 's';
    }
}

module.exports = Utility;
