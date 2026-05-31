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
 * XChain Indexer Sync - Utility Class
 *
 * This file provides utility functions used throughout the sync service
 *
 ********************************************************************/

const crypto = require('crypto');

class Utility {

    constructor(){}

    // Handle sleeping for a given number of milliseconds
    sleep(ms) {
        return new Promise((resolve) => setTimeout(resolve, ms));
    }

    // Check if a value is null or undefined
    isNull(val){
        return (val === null || val === undefined || val === '');
    }

    // Throw an error and log to console
    throwError(error){
        console.error('throwError:', error);
        throw new Error(error);
    }

    // Log an error to console
    logError(error, info){
        console.error('logError: ' + error, info);
    }

    // JSON.stringify with BigInt support
    jsonStringify(obj){
        return JSON.stringify(obj, (key, value) => typeof value === 'bigint' ? value.toString() : value);
    }

    // Get a SHA256 hash of a given data object
    // NOTE: Must produce identical output to xchain-indexer/src/utility.js getDataHash()
    getDataHash(data){
        let obj  = Object.assign({}, data);
        let json = this.jsonStringify(obj);
        let hash = crypto.createHash('sha256').update(json).digest('hex');
        return hash;
    }

    // Start a debug timer
    startTimer(){
        return Date.now();
    }

    // Get elapsed time string from a timer
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
