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
 * XChain Indexer Sync - Validation Utilities
 *
 * Pure validation functions for identifiers, DDL statements, and
 * WebSocket event schemas. Used across multiple modules to enforce
 * security boundaries on untrusted input.
 *
 ********************************************************************/

// Compiled once at module load for performance
const IDENTIFIER_RE = /^[A-Za-z0-9_]+$/;
const DDL_START_RE  = /^\s*CREATE\s+TABLE\s/i;
const DDL_BANNED_STATEMENT_RE = /;\s*(DROP|CREATE\s+TRIGGER|CREATE\s+PROCEDURE|CREATE\s+FUNCTION|CREATE\s+EVENT|CREATE\s+VIEW|EXEC|EXECUTE)\b/i;
const DDL_WRONG_TYPE_RE = /^\s*CREATE\s+(TRIGGER|PROCEDURE|FUNCTION|EVENT|VIEW)\b/i;

const VALID_WS_TYPES = new Set(['block', 'reorg', 'status']);

// Validate a SQL identifier (table name, column name, database name).
// Accepts only [A-Za-z0-9_], 1–64 characters.
function validateIdentifier(val){
    if(val === null || val === undefined)
        return { valid: false, reason: 'Identifier is null or undefined' };
    if(typeof val !== 'string')
        return { valid: false, reason: 'Identifier is not a string' };
    if(val.length === 0)
        return { valid: false, reason: 'Identifier is empty' };
    if(val.length > 64)
        return { valid: false, reason: 'Identifier exceeds 64 characters' };
    if(!IDENTIFIER_RE.test(val))
        return { valid: false, reason: 'Identifier contains invalid characters' };
    return { valid: true };
}

// Validate a DDL statement from a remote source.
// Must start with CREATE TABLE; rejects dangerous statement types and
// multi-statement injection (DROP, TRIGGER, PROCEDURE, etc. after semicolons).
function validateDdl(sql){
    if(sql === null || sql === undefined)
        return { valid: false, reason: 'DDL is null or undefined' };
    if(typeof sql !== 'string')
        return { valid: false, reason: 'DDL is not a string' };
    if(sql.trim().length === 0)
        return { valid: false, reason: 'DDL is empty' };
    if(!DDL_START_RE.test(sql))
        return { valid: false, reason: 'DDL does not start with CREATE TABLE' };
    if(DDL_WRONG_TYPE_RE.test(sql))
        return { valid: false, reason: 'DDL is a disallowed statement type' };
    if(DDL_BANNED_STATEMENT_RE.test(sql))
        return { valid: false, reason: 'DDL contains disallowed statement after semicolon' };
    return { valid: true };
}

// Validate a WebSocket event has the expected shape.
// Accepted types: block, reorg, status.
// block/reorg require a positive integer block_index.
// status requires a non-negative block_height (or null).
function validateWsEvent(event){
    if(event === null || event === undefined)
        return { valid: false, reason: 'Event is null or undefined' };
    if(typeof event !== 'object' || Array.isArray(event))
        return { valid: false, reason: 'Event is not an object' };
    if(typeof event.type !== 'string')
        return { valid: false, reason: 'Event type is not a string' };
    if(!VALID_WS_TYPES.has(event.type))
        return { valid: false, reason: 'Unknown event type: ' + event.type };

    if(event.type === 'block' || event.type === 'reorg'){
        if(event.block_index === null || event.block_index === undefined)
            return { valid: false, reason: event.type + ' event missing block_index' };
        if(typeof event.block_index !== 'number' || !Number.isFinite(event.block_index))
            return { valid: false, reason: event.type + ' event block_index is not a finite number' };
        if(event.block_index < 0)
            return { valid: false, reason: event.type + ' event block_index is negative' };
    }

    if(event.type === 'status'){
        if(event.block_height !== null && event.block_height !== undefined){
            if(typeof event.block_height !== 'number' || !Number.isFinite(event.block_height))
                return { valid: false, reason: 'status event block_height is not a finite number' };
        }
    }

    return { valid: true, type: event.type };
}

module.exports = {
    validateIdentifier,
    validateDdl,
    validateWsEvent
};
