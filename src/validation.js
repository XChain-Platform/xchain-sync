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

// Extract the ordered list of column names from a CREATE TABLE DDL
// (as produced by SHOW CREATE TABLE). Column definition lines are the
// only lines that start with a backtick-quoted identifier; the opening
// `CREATE TABLE \`name\` (` line begins with CREATE, and constraint lines
// begin with PRIMARY/UNIQUE/KEY/CONSTRAINT/FULLTEXT etc. Returns [] for
// non-string input or a DDL with no parseable columns.
function extractColumnNames(ddl){
    if(typeof ddl !== 'string') return [];
    let names = [];
    for(let line of ddl.split('\n')){
        let trimmed = line.trim();
        if(trimmed.charAt(0) !== '`') continue;
        let endTick = trimmed.indexOf('`', 1);
        if(endTick <= 1) continue;
        names.push(trimmed.substring(1, endTick));
    }
    return names;
}

// Extract a single column's definition from a CREATE TABLE DDL, suitable
// for use as the body of `ALTER TABLE ... ADD COLUMN`. Returns the
// backtick-quoted name plus its type/attributes (e.g.
// "`new_col` varchar(255) DEFAULT NULL") with any trailing comma stripped.
// Returns null if the column cannot be cleanly located, or if the
// definition would smuggle additional actions into the ALTER. Two guards:
//   1. Reject any line containing a semicolon (a hostile DDL could end the
//      column line with "; DROP TABLE ...", slipping a second statement
//      past validateDdl into the ALTER).
//   2. Reject any line carrying a bare comma at parenthesis-depth 0 (after
//      the trailing comma is stripped). MariaDB treats a single
//      "ADD COLUMN `c` int, DROP COLUMN victim" ALTER as ONE valid
//      statement; no semicolon, so guard 1 and multipleStatements:false
//      both miss it. Commas inside parentheses (decimal(18,8),
//      enum('a','b'), etc.) are part of the type and must survive, so the
//      scan only trips on a comma at depth 0.
// Callers should treat null as "skip this column" rather than aborting.
function extractColumnDefinition(ddl, columnName){
    if(typeof ddl !== 'string' || typeof columnName !== 'string')
        return null;
    for(let line of ddl.split('\n')){
        let trimmed = line.trim();
        if(trimmed.charAt(0) !== '`') continue;
        let endTick = trimmed.indexOf('`', 1);
        if(endTick <= 1) continue;
        if(trimmed.substring(1, endTick) !== columnName) continue;
        let def = trimmed.replace(/,\s*$/, '');
        if(def.indexOf(';') !== -1) return null;
        // Depth scan for a bare top-level comma (a smuggled second ALTER action),
        // QUOTE-AWARE: a '(' or ',' inside a string literal ('...'), a backtick
        // identifier (`...`) or COMMENT text must not inflate depth or be read as a
        // real separator. Without this a hostile schema line like
        //   `evil` int COMMENT '(' , DROP COLUMN `victim`
        // pushes depth to 1 on the quoted '(' so the top-level ", DROP COLUMN" comma
        // is seen at depth 1 and slips past, splicing a second action into the ALTER.
        // MariaDB escapes a quote by doubling it ('' / ``); skip the pair so the quoted
        // region does not close early and re-expose attacker text.
        let depth = 0;
        let inStr = false, inTick = false;
        for(let i = 0; i < def.length; i++){
            let ch = def.charAt(i);
            if(inStr){
                if(ch === "'"){
                    if(def.charAt(i + 1) === "'"){ i++; continue; } // escaped quote
                    inStr = false;
                }
                continue;
            }
            if(inTick){
                if(ch === '`'){
                    if(def.charAt(i + 1) === '`'){ i++; continue; } // escaped backtick
                    inTick = false;
                }
                continue;
            }
            if(ch === "'") inStr = true;
            else if(ch === '`') inTick = true;
            else if(ch === '(') depth++;
            else if(ch === ')'){ if(depth > 0) depth--; } // clamp: a stray ')' from a quoted value
            else if(ch === ',' && depth <= 0) return null;
        }
        return def;
    }
    return null;
}

// Blank out quoted regions ('...', `...`, "...") so a keyword scan cannot be
// fooled by attacker or comment text inside a column definition. Doubled quotes
// are MariaDB's escape form and stay inside the region. Length is preserved so
// callers can still index into the original string.
function maskSqlQuoted(sql){
    let out   = '';
    let quote = null;
    for(let i = 0; i < sql.length; i++){
        let ch = sql.charAt(i);
        if(quote){
            if(ch === quote && sql.charAt(i + 1) === quote){ out += '  '; i++; continue; }
            if(ch === quote){ quote = null; out += ' '; continue; }
            out += ' ';
            continue;
        }
        if(ch === "'" || ch === '`' || ch === '"'){ quote = ch; out += ' '; continue; }
        out += ch;
    }
    return out;
}

// True when a column definition line declares AUTO_INCREMENT. Quote-aware: a
// COMMENT 'auto_increment' must not read as one, because the caller uses this
// to decide whether the generated ALTER needs a key clause and a false positive
// would bolt a spurious index onto an ordinary column.
function isAutoIncrementDefinition(def){
    if(typeof def !== 'string') return false;
    return /\bAUTO_INCREMENT\b/i.test(maskSqlQuoted(def));
}

// Find the source-side index that covers a single column, as declared in a
// CREATE TABLE DDL (SHOW CREATE TABLE output). Returns
// { type: 'primary'|'unique'|'index', name, columns } for the narrowest
// single-column key on that column, or null when the source declares none.
//
// Exists for the AUTO_INCREMENT case: MariaDB refuses an ALTER that adds an
// auto-increment column without making it a key (errno 1075), so the generated
// ALTER has to carry the source's own key rather than invent one. Multi-column
// keys are ignored: they are not reproducible from a single ADD COLUMN and the
// caller synthesises a UNIQUE key instead. Preference order primary > unique >
// index keeps the replica's key as close to the source as the statement allows.
function extractKeyForColumn(ddl, columnName){
    if(typeof ddl !== 'string' || typeof columnName !== 'string') return null;
    let best = null;
    const RANK = { primary: 0, unique: 1, index: 2 };
    for(let line of ddl.split('\n')){
        let trimmed = line.trim().replace(/,\s*$/, '');
        if(trimmed.indexOf(';') !== -1) continue;

        let type = null, name = null, colsRaw = null, m = null;
        // Greedy inner capture: an index part may carry a prefix length
        // (`pubkey`(16)), so the closing paren is the LAST one on the line.
        if((m = /^PRIMARY\s+KEY\s*\((.*)\)$/i.exec(trimmed))){
            type = 'primary'; colsRaw = m[1];
        } else if((m = /^UNIQUE\s+KEY\s+`([^`]+)`\s*\((.*)\)$/i.exec(trimmed))){
            type = 'unique'; name = m[1]; colsRaw = m[2];
        } else if((m = /^KEY\s+`([^`]+)`\s*\((.*)\)$/i.exec(trimmed))){
            type = 'index'; name = m[1]; colsRaw = m[2];
        } else {
            continue;
        }

        // Index parts are `col` or `col`(n) (prefix length); anything that is not
        // a plain backtick-quoted identifier (a functional/expression index) makes
        // the whole key unusable here.
        let parts = colsRaw.split(',').map(p => p.trim());
        let cols  = [];
        let clean = true;
        for(let part of parts){
            let pm = /^`([^`]+)`(\(\d+\))?$/.exec(part);
            if(!pm || !validateIdentifier(pm[1]).valid){ clean = false; break; }
            cols.push(pm[1]);
        }
        if(!clean || cols.length !== 1 || cols[0] !== columnName) continue;
        if(name !== null && !validateIdentifier(name).valid) continue;

        let candidate = { type, name: name, columns: cols };
        if(best === null || RANK[type] < RANK[best.type]) best = candidate;
    }
    return best;
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
        if(!Number.isInteger(event.block_index))
            return { valid: false, reason: event.type + ' event block_index is not an integer' };
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
    validateWsEvent,
    extractColumnNames,
    extractColumnDefinition,
    isAutoIncrementDefinition,
    extractKeyForColumn
};
