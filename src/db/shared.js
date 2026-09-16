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
 * The module-scope helpers shared by db/index.js and more than one mixin.
 * They live here rather than in index.js so a mixin does not have to require
 * the class it is installed on.
 *
 ********************************************************************/

const validation = require('../util/validation');

// Guard for the few queries that must interpolate a table name into a
// backtick-quoted identifier (COUNT(*), pagination, TRUNCATE). Parameter
// binding cannot carry identifiers. Some callers pass server-supplied names
// (e.g. a sync source's `table_counts` keys), so a stray backtick or
// metacharacter here would break out of the quoting. Reject anything that
// isn't a plain [A-Za-z0-9_] identifier before it reaches the query string.
function assertValidIdentifier(table){
    const check = validation.validateIdentifier(table);
    if(!check.valid)
        throw new Error('Refusing to query unsafe table identifier: ' + check.reason);
}

// A stake weight, as stake_weighted_quorum.bcnum accepts one (plain decimal string).
// Kept identical to that predicate's pattern so this producer can never emit a row the
// predicate then has to fail closed on. Twin of the indexer's requireStakeWeight.
const STAKE_WEIGHT_NUMERIC = /^[+-]?(\d+\.?\d*|\.\d+)$/;

// Fail CLOSED on a weightless stake-weight row. The source-keyed weight
// producer routes through here instead of resolving a missing weight to '0'. The '0'
// looks harmless and is not: the source stays in the quorum's dedupe map carrying no
// stake, so the denominator S shrinks while a signer keeps the full numerator, and a
// smaller real stake clears 3*tally > 2*S. stake_weighted_quorum already rejects such
// a row, but it never sees one - consumers re-map the set through
// `String(v.weight != null ? v.weight : '0')`, laundering the missing weight into a
// well-formed zero before the predicate runs. stakes.amount is NOT NULL and the
// source-aggregate is HAVING-filtered, so a null here is a corrupt read, not a
// stakeless source. The value is returned UNTRIMMED so the stakes_root leaves this
// feeds keep hashing byte-for-byte what they did before. A legitimate '0' still passes.
function requireStakeWeight(weight, label){
    if(weight === null || weight === undefined)
        throw new Error((label || 'stake weights') + ': missing validator weight would silently lower the stake-quorum denominator S');
    let w = String(weight).trim();
    if(w === '' || !STAKE_WEIGHT_NUMERIC.test(w))
        throw new Error((label || 'stake weights') + ': nonnumeric validator weight "' + w.slice(0, 32) + '" would silently lower the stake-quorum denominator S');
    return String(weight);
}

module.exports = { assertValidIdentifier, requireStakeWeight, STAKE_WEIGHT_NUMERIC };
