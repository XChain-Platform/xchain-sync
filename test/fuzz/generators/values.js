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
 * fast-check arbitraries for primitive field types found in the sync DB schema and config.
 *
 * No imports from other generators — this is the leaf module.
 */

const fc = require('fast-check');

const HEX_CHARS = '0123456789abcdef'.split('');
const ALNUM_CHARS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'.split('');

// Helper: string of exact length from a character set
function charString(chars, minLen, maxLen) {
    return fc.array(fc.constantFrom(...chars), { minLength: minLen, maxLength: maxLen })
        .map(a => a.join(''));
}

/** BIGINT UNSIGNED block_index (1–9,999,999) */
function blockIndex() {
    return fc.integer({ min: 1, max: 9999999 });
}

/** BIGINT UNSIGNED action_index */
function actionIndex() {
    return fc.integer({ min: 1, max: 999999999 });
}

/** BIGINT UNSIGNED tx_index */
function txIndex() {
    return fc.integer({ min: 1, max: 999999999 });
}

/** Unix timestamp in plausible range */
function blockTime() {
    return fc.integer({ min: 1000000000, max: 2000000000 });
}

/** VARCHAR(250) amount — valid, invalid, and adversarial */
function amountString() {
    return fc.oneof(
        fc.integer({ min: 0, max: 999999999 }).map(String),
        fc.tuple(fc.integer({ min: 0, max: 999999 }), fc.integer({ min: 1, max: 18 }))
            .map(([int, dec]) => int + '.' + String(Math.random()).slice(2, 2 + dec)),
        fc.constantFrom('0', '-1', 'abc', '', 'NaN', 'Infinity', '1e18', '0x1A', null, undefined),
        fc.string({ minLength: 0, maxLength: 50 }),
    );
}

/** 64-char hex hash — valid, short, long, garbage, null */
function hashString() {
    return fc.oneof(
        charString(HEX_CHARS, 64, 64),
        fc.string({ minLength: 0, maxLength: 80 }),
        fc.constantFrom(null, undefined, '', 'null'),
    );
}

/** BIGINT UNSIGNED foreign key ID — valid, negative, null, NaN */
function bigIntLikeValue() {
    return fc.oneof(
        fc.integer({ min: 0, max: Number.MAX_SAFE_INTEGER }),
        fc.integer({ min: -1000, max: -1 }),
        fc.constantFrom(null, undefined, 0, NaN, Infinity, '', 'abc'),
    );
}

/** TINYINT UNSIGNED — valid (0–255), out of range, wrong types */
function tinyIntValue() {
    return fc.oneof(
        fc.integer({ min: 0, max: 255 }),
        fc.integer({ min: -10, max: -1 }),
        fc.integer({ min: 256, max: 1000 }),
        fc.constantFrom(null, undefined, 'abc', 1.5, NaN, Infinity),
    );
}

/** TEXT/MEDIUMTEXT — strings, nulls, embedded null bytes */
function textValue() {
    return fc.oneof(
        fc.string({ minLength: 0, maxLength: 500 }),
        fc.constantFrom(null, undefined, '', '\x00test', '\u0000'),
    );
}

/** Address string — valid lengths, invalid, SQL injection probes */
function addressString() {
    return fc.oneof(
        charString(ALNUM_CHARS, 26, 42),
        fc.string({ minLength: 0, maxLength: 60 }),
        fc.constantFrom(null, undefined, '', "'; DROP TABLE--", '1 OR 1=1'),
    );
}

/** Tick string — valid, overlength, adversarial */
function tickString() {
    return fc.oneof(
        charString(ALNUM_CHARS, 1, 50),
        fc.string({ minLength: 0, maxLength: 260 }),
        fc.constantFrom(null, undefined, '', '\x00', '`tick`', '"tick"'),
    );
}

/** Port value — valid, out of range, wrong types */
function portValue() {
    return fc.oneof(
        fc.integer({ min: 1, max: 65535 }),
        fc.integer({ min: -100, max: 0 }),
        fc.integer({ min: 65536, max: 99999 }),
        fc.constantFrom(null, undefined, '', 'abc', '3306', 3306),
    );
}

/** Environment variable string — anything process.env might hold */
function envVarString() {
    return fc.oneof(
        fc.string({ minLength: 0, maxLength: 100 }),
        fc.constantFrom(undefined, null, '', '0', '-1', 'true', 'false', 'FALSE', 'abc', '9999999'),
    );
}

/** Coin key from hub config */
function coinString() {
    return fc.oneof(
        fc.constantFrom('bitcoin', 'litecoin', 'dogecoin'),
        fc.string({ minLength: 0, maxLength: 30 }),
    );
}

/** Network key from hub config */
function networkString() {
    return fc.oneof(
        fc.constantFrom('mainnet', 'testnet', 'regtest'),
        fc.string({ minLength: 0, maxLength: 30 }),
    );
}

module.exports = {
    charString,
    blockIndex,
    actionIndex,
    txIndex,
    blockTime,
    amountString,
    hashString,
    bigIntLikeValue,
    tinyIntValue,
    textValue,
    addressString,
    tickString,
    portValue,
    envVarString,
    coinString,
    networkString,
};
