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
 * fast-check arbitraries for complete DB row objects matching table schemas.
 *
 * Used by suites for _insertRows, applyBlock, and snapshot testing.
 */

const fc = require('fast-check');
const {
    blockIndex, actionIndex, txIndex, blockTime,
    amountString, hashString, bigIntLikeValue, tinyIntValue,
    textValue, addressString, tickString,
} = require('./values');

function blocksRow() {
    return fc.record({
        block_index: blockIndex(),
        block_time: blockTime(),
        ledger_hash_id: bigIntLikeValue(),
        actions_hash_id: bigIntLikeValue(),
        contract_hash_id: bigIntLikeValue(),
    });
}

function transactionsRow() {
    return fc.record({
        tx_index: txIndex(),
        block_index: blockIndex(),
        tx_hash_id: bigIntLikeValue(),
        source_id: bigIntLikeValue(),
    });
}

function actionsRow() {
    return fc.record({
        action_index: actionIndex(),
        block_index: blockIndex(),
        tx_index: txIndex(),
        tx_vout: fc.integer({ min: 0, max: 10 }),
        action_id: bigIntLikeValue(),
        action_format: tinyIntValue(),
    });
}

function creditsRow() {
    return fc.record({
        action_index: actionIndex(),
        address_id: bigIntLikeValue(),
        tick_id: bigIntLikeValue(),
        amount: amountString(),
    });
}

/** debits table row (same shape as credits) */
function debitsRow() {
    return fc.record({
        action_index: actionIndex(),
        address_id: bigIntLikeValue(),
        tick_id: bigIntLikeValue(),
        amount: amountString(),
    });
}

function indexAddressRow() {
    return fc.record({ address: addressString() });
}

function indexTransactionRow() {
    return fc.record({ hash: hashString() });
}

function indexTickerRow() {
    return fc.record({ tick: tickString() });
}

function indexActionRow() {
    return fc.record({ action: fc.string({ minLength: 0, maxLength: 30 }) });
}

function indexCoinRow() {
    return fc.record({ coin: fc.string({ minLength: 0, maxLength: 50 }) });
}

function indexMemoRow() {
    return fc.record({ memo: textValue() });
}

function indexPubkeyRow() {
    return fc.record({ pubkey: hashString() });
}

function indexStatusRow() {
    return fc.record({ status: fc.string({ minLength: 0, maxLength: 50 }) });
}

function indexMimeTypeRow() {
    return fc.record({ type: fc.string({ minLength: 0, maxLength: 100 }) });
}

function indexFiatRow() {
    return fc.record({
        code: fc.string({ minLength: 0, maxLength: 10 }),
        name: fc.string({ minLength: 0, maxLength: 50 }),
    });
}

/**
 * Generic data row with 1–8 columns of arbitrary names and values.
 * Column names are restricted to lowercase + underscore to be SQL-safe.
 */
function genericDataRow() {
    return fc.integer({ min: 1, max: 8 }).chain(count =>
        fc.array(
            fc.tuple(
                fc.string({ unit: fc.constantFrom(...'abcdefghijklmnopqrstuvwxyz_'.split('')), minLength: 1, maxLength: 20 })
                    .filter(s => /^[a-z]/.test(s)),
                fc.oneof(amountString(), bigIntLikeValue(), textValue())
            ),
            { minLength: count, maxLength: count }
        ).filter(pairs => {
            // Ensure unique column names
            let names = pairs.map(p => p[0]);
            return new Set(names).size === names.length;
        }).map(pairs => Object.fromEntries(pairs))
    );
}

function rowArray(rowArb, minLength, maxLength) {
    return fc.array(rowArb, { minLength: minLength || 0, maxLength: maxLength || 20 });
}

function mixedRows() {
    return fc.oneof(
        rowArray(blocksRow(), 1, 5),
        rowArray(actionsRow(), 1, 20),
        rowArray(creditsRow(), 1, 20),
        rowArray(genericDataRow(), 1, 50),
    );
}

module.exports = {
    blocksRow,
    transactionsRow,
    actionsRow,
    creditsRow,
    debitsRow,
    indexAddressRow,
    indexTransactionRow,
    indexTickerRow,
    indexActionRow,
    indexCoinRow,
    indexMemoRow,
    indexPubkeyRow,
    indexStatusRow,
    indexMimeTypeRow,
    indexFiatRow,
    genericDataRow,
    rowArray,
    mixedRows,
};
