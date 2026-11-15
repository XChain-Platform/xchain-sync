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
 * XChain Sync - Snapshot Hash Headers
 *
 ********************************************************************/

'use strict';

function setHashHeaders(res, dbType, hashRow){
    if(!hashRow) return;
    if(dbType === 'decoder'){
        res.setHeader('X-Block-Hash', hashRow.block_hash || '');
        return;
    }
    res.setHeader('X-Ledger-Hash', hashRow.ledger_hash || '');
    res.setHeader('X-Actions-Hash', hashRow.actions_hash || '');
    res.setHeader('X-Contract-Hash', hashRow.contract_hash || '');
}

module.exports = { setHashHeaders };
