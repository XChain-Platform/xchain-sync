/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
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
 * XChain Sync - DATETIME retype session-zone validation
 *
 *********************************************************************/

'use strict';

function normalizeZone(value){
    return typeof value === 'string' ? value.trim().toUpperCase() : '';
}

function isUtcSessionZone(row){
    if(!row) return false;
    const timezone = normalizeZone(row.tz);
    if(timezone === '+00:00' || timezone === 'UTC') return true;
    return timezone === 'SYSTEM' && normalizeZone(row.sys) === 'UTC';
}

module.exports = { isUtcSessionZone };
